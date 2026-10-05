import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { eq, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  agents, companies, createDb, executionDispatchCheckpoints, heartbeatRuns, issues,
} from "@paperclipai/db";
import type { ServerAdapterModule } from "../adapters/index.js";
import { registerServerAdapter, unregisterServerAdapter } from "../adapters/index.js";
import { heartbeatService } from "../services/heartbeat.js";
import { recordDispatchIntent } from "../services/execution-dispatch-checkpoints.js";
import { drainHeartbeatRunsToQuiescence } from "./helpers/drain-heartbeat-runs.js";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";

const support = await getEmbeddedPostgresTestSupport();
const suite = support.supported ? describe : describe.skip;

async function waitForRun(heartbeat: ReturnType<typeof heartbeatService>, runId: string, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const run = await heartbeat.getRun(runId);
    if (run && !["queued", "running"].includes(run.status)) return run;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return heartbeat.getRun(runId);
}

suite("heartbeat dispatch checkpoint hooks", () => {
  let db!: ReturnType<typeof createDb>;
  let heartbeat!: ReturnType<typeof heartbeatService>;
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;
  const execute = vi.fn<ServerAdapterModule["execute"]>();

  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("heartbeat-dispatch-hooks-");
    db = createDb(temporary.connectionString);
    heartbeat = heartbeatService(db);
    registerServerAdapter({
      type: "claude_local",
      supportsLocalAgentJwt: false,
      execute,
      testEnvironment: async () => ({ adapterType: "claude_local", status: "pass", checks: [], testedAt: new Date(0).toISOString() }),
    });
  }, 30_000);

  afterEach(async () => {
    await drainHeartbeatRunsToQuiescence(db, heartbeat);
    vi.clearAllMocks();
    await db.execute(sql.raw(`
      TRUNCATE TABLE "execution_dispatch_checkpoints", "issue_execution_fences", "issue_recovery_actions",
        "activity_log", "heartbeat_run_events", "heartbeat_runs", "agent_wakeup_requests", "agent_runtime_state",
        "issues", "agents", "companies" RESTART IDENTITY CASCADE`));
  });

  afterAll(async () => {
    await drainHeartbeatRunsToQuiescence(db, heartbeat);
    unregisterServerAdapter("claude_local");
    await temporary?.cleanup();
  });

  async function seed() {
    const companyId = randomUUID(), agentId = randomUUID(), issueId = randomUUID();
    await db.insert(companies).values({
      id: companyId, name: "Dispatch hooks", issuePrefix: `H${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false, defaultResponsibleUserId: "responsible-user",
    });
    await db.insert(agents).values({
      id: agentId, companyId, name: "Hook agent", role: "engineer", status: "idle", adapterType: "claude_local",
      adapterConfig: {}, runtimeConfig: {}, permissions: {},
    });
    await db.insert(issues).values({
      id: issueId, companyId, title: "Hook work", status: "in_progress", priority: "medium", assigneeAgentId: agentId,
      responsibleUserId: "responsible-user", issueNumber: 1, identifier: `HK-${issueId.slice(0, 6)}`,
    });
    return { companyId, agentId, issueId };
  }

  const okResult = { exitCode: 0, signal: null, timedOut: false, provider: "claude", model: "m", summary: "done" };

  it("walks a normal run through dispatching, provider_started and provider_returned to completed", async () => {
    const s = await seed();
    const seen: string[] = [];
    execute.mockImplementation(async (ctx) => {
      const [cp] = await db.select().from(executionDispatchCheckpoints).where(eq(executionDispatchCheckpoints.runId, ctx.runId));
      seen.push(cp!.stage);
      await ctx.onSpawn?.({ pid: process.pid, processGroupId: null, startedAt: new Date().toISOString() });
      const [after] = await db.select().from(executionDispatchCheckpoints).where(eq(executionDispatchCheckpoints.runId, ctx.runId));
      seen.push(after!.stage);
      return { ...okResult, sessionId: "sess-hook" };
    });
    const queued = await heartbeat.invoke(s.agentId, "on_demand", { issueId: s.issueId }, "manual");
    const finished = await waitForRun(heartbeat, queued!.id);
    expect(finished?.status).toBe("succeeded");
    expect(seen).toEqual(["dispatching", "provider_started"]);
    await drainHeartbeatRunsToQuiescence(db, heartbeat);
    await heartbeat.drainActiveRunExecutions();
    const [cp] = await db.select().from(executionDispatchCheckpoints).where(eq(executionDispatchCheckpoints.runId, queued!.id));
    expect(cp).toMatchObject({ stage: "completed", providerRef: "sess-hook" });
  });

  it("F3: a stale fence at spawn keeps the pid on the run and stops the provider through the execution control", async () => {
    const s = await seed();
    let aborted = false;
    execute.mockImplementation(async (ctx) => {
      await db.update(executionDispatchCheckpoints).set({ recoveryState: "recovery_open" })
        .where(eq(executionDispatchCheckpoints.runId, ctx.runId));
      await ctx.onSpawn?.({ pid: process.pid, processGroupId: null, startedAt: new Date().toISOString() });
      aborted = ctx.signal?.aborted === true;
      const [row] = await db.select({ pid: heartbeatRuns.processPid }).from(heartbeatRuns).where(eq(heartbeatRuns.id, ctx.runId));
      expect(row?.pid).toBe(process.pid);
      return { ...okResult, exitCode: 1, errorMessage: "provider stopped" };
    });
    const queued = await heartbeat.invoke(s.agentId, "on_demand", { issueId: s.issueId }, "manual");
    const finished = await waitForRun(heartbeat, queued!.id);
    expect(aborted).toBe(true);
    expect(finished?.processPid).toBe(process.pid);
    expect(finished?.status).not.toBe("running");
  });

  it("F3b: a stale fence at spawn terminates the real provider process group, not just the abort signal", async () => {
    const s = await seed();
    const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { detached: true, stdio: "ignore" });
    const childPid = child.pid!;
    const isAlive = (pid: number) => {
      try { process.kill(pid, 0); return true; } catch { return false; }
    };
    let aliveAfterSpawnHook = true;
    let aliveGroupAfterSpawnHook = true;
    try {
      execute.mockImplementation(async (ctx) => {
        await db.update(executionDispatchCheckpoints).set({ recoveryState: "recovery_open" })
          .where(eq(executionDispatchCheckpoints.runId, ctx.runId));
        await ctx.onSpawn?.({ pid: childPid, processGroupId: childPid, startedAt: new Date().toISOString() });
        const deadline = Date.now() + 8_000;
        while (Date.now() < deadline && isAlive(childPid)) await new Promise((resolve) => setTimeout(resolve, 50));
        aliveAfterSpawnHook = isAlive(childPid);
        try { process.kill(-childPid, 0); aliveGroupAfterSpawnHook = true; } catch { aliveGroupAfterSpawnHook = false; }
        const [row] = await db.select({ pid: heartbeatRuns.processPid, pgid: heartbeatRuns.processGroupId })
          .from(heartbeatRuns).where(eq(heartbeatRuns.id, ctx.runId));
        expect(row).toMatchObject({ pid: childPid, pgid: childPid });
        return { ...okResult, exitCode: 1, errorMessage: "provider stopped" };
      });
      const queued = await heartbeat.invoke(s.agentId, "on_demand", { issueId: s.issueId }, "manual");
      const finished = await waitForRun(heartbeat, queued!.id);
      expect(aliveAfterSpawnHook).toBe(false);
      expect(aliveGroupAfterSpawnHook).toBe(false);
      expect(finished?.status).not.toBe("running");
    } finally {
      try { process.kill(-childPid, "SIGKILL"); } catch { /* already gone */ }
    }
  }, 30_000);

  it("F6: when the intent cannot be saved the run fails with its own error code and the provider is never called", async () => {
    const s = await seed();
    execute.mockResolvedValue(okResult);
    await db.execute(sql.raw(`create or replace function public.dispatch_hook_fail() returns trigger language plpgsql as $$
      begin raise exception 'forced checkpoint failure'; end $$`));
    await db.execute(sql.raw(`create trigger dispatch_hook_fail before insert on execution_dispatch_checkpoints for each row execute function public.dispatch_hook_fail()`));
    try {
      const queued = await heartbeat.invoke(s.agentId, "on_demand", { issueId: s.issueId }, "manual");
      const finished = await waitForRun(heartbeat, queued!.id);
      expect(finished?.status).toBe("failed");
      expect(finished?.errorCode).toBe("dispatch_checkpoint_unavailable");
      expect(execute).not.toHaveBeenCalled();
    } finally {
      await db.execute(sql.raw(`drop trigger if exists dispatch_hook_fail on execution_dispatch_checkpoints`));
      await db.execute(sql.raw(`drop function if exists public.dispatch_hook_fail()`));
    }
  });

  it("F2: another run taking a generation on the same issue mid-flight does not fence a healthy run", async () => {
    const s = await seed();
    const otherRunId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: otherRunId, companyId: s.companyId, agentId: s.agentId, status: "succeeded", invocationSource: "on_demand",
      contextSnapshot: { issueId: s.issueId },
    });
    let ownGeneration = 0, otherGeneration = 0;
    let firstRunId: string | null = null;
    execute.mockImplementation(async (ctx) => {
      if (firstRunId && ctx.runId !== firstRunId) return okResult;
      firstRunId = ctx.runId;
      const [own] = await db.select().from(executionDispatchCheckpoints).where(eq(executionDispatchCheckpoints.runId, ctx.runId));
      ownGeneration = own!.leaseGeneration;
      const other = await recordDispatchIntent(db, { runId: otherRunId, companyId: s.companyId, agentId: s.agentId, issueId: s.issueId });
      otherGeneration = other.leaseGeneration;
      await ctx.onSpawn?.({ pid: process.pid, processGroupId: null, startedAt: new Date().toISOString() });
      return { ...okResult, sessionId: "sess-overlap" };
    });
    const queued = await heartbeat.invoke(s.agentId, "on_demand", { issueId: s.issueId }, "manual");
    const finished = await waitForRun(heartbeat, queued!.id);
    await heartbeat.drainActiveRunExecutions();
    expect(finished?.status).toBe("succeeded");
    expect(otherGeneration).toBeGreaterThan(ownGeneration);
    const [cp] = await db.select().from(executionDispatchCheckpoints).where(eq(executionDispatchCheckpoints.runId, queued!.id));
    expect(cp).toMatchObject({ stage: "completed", providerRef: "sess-overlap", recoveryState: "none" });
  });
});
