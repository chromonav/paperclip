import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import {
  agents, companies, createDb, executionDispatchCheckpoints, heartbeatRuns,
  activityLog, issueExecutionFences, issueRecoveryActions, issues,
} from "@paperclipai/db";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "../__tests__/helpers/embedded-postgres.js";
import {
  StaleDispatchFenceError, advanceDispatchCheckpoint, completeDispatchCheckpointOnExit, listDispatchDiagnostics,
  reconcileAbandonedDispatchCheckpoints as sweep, recordDispatchIntent, shouldCheckpointDispatch,
} from "./execution-dispatch-checkpoints.js";
import { validateExecutionReconciliation, markExecutionReconciliation, deliverReconciledExecutions } from "./execution-recovery-resolution.js";

const support = await getEmbeddedPostgresTestSupport();
(support.supported ? describe : describe.skip)("durable dispatch checkpoints and fenced recovery", () => {
  let database: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  beforeAll(async () => {
    database = await startEmbeddedPostgresTestDatabase("dispatch-checkpoint-");
    db = createDb(database.connectionString);
  }, 60000);
  afterAll(async () => { await database?.cleanup(); });

  async function seed(opts: { status?: string; errorCode?: string | null } = {}) {
    const companyId = randomUUID(), agentId = randomUUID(), issueId = randomUUID();
    await db.insert(companies).values({ id: companyId, name: "Dispatch test", issuePrefix: `D${companyId.slice(0, 7)}` });
    await db.insert(agents).values({ id: agentId, companyId, name: "Agent", role: "general", adapterType: "claude_local", status: "idle" });
    await db.insert(issues).values({ id: issueId, companyId, title: "Work", status: "in_progress", priority: "medium",
      assigneeAgentId: agentId, issueNumber: 1, identifier: `D-${issueId.slice(0, 8)}` });
    const run = await newRun(companyId, agentId, issueId, opts);
    return { companyId, agentId, issueId, run };
  }
  async function newRun(companyId: string, agentId: string, issueId: string, opts: { status?: string; errorCode?: string | null } = {}) {
    const [run] = await db.insert(heartbeatRuns).values({
      companyId, agentId, status: opts.status ?? "running", errorCode: opts.errorCode ?? null,
      contextSnapshot: { issueId },
    }).returning();
    await db.update(issues).set({ executionRunId: run!.id, executionLockedAt: new Date() }).where(eq(issues.id, issueId));
    return run!;
  }
  async function abandon(runId: string, errorCode = "process_lost", status = "failed") {
    await db.update(heartbeatRuns).set({ status, errorCode, finishedAt: new Date() }).where(eq(heartbeatRuns.id, runId));
  }
  const intentFor = (s: Awaited<ReturnType<typeof seed>>) => recordDispatchIntent(db, {
    runId: s.run.id, companyId: s.companyId, agentId: s.agentId, issueId: s.issueId,
  });
  const checkpoint = async (runId: string) =>
    (await db.select().from(executionDispatchCheckpoints).where(eq(executionDispatchCheckpoints.runId, runId)))[0]!;
  const actions = (issueId: string) => db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.sourceIssueId, issueId));

  it("persists one idempotent intent and a single fence generation before dispatch", async () => {
    const s = await seed();
    const [a, b] = await Promise.all([intentFor(s), intentFor(s)]);
    expect(a.runId).toBe(b.runId);
    expect(a.leaseGeneration).toBe(1);
    expect(b.leaseGeneration).toBe(1);
    expect(a).toMatchObject({ stage: "intent", idempotencyKey: `dispatch:${s.run.id}`, recoveryState: "none" });
    const rows = await db.select().from(executionDispatchCheckpoints).where(eq(executionDispatchCheckpoints.issueId, s.issueId));
    expect(rows).toHaveLength(1);
    expect((await checkpoint(s.run.id)).recoveryState).toBe("none");
  });

  it("F2: overlapping runs on one issue each get their own generation and both complete", async () => {
    const s = await seed();
    const first = await intentFor(s);
    const secondRun = await newRun(s.companyId, s.agentId, s.issueId);
    const second = await recordDispatchIntent(db, { runId: secondRun.id, companyId: s.companyId, agentId: s.agentId, issueId: s.issueId });
    expect(second.leaseGeneration).toBeGreaterThan(first.leaseGeneration);
    for (const cp of [first, second]) {
      await advanceDispatchCheckpoint(db, { runId: cp.runId, generation: cp.leaseGeneration, stage: "dispatching" });
    }
    await advanceDispatchCheckpoint(db, { runId: first.runId, generation: first.leaseGeneration, stage: "provider_started" });
    await advanceDispatchCheckpoint(db, { runId: second.runId, generation: second.leaseGeneration, stage: "provider_started" });
    await advanceDispatchCheckpoint(db, { runId: first.runId, generation: first.leaseGeneration, stage: "provider_returned" });
    await advanceDispatchCheckpoint(db, { runId: second.runId, generation: second.leaseGeneration, stage: "provider_returned" });
    expect((await checkpoint(first.runId)).stage).toBe("provider_returned");
    expect((await checkpoint(second.runId)).stage).toBe("provider_returned");
  });

  it("concurrent intents for different runs on one issue get distinct generations", async () => {
    const s = await seed();
    const runs = [s.run, await newRun(s.companyId, s.agentId, s.issueId), await newRun(s.companyId, s.agentId, s.issueId)];
    const cps = await Promise.all(runs.map((r) => recordDispatchIntent(db, {
      runId: r.id, companyId: s.companyId, agentId: s.agentId, issueId: s.issueId,
    })));
    expect(new Set(cps.map((c) => c.leaseGeneration)).size).toBe(3);
  });

  it("rejects a holder once its own run is fenced by recovery", async () => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "dispatching" });
    await abandon(s.run.id);
    await sweep(db, { graceMs: 0 });
    await expect(advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_returned" }))
      .rejects.toBeInstanceOf(StaleDispatchFenceError);
    expect((await checkpoint(s.run.id)).stage).toBe("dispatching");
    await expect(advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration + 7, stage: "provider_returned" }))
      .rejects.toBeInstanceOf(StaleDispatchFenceError);
  });

  it("never moves a checkpoint backwards", async () => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_started" });
    await expect(advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "dispatching" }))
      .rejects.toBeInstanceOf(StaleDispatchFenceError);
  });

  it("window 1: before dispatch is proven safe and yields exactly one continuation", async () => {
    const s = await seed();
    await intentFor(s);
    await abandon(s.run.id, "server_shutdown_interrupted", "interrupted");
    const wake = vi.fn(async () => { const r = await newRun(s.companyId, s.agentId, s.issueId, { status: "queued" }); return r; });
    const [one, two] = await Promise.all([sweep(db, { graceMs: 0 }), sweep(db, { graceMs: 0 })]);
    expect(one.autoSettled + two.autoSettled).toBe(1);
    const rows = await actions(s.issueId);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ status: "resolved", outcome: "handed_back" });
    expect((await checkpoint(s.run.id)).recoveryState).toBe("continuation_pending");
    const [task] = await db.select().from(issues).where(eq(issues.id, s.issueId));
    expect(task!.executionRunId).toBeNull();
    await deliverReconciledExecutions(db, wake as never);
    await deliverReconciledExecutions(db, wake as never);
    expect(wake).toHaveBeenCalledTimes(1);
    const done = await checkpoint(s.run.id);
    expect(done.recoveryState).toBe("continued");
    expect(done.continuationRunId).toBeTruthy();
  });

  it.each([
    ["window 2: in provider", "provider_started"],
    ["window 3: dispatching handoff", "dispatching"],
    ["window 4: after provider, before checkpoint", "provider_returned"],
  ] as const)("%s opens exactly one governed recovery action and never replays", async (_name, stage) => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage, providerRef: "sess-1" });
    await abandon(s.run.id);
    const results = await Promise.all([sweep(db, { graceMs: 0 }), sweep(db, { graceMs: 0 })]);
    expect(results.reduce((n, r) => n + r.opened, 0)).toBe(1);
    const rows = await actions(s.issueId);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ status: "active", ownerType: "board", cause: "legacy_execution_requires_reconciliation" });
    expect(rows[0]!.evidence).toMatchObject({ runId: s.run.id, dispatchCheckpoint: { stage, providerRef: "sess-1", leaseGeneration: 1 } });
    expect(rows[0]!.evidence.continuationDelivery).toBeUndefined();
    const after = await checkpoint(s.run.id);
    expect(after).toMatchObject({ recoveryState: "recovery_open", recoveryActionId: rows[0]!.id });
    await expect(advanceDispatchCheckpoint(db, { runId: s.run.id, generation: 1, stage: "provider_returned" }))
      .rejects.toBeInstanceOf(StaleDispatchFenceError);
  });

  it("window 5: host restart (lease reaped) is detected from the abandoned-run error code", async () => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_started" });
    expect(await sweep(db, { graceMs: 0 })).toMatchObject({ opened: 0, autoSettled: 0 });
    await abandon(s.run.id, "process_lost");
    expect((await sweep(db, { graceMs: 0 })).opened).toBe(1);
    const diag = await listDispatchDiagnostics(db, s.companyId, s.issueId);
    expect(diag.fenceGeneration).toBe(1);
    expect(diag.checkpoints[0]).toMatchObject({
      runId: s.run.id, stage: "provider_started", leaseGeneration: 1, stale: true,
      recoveryState: "recovery_open", runStatus: "failed",
    });
    expect(diag.checkpoints[0]!.recoveryActionId).toBeTruthy();
  });

  it("does not touch a run the owning executor finished", async () => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_returned" });
    await abandon(s.run.id, "adapter_failed");
    await completeDispatchCheckpointOnExit(db, s.run.id);
    expect((await checkpoint(s.run.id)).stage).toBe("completed");
    expect(await sweep(db, { graceMs: 0 })).toEqual({ opened: 0, autoSettled: 0, skipped: 0 });
    expect(await actions(s.issueId)).toHaveLength(0);
  });

  it("refuses to mark a lost-run checkpoint completed when the reaper ended it", async () => {
    const s = await seed();
    await intentFor(s);
    await abandon(s.run.id, "process_lost");
    await completeDispatchCheckpointOnExit(db, s.run.id);
    expect((await checkpoint(s.run.id)).stage).toBe("intent");
  });

  it("refuses reconciliation that contradicts recorded side effects or provider return", async () => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, {
      runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_started",
      sideEffect: { kind: "aws.iam.create_user", ref: "arn:aws:iam::1:user/x", idempotent: false },
    });
    await abandon(s.run.id);
    await sweep(db, { graceMs: 0 });
    const [action] = await actions(s.issueId);
    const base = { db: db as never, companyId: s.companyId, issueId: s.issueId, agentId: s.agentId, sourceRunId: s.run.id };
    await expect(validateExecutionReconciliation({ ...base, decision: {
      runId: s.run.id, providerStopped: true, actionOutcome: "not_performed", outcomeEvidence: "Nothing was changed in AWS at all.",
    } })).rejects.toThrow(/side effects/);
    await expect(validateExecutionReconciliation({ ...base, decision: undefined })).rejects.toThrow(/Reconcile/);
    const ok = await validateExecutionReconciliation({ ...base, decision: {
      runId: s.run.id, providerStopped: true, actionOutcome: "mixed", outcomeEvidence: "IAM user exists; verified via CloudTrail event.",
    } });
    expect(ok.id).toBe(s.run.id);
    await markExecutionReconciliation(db as never, action!, {
      runId: s.run.id, providerStopped: true, actionOutcome: "mixed", outcomeEvidence: "IAM user exists; verified via CloudTrail event.",
    }, "operator-1");
    expect((await checkpoint(s.run.id)).recoveryState).toBe("continuation_pending");

    const s2 = await seed();
    const cp2 = await recordDispatchIntent(db, { runId: s2.run.id, companyId: s2.companyId, agentId: s2.agentId, issueId: s2.issueId });
    await advanceDispatchCheckpoint(db, { runId: s2.run.id, generation: cp2.leaseGeneration, stage: "provider_returned" });
    await expect(validateExecutionReconciliation({ db: db as never, companyId: s2.companyId, issueId: s2.issueId,
      agentId: s2.agentId, sourceRunId: s2.run.id, decision: {
        runId: s2.run.id, providerStopped: true, actionOutcome: "not_performed", outcomeEvidence: "Operator believes nothing happened here.",
      } })).rejects.toThrow(/provider returned/);
  });

  it("F10: leaves an unrelated active action untouched and waits behind it", async () => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_started" });
    const [existing] = await db.insert(issueRecoveryActions).values({
      companyId: s.companyId, sourceIssueId: s.issueId, kind: "active_run_watchdog", cause: "uncertain_provider_action",
      fingerprint: "pre-existing", nextAction: "inspect",
    }).returning();
    await abandon(s.run.id);
    expect((await sweep(db, { graceMs: 0 })).opened).toBe(1);
    const rows = await actions(s.issueId);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.id).toBe(existing!.id);
    expect(rows[0]!.evidence).not.toHaveProperty("dispatchCheckpoint");
    expect(await checkpoint(s.run.id)).toMatchObject({ recoveryState: "recovery_open", recoveryActionId: null });
    await expect(advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_returned" }))
      .rejects.toBeInstanceOf(StaleDispatchFenceError);

    const decision = { runId: s.run.id, providerStopped: true as const, actionOutcome: "mixed" as const, outcomeEvidence: "Unrelated repair finished; nothing verified for this run." };
    await markExecutionReconciliation(db as never, existing!, decision, "operator-1");
    expect(await checkpoint(s.run.id)).toMatchObject({ recoveryState: "recovery_open", recoveryActionId: null });

    await db.update(issueRecoveryActions).set({ status: "resolved", resolvedAt: new Date(), outcome: "handed_back" })
      .where(eq(issueRecoveryActions.id, existing!.id));
    expect((await sweep(db, { graceMs: 0 })).opened).toBe(1);
    const after = await actions(s.issueId);
    expect(after).toHaveLength(2);
    const own = after.find((row) => row.id !== existing!.id)!;
    expect(own).toMatchObject({ status: "active", cause: "legacy_execution_requires_reconciliation" });
    expect(await checkpoint(s.run.id)).toMatchObject({ recoveryState: "recovery_open", recoveryActionId: own.id });
  });

  it("F1/F2: a healthy retry that took its own generation survives the sweep of its abandoned predecessor", async () => {
    const s = await seed();
    await intentFor(s);
    await abandon(s.run.id, "process_lost");
    const retry = await newRun(s.companyId, s.agentId, s.issueId);
    await db.update(heartbeatRuns).set({ retryOfRunId: s.run.id }).where(eq(heartbeatRuns.id, retry.id));
    const live = await recordDispatchIntent(db, { runId: retry.id, companyId: s.companyId, agentId: s.agentId, issueId: s.issueId });
    await advanceDispatchCheckpoint(db, { runId: retry.id, generation: live.leaseGeneration, stage: "dispatching" });
    await advanceDispatchCheckpoint(db, { runId: retry.id, generation: live.leaseGeneration, stage: "provider_started" });
    const [fenceBefore] = await db.select().from(issueExecutionFences).where(eq(issueExecutionFences.issueId, s.issueId));
    expect(await sweep(db, { graceMs: 0 })).toMatchObject({ autoSettled: 1, opened: 0 });
    const [fenceAfter] = await db.select().from(issueExecutionFences).where(eq(issueExecutionFences.issueId, s.issueId));
    expect(fenceAfter!.generation).toBe(fenceBefore!.generation);
    const done = await advanceDispatchCheckpoint(db, { runId: retry.id, generation: live.leaseGeneration, stage: "provider_returned", providerRef: "sess-live" });
    expect(done.stage).toBe("provider_returned");
    expect(await checkpoint(s.run.id)).toMatchObject({ recoveryState: "continued", continuationRunId: retry.id });
  });

  it("F1: the sweep of an abandoned run never fences a different live run on the same issue", async () => {
    const s = await seed();
    const dead = await intentFor(s);
    await advanceDispatchCheckpoint(db, { runId: s.run.id, generation: dead.leaseGeneration, stage: "provider_started" });
    const live = await newRun(s.companyId, s.agentId, s.issueId);
    const liveCp = await recordDispatchIntent(db, { runId: live.id, companyId: s.companyId, agentId: s.agentId, issueId: s.issueId });
    await advanceDispatchCheckpoint(db, { runId: live.id, generation: liveCp.leaseGeneration, stage: "provider_started" });
    await abandon(s.run.id, "process_lost");
    await sweep(db, { graceMs: 0 });
    const done = await advanceDispatchCheckpoint(db, { runId: live.id, generation: liveCp.leaseGeneration, stage: "provider_returned" });
    expect(done.stage).toBe("provider_returned");
  });

  it("F5: a scheduled_retry successor counts as the one continuation", async () => {
    const s = await seed();
    await intentFor(s);
    await abandon(s.run.id, "process_lost");
    const [scheduled] = await db.insert(heartbeatRuns).values({
      companyId: s.companyId, agentId: s.agentId, status: "scheduled_retry", retryOfRunId: s.run.id,
      contextSnapshot: { issueId: s.issueId },
    }).returning();
    expect(await sweep(db, { graceMs: 0 })).toMatchObject({ autoSettled: 1, opened: 0 });
    expect(await actions(s.issueId)).toHaveLength(0);
    expect(await checkpoint(s.run.id)).toMatchObject({ recoveryState: "continued", continuationRunId: scheduled!.id });
  });

  it("F5: the sweep gives the reaper a grace period to enqueue its own retry first", async () => {
    const s = await seed();
    await intentFor(s);
    await abandon(s.run.id, "process_lost");
    expect(await sweep(db)).toEqual({ opened: 0, autoSettled: 0, skipped: 0 });
    expect((await checkpoint(s.run.id)).recoveryState).toBe("none");
    const [scheduled] = await db.insert(heartbeatRuns).values({
      companyId: s.companyId, agentId: s.agentId, status: "scheduled_retry", retryOfRunId: s.run.id,
      contextSnapshot: { issueId: s.issueId },
    }).returning();
    expect(await sweep(db, { now: new Date(Date.now() + 60_000) })).toMatchObject({ autoSettled: 1 });
    expect(await actions(s.issueId)).toHaveLength(0);
    expect((await checkpoint(s.run.id)).continuationRunId).toBe(scheduled!.id);
  });

  it("F7: rows for long-dead runs are not recovered automatically", async () => {
    const s = await seed();
    await intentFor(s);
    await db.update(heartbeatRuns).set({
      status: "failed", errorCode: "process_lost", finishedAt: new Date(Date.now() - 30 * 24 * 3600 * 1000),
    }).where(eq(heartbeatRuns.id, s.run.id));
    expect(await sweep(db, { graceMs: 0 })).toEqual({ opened: 0, autoSettled: 0, skipped: 0 });
    expect(await actions(s.issueId)).toHaveLength(0);
    expect((await checkpoint(s.run.id)).recoveryState).toBe("none");
  });

  it("F7: a candidate that keeps failing is pushed behind newer ones and does not block them", async () => {
    const bad = await seed();
    const badCp = await intentFor(bad);
    await advanceDispatchCheckpoint(db, { runId: bad.run.id, generation: badCp.leaseGeneration, stage: "provider_started" });
    await abandon(bad.run.id, "process_lost");
    const old = new Date(Date.now() - 3600_000);
    await db.update(executionDispatchCheckpoints).set({ createdAt: old, updatedAt: old }).where(eq(executionDispatchCheckpoints.runId, bad.run.id));
    const good = await seed();
    const goodCp = await intentFor(good);
    await advanceDispatchCheckpoint(db, { runId: good.run.id, generation: goodCp.leaseGeneration, stage: "provider_started" });
    await abandon(good.run.id, "process_lost");
    await db.execute(sql.raw(`create or replace function public.dispatch_test_fail() returns trigger language plpgsql as $$
      begin if new.source_issue_id = '${bad.issueId}' then raise exception 'forced recovery failure'; end if; return new; end $$`));
    await db.execute(sql.raw(`create trigger dispatch_test_fail before insert on issue_recovery_actions for each row execute function public.dispatch_test_fail()`));
    try {
      const first = await sweep(db, { graceMs: 0 });
      expect(first.opened).toBe(1);
      expect((await checkpoint(good.run.id)).recoveryState).toBe("recovery_open");
      const failed = await checkpoint(bad.run.id);
      expect(failed.recoveryState).toBe("none");
      expect(failed.updatedAt.getTime()).toBeGreaterThan(old.getTime() + 60_000);
    } finally {
      await db.execute(sql.raw(`drop trigger if exists dispatch_test_fail on issue_recovery_actions`));
      await db.execute(sql.raw(`drop function if exists public.dispatch_test_fail()`));
    }
    expect((await sweep(db, { graceMs: 0 })).opened).toBe(1);
    expect((await checkpoint(bad.run.id)).recoveryState).toBe("recovery_open");
  });

  it("F8: auto-settle releases both the execution lock and the checkout holder", async () => {
    const s = await seed();
    await intentFor(s);
    await db.update(issues).set({ checkoutRunId: s.run.id }).where(eq(issues.id, s.issueId));
    await abandon(s.run.id, "server_shutdown_interrupted", "interrupted");
    expect((await sweep(db, { graceMs: 0 })).autoSettled).toBe(1);
    const [task] = await db.select().from(issues).where(eq(issues.id, s.issueId));
    expect(task).toMatchObject({ executionRunId: null, checkoutRunId: null });
  });

  it("F9: the continuation_pending mark follows the saved decision, not a failed write", async () => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_started" });
    await abandon(s.run.id);
    await sweep(db, { graceMs: 0 });
    const [action] = await actions(s.issueId);
    const decision = { runId: s.run.id, providerStopped: true as const, actionOutcome: "mixed" as const, outcomeEvidence: "Operator verified the provider state by hand." };
    await expect(markExecutionReconciliation(db as never, { ...action!, id: randomUUID() } as never, decision, "op")).resolves.toBeUndefined();
    expect((await checkpoint(s.run.id)).recoveryState).toBe("recovery_open");
    await markExecutionReconciliation(db as never, action!, decision, "op");
    expect((await checkpoint(s.run.id)).recoveryState).toBe("continuation_pending");
  });

  it("F13: a run with no eligible owner records a distinct settled-without-continuation activity", async () => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_started" });
    await db.update(issues).set({ status: "done" }).where(eq(issues.id, s.issueId));
    await abandon(s.run.id);
    await sweep(db, { graceMs: 0 });
    const rows = await db.select().from(activityLog).where(and(eq(activityLog.entityId, s.issueId), eq(activityLog.runId, s.run.id)));
    expect(rows.map((r) => r.action)).toEqual(["issue.dispatch_checkpoint_settled_without_continuation"]);
  });

  it("F15: provider references and raw side effects are returned only with explicit provider-evidence access", async () => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, {
      runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_started", providerRef: "sess-secret",
      sideEffect: { kind: "aws.iam.create_user", ref: "arn:aws:iam::1:user/x", idempotent: false },
    });
    const limited = await listDispatchDiagnostics(db, s.companyId, s.issueId);
    expect(limited.checkpoints[0]).toMatchObject({ providerRef: null, idempotencyKey: null, sideEffects: [], sideEffectCount: 1 });
    const full = await listDispatchDiagnostics(db, s.companyId, s.issueId, { includeProviderEvidence: true });
    expect(full.checkpoints[0]).toMatchObject({ providerRef: "sess-secret", idempotencyKey: `dispatch:${s.run.id}`, sideEffectCount: 1 });
    expect(full.checkpoints[0]!.sideEffects).toHaveLength(1);
  });

  it("F14: deleting the agent keeps the checkpoint as audit evidence", async () => {
    const s = await seed();
    const other = randomUUID();
    await db.insert(agents).values({ id: other, companyId: s.companyId, name: "Other", role: "general", adapterType: "claude_local", status: "idle" });
    const run2 = await newRun(s.companyId, s.agentId, s.issueId);
    await db.insert(executionDispatchCheckpoints).values({
      runId: run2.id, companyId: s.companyId, agentId: other, issueId: s.issueId, idempotencyKey: `dispatch:${run2.id}`, leaseGeneration: 99,
    });
    await db.delete(agents).where(eq(agents.id, other));
    const [kept] = await db.select().from(executionDispatchCheckpoints).where(eq(executionDispatchCheckpoints.runId, run2.id));
    expect(kept).toMatchObject({ agentId: null, leaseGeneration: 99 });
    const diag = await listDispatchDiagnostics(db, s.companyId, s.issueId);
    expect(diag.checkpoints.find((c) => c.runId === run2.id)).toMatchObject({ agentId: null, agentName: null });
  });

  it("F4: native runs are never checkpointed, by resolved kind or persisted mode", () => {
    expect(shouldCheckpointDispatch({ issueId: "i", resolvedRuntimeKind: "native", persistedRuntimeMode: "legacy" })).toBe(false);
    expect(shouldCheckpointDispatch({ issueId: "i", resolvedRuntimeKind: "legacy", persistedRuntimeMode: "native" })).toBe(false);
    expect(shouldCheckpointDispatch({ issueId: null, resolvedRuntimeKind: "legacy", persistedRuntimeMode: "legacy" })).toBe(false);
    expect(shouldCheckpointDispatch({ issueId: "i", resolvedRuntimeKind: "legacy", persistedRuntimeMode: "legacy" })).toBe(true);
  });

  it("closes cleanly without a continuation when the task owner changed", async () => {
    const s = await seed();
    const cp = await intentFor(s);
    await advanceDispatchCheckpoint(db, { runId: s.run.id, generation: cp.leaseGeneration, stage: "provider_started" });
    await db.update(issues).set({ status: "done" }).where(eq(issues.id, s.issueId));
    await abandon(s.run.id);
    await sweep(db, { graceMs: 0 });
    expect(await actions(s.issueId)).toHaveLength(0);
    expect((await checkpoint(s.run.id)).recoveryState).toBe("settled_no_continuation");
  });

  it("does not duplicate when a successor continuation already exists", async () => {
    const s = await seed();
    await intentFor(s);
    await abandon(s.run.id, "process_lost");
    const [successor] = await db.insert(heartbeatRuns).values({
      companyId: s.companyId, agentId: s.agentId, status: "queued", retryOfRunId: s.run.id, contextSnapshot: { issueId: s.issueId },
    }).returning();
    await sweep(db, { graceMs: 0 });
    expect(await actions(s.issueId)).toHaveLength(0);
    expect(await checkpoint(s.run.id)).toMatchObject({ recoveryState: "continued", continuationRunId: successor!.id });
  });
});
