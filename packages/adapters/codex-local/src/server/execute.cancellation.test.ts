import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";
import type { CommandManagedRuntimeRunner } from "@paperclipai/adapter-utils/command-managed-runtime";
import type * as ExecutionTargetModule from "@paperclipai/adapter-utils/execution-target";
import type * as ServerUtilsModule from "@paperclipai/adapter-utils/server-utils";

const seams = vi.hoisted(() => ({
  restoreWorkspace: vi.fn(async () => {}),
}));
vi.mock("@paperclipai/adapter-utils/execution-target", async () => {
  const actual = await vi.importActual<typeof ExecutionTargetModule>(
    "@paperclipai/adapter-utils/execution-target",
  );
  return {
    ...actual,
    prepareAdapterExecutionTargetRuntime: async (input: { workspaceLocalDir: string }) => ({
      workspaceRemoteDir: input.workspaceLocalDir,
      assetDirs: { home: path.join(input.workspaceLocalDir, "home") },
      restoreWorkspace: seams.restoreWorkspace,
    }),
    ensureAdapterExecutionTargetRuntimeCommandInstalled: async () => {},
    ensureAdapterExecutionTargetCommandResolvable: async () => {},
    resolveAdapterExecutionTargetCommandForLogs: async (command: string) => command,
    runAdapterExecutionTargetShellCommand: async () => ({ exitCode: 1, timedOut: false, stdout: "", stderr: "" }),
    startAdapterExecutionTargetPaperclipBridge: async () => null,
  };
});
vi.mock("@paperclipai/adapter-utils/server-utils", async () => {
  const actual = await vi.importActual<typeof ServerUtilsModule>(
    "@paperclipai/adapter-utils/server-utils",
  );
  return { ...actual, readPaperclipRuntimeSkillEntries: async () => [] };
});

import { execute } from "./execute.js";

function deferred<T>() {
  return Promise.withResolvers<T>();
}

const roots: string[] = [];
afterEach(async () => {
  seams.restoreWorkspace.mockReset().mockResolvedValue(undefined);
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "paperclip-cli-cancel-"));
  roots.push(root);
  const home = path.join(root, "home");
  await mkdir(home);
  await writeFile(path.join(home, "auth.json"), "{}");
  const command = path.join(root, "bounded-cli.cjs");
  const marker = path.join(root, "alive.txt");
  // This regression deliberately runs an independent OS process; Vitest fake
  // time cannot drive its timers. Assertions await process exit, never a sleep.
  await writeFile(command, `#!/usr/bin/env node
const fs = require("node:fs");
const marker = ${JSON.stringify(marker)};
let sequence = 0;
function alive() {
  const record = JSON.stringify({ probe: "alive", pid: process.pid, sequence: sequence++, at: Date.now() }) + "\\n";
  fs.appendFileSync(marker, record);
  process.stdout.write(record);
}
alive();
setInterval(alive, 25);
setTimeout(() => process.exit(2), 10000);
`);
  await chmod(command, 0o700);
  const started = deferred<void>();
  const exited = deferred<void>();
  const controller = new AbortController();
  let child: ChildProcessWithoutNullStreams | undefined;
  let registered = false;
  const runner: CommandManagedRuntimeRunner = {
    execute: async (input) => {
      child = spawn(input.command, input.args ?? [], {
        cwd: root,
        env: { ...process.env, ...input.env },
        stdio: ["pipe", "pipe", "pipe"],
      });
      let stdout = "";
      let stderr = "";
      child.stdout.on("data", (chunk: Buffer) => {
        stdout += chunk.toString();
        void input.onLog?.("stdout", chunk.toString());
        started.resolve();
      });
      child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
      child.stdin.end(input.stdin);
      const completion = Promise.withResolvers<Awaited<ReturnType<CommandManagedRuntimeRunner["execute"]>>>();
      child.once("error", completion.reject);
      child.once("close", (exitCode, signal) => {
        exited.resolve();
        completion.resolve({ exitCode, signal, timedOut: false, stdout, stderr });
      });
      return await completion.promise;
    },
  };
  const ctx: AdapterExecutionContext = {
    runId: "cancel-regression",
    agent: { id: "agent", companyId: "company", name: "fixture", adapterType: "codex_local", adapterConfig: {} },
    runtime: { sessionId: null, sessionParams: null, sessionDisplayId: null, taskKey: null },
    config: { engine: "cli", command, cwd: root, outputInactivityTimeoutMs: null, env: { CODEX_HOME: home } },
    context: {},
    executionTarget: { kind: "remote", transport: "sandbox", providerKey: "fixture", remoteCwd: root, runner },
    signal: controller.signal,
    onCancellationReady: async () => { registered = true; },
    stopRemoteExecution: async () => {
      if (child && child.exitCode === null && child.signalCode === null) {
        child.kill("SIGTERM");
        await exited.promise;
      }
    },
    onLog: async () => {},
  };
  return {
    ctx, controller, started, exited, marker,
    child: () => child,
    registered: () => registered,
    cleanup: async () => {
      if (child && child.exitCode === null && child.signalCode === null) {
        child.kill("SIGKILL");
        await exited.promise;
      }
    },
  };
}

describe("remote CLI Stop ownership", () => {
  it("stops the running process and waits for provider proof and adapter cleanup before settling", async () => {
    const f = await fixture();
    const stopped = deferred<void>();
    const receipt = deferred<void>();
    const cleanupStarted = deferred<void>();
    const cleanup = deferred<void>();
    const stop = f.ctx.stopRemoteExecution!;
    f.ctx.stopRemoteExecution = async () => {
      await stop();
      stopped.resolve();
      await receipt.promise;
    };
    seams.restoreWorkspace.mockImplementation(async () => {
      cleanupStarted.resolve();
      await cleanup.promise;
    });
    let settled = false;
    const outcome = execute(f.ctx).catch((error: unknown) => error).finally(() => { settled = true; });
    try {
      await Promise.race([
        f.started.promise,
        outcome.then((result) => { throw new Error("CLI ended before the fixture started", { cause: result }); }),
      ]);
      expect(f.registered()).toBe(true);
      const reason = new Error("Operator Stop");
      f.controller.abort(reason);
      await stopped.promise;
      expect(f.child()?.signalCode).toBe("SIGTERM");
      const markerAtExit = await readFile(f.marker, "utf8");
      expect(settled).toBe(false);
      receipt.resolve();
      await cleanupStarted.promise;
      expect(settled).toBe(false);
      cleanup.resolve();
      expect(await outcome).toBe(reason);
      expect(await readFile(f.marker, "utf8")).toBe(markerAtExit);
    } finally {
      receipt.resolve();
      cleanup.resolve();
      await f.cleanup();
      await outcome;
    }
  });

  it("does not start remote work when Stop wins the readiness race", async () => {
    const f = await fixture();
    const ready = deferred<void>();
    const releaseReadiness = deferred<void>();
    f.ctx.onCancellationReady = async () => {
      ready.resolve();
      await releaseReadiness.promise;
    };
    const outcome = execute(f.ctx).catch((error: unknown) => error);
    const reason = new Error("Stopped during startup");
    await ready.promise;
    f.controller.abort(reason);
    releaseReadiness.resolve();
    try {
      expect(await outcome).toBe(reason);
      await expect(readFile(f.marker)).rejects.toMatchObject({ code: "ENOENT" });
      expect(f.child()).toBeUndefined();
    } finally {
      await f.cleanup();
    }
  });
});
