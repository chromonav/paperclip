import { and, asc, desc, eq, inArray, isNull, or, sql } from "drizzle-orm";
import {
  agents,
  executionDispatchCheckpoints,
  heartbeatRuns,
  issueExecutionFences,
  issueRecoveryActions,
  issues,
  type Db,
} from "@paperclipai/db";
import { conflict } from "../errors.js";
import { persistActivity } from "./activity-log.js";
import { logger } from "../middleware/logger.js";

export const DISPATCH_CHECKPOINT_STAGES = [
  "intent",
  "dispatching",
  "provider_started",
  "provider_returned",
  "completed",
] as const;
export type DispatchCheckpointStage = (typeof DISPATCH_CHECKPOINT_STAGES)[number];

export type DispatchRecoveryState =
  | "none"
  | "recovery_open"
  | "continuation_pending"
  | "continued"
  | "settled_no_continuation";

export type DispatchSideEffect = {
  kind: string;
  ref?: string | null;
  /** False when the provider offers no idempotency key; reconciliation is mandatory. */
  idempotent: boolean;
  recordedAt?: string;
};

export const DISPATCH_RECOVERY_CAUSE = "legacy_execution_requires_reconciliation";
/** Terminal codes written by the reaper or shutdown drain, never by the owning executor. */
export const DISPATCH_ABANDONMENT_ERROR_CODES = ["process_lost", "server_shutdown_interrupted"] as const;
const SWEEP_BATCH = 25;
/** The reaper writes the failed status, then enqueues its own bounded retry in a later step. */
export const ABANDONED_SWEEP_GRACE_MS = 30_000;
/** Rows for runs that ended long ago (for example left from before a rollback) are not recovered automatically. */
export const ABANDONED_SWEEP_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const TERMINAL_RUN_STATUSES = ["failed", "timed_out", "interrupted", "cancelled"] as const;
/** Successors that mean a continuation already exists or is already scheduled. */
const CONTINUATION_SUCCESSOR_STATUSES = ["scheduled_retry", "queued", "running", "succeeded"] as const;

export class StaleDispatchFenceError extends Error {
  readonly code = "stale_dispatch_fence";
  constructor(runId: string, generation: number) {
    super(`Execution fence for run ${runId} (generation ${generation}) is stale`);
  }
}

/** Raised when the durable intent cannot be saved, so the run fails before any provider call. */
export class DispatchCheckpointUnavailableError extends Error {
  readonly code = "dispatch_checkpoint_unavailable";
  constructor(runId: string, cause: unknown) {
    super(`Dispatch checkpoint for run ${runId} could not be saved; the provider was not called.`, { cause });
  }
}

/** Native runs keep their own coordinator; only direct-adapter runs on an issue are checkpointed. */
export function shouldCheckpointDispatch(input: {
  issueId: string | null | undefined;
  resolvedRuntimeKind: string | null | undefined;
  persistedRuntimeMode: string | null | undefined;
}) {
  return Boolean(input.issueId) && input.resolvedRuntimeKind !== "native" && input.persistedRuntimeMode !== "native";
}

export function dispatchIdempotencyKey(runId: string) {
  return `dispatch:${runId}`;
}

function stageRank(stage: string) {
  return DISPATCH_CHECKPOINT_STAGES.indexOf(stage as DispatchCheckpointStage);
}

/** A provider call may have happened once the run left the pre-dispatch stages. */
export function providerMayHaveBeenEntered(stage: string) {
  return stageRank(stage) >= stageRank("dispatching");
}

type Checkpoint = typeof executionDispatchCheckpoints.$inferSelect;

/**
 * Persist the idempotent dispatch intent and allocate a unique lease generation
 * for this run. The issue row is not locked: only the per-issue allocator row is
 * held briefly, so normal dispatch does not contend with other issue writes.
 * The generation identifies one run's lease; fencing is per run (see
 * `recoveryState`), so overlapping legitimate runs never fence each other.
 * Retrying with the same run returns the same checkpoint.
 */
export async function recordDispatchIntent(db: Db, input: {
  runId: string;
  companyId: string;
  agentId: string;
  issueId: string;
  providerRef?: string | null;
}): Promise<Checkpoint> {
  const idempotencyKey = dispatchIdempotencyKey(input.runId);
  return db.transaction(async (tx) => {
    await tx.execute(sql`select set_config('lock_timeout', '5000', true)`);
    const [issue] = await tx.select({ id: issues.id }).from(issues).where(and(
      eq(issues.companyId, input.companyId), eq(issues.id, input.issueId),
    ));
    if (!issue) throw conflict("Cannot record a dispatch intent for a missing issue.");
    await tx.insert(issueExecutionFences).values({
      issueId: input.issueId, companyId: input.companyId, generation: 0,
    }).onConflictDoNothing();
    await tx.select({ generation: issueExecutionFences.generation }).from(issueExecutionFences)
      .where(eq(issueExecutionFences.issueId, input.issueId)).for("update");
    const [existing] = await tx.select().from(executionDispatchCheckpoints).where(and(
      eq(executionDispatchCheckpoints.companyId, input.companyId),
      eq(executionDispatchCheckpoints.idempotencyKey, idempotencyKey),
    ));
    if (existing) return existing;
    const [allocated] = await tx.update(issueExecutionFences).set({
      generation: sql`${issueExecutionFences.generation} + 1`, updatedAt: new Date(),
    }).where(eq(issueExecutionFences.issueId, input.issueId)).returning();
    const [created] = await tx.insert(executionDispatchCheckpoints).values({
      runId: input.runId,
      companyId: input.companyId,
      agentId: input.agentId,
      issueId: input.issueId,
      idempotencyKey,
      leaseGeneration: allocated!.generation,
      stage: "intent",
      providerRef: input.providerRef ?? null,
    }).returning();
    return created!;
  });
}

/**
 * Advance a checkpoint only while the caller holds this run's own lease
 * generation and recovery has not fenced the run (`recoveryState = 'none'`).
 * A fenced or mismatched holder gets StaleDispatchFenceError and must not
 * commit a later stage. Other runs on the same issue never affect this check.
 */
export async function advanceDispatchCheckpoint(db: Db, input: {
  runId: string;
  generation: number;
  stage: DispatchCheckpointStage;
  providerRef?: string | null;
  sideEffect?: Omit<DispatchSideEffect, "recordedAt">;
}): Promise<Checkpoint> {
  const allowedPrevious = DISPATCH_CHECKPOINT_STAGES.slice(0, stageRank(input.stage) + 1);
  const sideEffects = input.sideEffect
    ? sql`${executionDispatchCheckpoints.sideEffects} || ${JSON.stringify([{ ...input.sideEffect, recordedAt: new Date().toISOString() }])}::jsonb`
    : executionDispatchCheckpoints.sideEffects;
  const [updated] = await db.update(executionDispatchCheckpoints).set({
    stage: input.stage,
    providerRef: input.providerRef ?? sql`${executionDispatchCheckpoints.providerRef}`,
    sideEffects,
    updatedAt: new Date(),
  }).where(and(
    eq(executionDispatchCheckpoints.runId, input.runId),
    eq(executionDispatchCheckpoints.leaseGeneration, input.generation),
    eq(executionDispatchCheckpoints.recoveryState, "none"),
    inArray(executionDispatchCheckpoints.stage, [...allowedPrevious]),
  )).returning();
  if (!updated) throw new StaleDispatchFenceError(input.runId, input.generation);
  return updated;
}

/** Executor exit: a run it finished itself is not an abandoned dispatch. */
export async function completeDispatchCheckpointOnExit(db: Db, runId: string) {
  await db.update(executionDispatchCheckpoints).set({ stage: "completed", updatedAt: new Date() }).where(and(
    eq(executionDispatchCheckpoints.runId, runId),
    eq(executionDispatchCheckpoints.recoveryState, "none"),
    sql`${executionDispatchCheckpoints.stage} <> 'completed'`,
    sql`exists (select 1 from ${heartbeatRuns} r where r.id = ${executionDispatchCheckpoints.runId}
      and (r.status = 'succeeded'
        or (r.status in ('failed','timed_out','interrupted','cancelled')
            and coalesce(r.error_code, '') not in ('process_lost','server_shutdown_interrupted'))))`,
  ));
}

function diagnosticsEvidence(cp: Checkpoint, fenceGeneration: number, reason: string) {
  return {
    runId: cp.runId,
    dispatchCheckpoint: {
      runId: cp.runId,
      providerRef: cp.providerRef,
      leaseGeneration: cp.leaseGeneration,
      currentFenceGeneration: fenceGeneration,
      stage: cp.stage,
      sideEffects: cp.sideEffects,
      abandonedReason: reason,
    },
  };
}

export type AbandonedSweepResult = { opened: number; autoSettled: number; skipped: number };

const OWN_ACTION_FINGERPRINT_PREFIX = "legacy-execution:";
const openForSweep = or(
  eq(executionDispatchCheckpoints.recoveryState, "none"),
  and(eq(executionDispatchCheckpoints.recoveryState, "recovery_open"), isNull(executionDispatchCheckpoints.recoveryActionId)),
);

/**
 * Find unfinished dispatch checkpoints whose run was ended by the reaper or a
 * shutdown drain (lease expiry, host restart or outage). Fence that run's
 * checkpoint (any `recoveryState` other than `none`), then create exactly one
 * governed recovery action, or when the provider was provably never entered,
 * one reconciled continuation delivery. Other runs on the same issue are never
 * fenced. Candidates wait out a grace period so the reaper can enqueue its own
 * bounded retry first, are ordered by last attempt so a failing row cannot
 * starve the queue, and are ignored once older than the age cutoff.
 * Runs the owning executor finished are completed by the executor, not here.
 */
export async function reconcileAbandonedDispatchCheckpoints(
  db: Db,
  options: { now?: Date; graceMs?: number; maxAgeMs?: number } = {},
): Promise<AbandonedSweepResult> {
  const result: AbandonedSweepResult = { opened: 0, autoSettled: 0, skipped: 0 };
  const now = options.now ?? new Date();
  const graceCutoff = new Date(now.getTime() - (options.graceMs ?? ABANDONED_SWEEP_GRACE_MS));
  const ageCutoff = new Date(now.getTime() - (options.maxAgeMs ?? ABANDONED_SWEEP_MAX_AGE_MS));
  const endedAt = sql`coalesce(${heartbeatRuns.finishedAt}, ${heartbeatRuns.updatedAt})`;
  const candidates = await db.select({ runId: executionDispatchCheckpoints.runId, companyId: executionDispatchCheckpoints.companyId })
    .from(executionDispatchCheckpoints)
    .innerJoin(heartbeatRuns, eq(heartbeatRuns.id, executionDispatchCheckpoints.runId))
    .where(and(
      openForSweep,
      sql`${executionDispatchCheckpoints.stage} <> 'completed'`,
      inArray(heartbeatRuns.status, [...TERMINAL_RUN_STATUSES]),
      inArray(heartbeatRuns.errorCode, [...DISPATCH_ABANDONMENT_ERROR_CODES]),
      sql`${endedAt} <= ${graceCutoff.toISOString()}::timestamptz`,
      sql`${endedAt} >= ${ageCutoff.toISOString()}::timestamptz`,
    ))
    .orderBy(asc(executionDispatchCheckpoints.updatedAt), asc(executionDispatchCheckpoints.createdAt))
    .limit(SWEEP_BATCH);
  for (const candidate of candidates) {
    try {
      const outcome = await db.transaction((tx) => settleAbandonedCheckpoint(tx as unknown as Db, candidate));
      result[outcome] += 1;
    } catch (err) {
      logger.warn({ err, runId: candidate.runId }, "dispatch checkpoint recovery remains pending");
      await db.update(executionDispatchCheckpoints).set({ updatedAt: new Date() })
        .where(eq(executionDispatchCheckpoints.runId, candidate.runId)).catch(() => undefined);
    }
  }
  return result;
}

async function settleAbandonedCheckpoint(tx: Db, candidate: { runId: string; companyId: string }): Promise<keyof AbandonedSweepResult> {
  await tx.execute(sql`select set_config('statement_timeout', '15000', true), set_config('lock_timeout', '1000', true)`);
  const [pre] = await tx.select({ issueId: executionDispatchCheckpoints.issueId }).from(executionDispatchCheckpoints)
    .where(eq(executionDispatchCheckpoints.runId, candidate.runId));
  if (!pre) return "skipped";
  const [task] = await tx.select().from(issues).where(and(
    eq(issues.companyId, candidate.companyId), eq(issues.id, pre.issueId),
  )).for("update");
  const [cp] = await tx.select().from(executionDispatchCheckpoints).where(and(
    eq(executionDispatchCheckpoints.runId, candidate.runId), openForSweep,
  )).for("update");
  const [run] = await tx.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, candidate.runId)).for("update");
  if (!task || !cp || !run || cp.stage === "completed") return "skipped";
  if (!(TERMINAL_RUN_STATUSES as readonly string[]).includes(run.status)
      || !(DISPATCH_ABANDONMENT_ERROR_CODES as readonly (string | null)[]).includes(run.errorCode)) return "skipped";

  const now = new Date();
  const [successor] = await tx.select({ id: heartbeatRuns.id }).from(heartbeatRuns).where(and(
    eq(heartbeatRuns.companyId, task.companyId), eq(heartbeatRuns.retryOfRunId, run.id),
    inArray(heartbeatRuns.status, [...CONTINUATION_SUCCESSOR_STATUSES]),
  )).limit(1);
  if (successor) {
    await tx.update(executionDispatchCheckpoints).set({
      recoveryState: "continued", continuationRunId: successor.id, updatedAt: now,
    }).where(eq(executionDispatchCheckpoints.runId, cp.runId));
    return "autoSettled";
  }
  const [fence] = await tx.select().from(issueExecutionFences).where(eq(issueExecutionFences.issueId, task.id));
  const fenceGeneration = fence?.generation ?? cp.leaseGeneration;
  const reason = `${run.errorCode}:${run.status}`;
  const [active] = await tx.select().from(issueRecoveryActions).where(and(
    eq(issueRecoveryActions.companyId, task.companyId),
    eq(issueRecoveryActions.sourceIssueId, task.id),
    inArray(issueRecoveryActions.status, ["active", "escalated"]),
  )).for("update");
  const evidence = diagnosticsEvidence(cp, fenceGeneration, reason);
  const ownAction = active
    && active.cause === DISPATCH_RECOVERY_CAUSE
    && active.fingerprint === `${OWN_ACTION_FINGERPRINT_PREFIX}${run.id}`
    ? active : null;

  const eligibleOwner = task.assigneeAgentId !== null && task.assigneeAgentId === cp.agentId
    && !["done", "cancelled"].includes(task.status);
  const safeToReplay = !providerMayHaveBeenEntered(cp.stage) && eligibleOwner && !active
    && (!task.executionRunId || task.executionRunId === run.id)
    && (!task.checkoutRunId || task.checkoutRunId === run.id);

  if (safeToReplay) {
    const settledAt = now.toISOString();
    const [action] = await tx.insert(issueRecoveryActions).values({
      companyId: task.companyId,
      sourceIssueId: task.id,
      kind: "active_run_watchdog",
      status: "resolved",
      ownerType: "board",
      returnOwnerAgentId: task.assigneeAgentId,
      cause: DISPATCH_RECOVERY_CAUSE,
      fingerprint: `${OWN_ACTION_FINGERPRINT_PREFIX}${run.id}`,
      evidence: {
        ...evidence,
        executionReconciliation: {
          runId: run.id,
          providerStopped: true,
          actionOutcome: "not_performed",
          outcomeEvidence: `Dispatch checkpoint stayed at "${cp.stage}" (generation ${cp.leaseGeneration}); the provider was never entered and the run is ${run.status}.`,
          actorId: "dispatch-checkpoint",
          recordedAt: settledAt,
        },
        continuationDelivery: "pending",
      },
      nextAction: "The provider was never entered; one continuation is being delivered from the last safe checkpoint.",
      outcome: "handed_back",
      resolutionNote: "Dispatch checkpoint proves no provider action started.",
      resolvedAt: now,
    }).returning();
    if (task.executionRunId === run.id) {
      await tx.update(issues).set({ executionRunId: null, executionAgentNameKey: null, executionLockedAt: null })
        .where(eq(issues.id, task.id));
    }
    if (task.checkoutRunId === run.id) {
      await tx.update(issues).set({ checkoutRunId: null }).where(eq(issues.id, task.id));
    }
    await tx.update(executionDispatchCheckpoints).set({
      recoveryState: "continuation_pending", recoveryActionId: action!.id, updatedAt: now,
    }).where(eq(executionDispatchCheckpoints.runId, cp.runId));
    await persistActivity(tx, {
      companyId: task.companyId, actorType: "system", actorId: "dispatch-checkpoint",
      action: "issue.dispatch_checkpoint_reconciled", entityType: "issue", entityId: task.id, runId: run.id,
      details: { recoveryActionId: action!.id, stage: cp.stage, leaseGeneration: cp.leaseGeneration, fenceGeneration, reason },
    });
    return "autoSettled";
  }

  if (active && !ownAction) {
    // A different governed action already owns this issue. Keep it intact and wait
    // behind it; the next sweep reopens this checkpoint once that action settles.
    if (cp.recoveryState === "none") {
      await tx.update(executionDispatchCheckpoints).set({ recoveryState: "recovery_open", updatedAt: now })
        .where(eq(executionDispatchCheckpoints.runId, cp.runId));
      await persistActivity(tx, {
        companyId: task.companyId, actorType: "system", actorId: "dispatch-checkpoint",
        action: "issue.dispatch_checkpoint_recovery_deferred", entityType: "issue", entityId: task.id, runId: run.id,
        details: { deferredBehindActionId: active.id, deferredBehindCause: active.cause, stage: cp.stage, leaseGeneration: cp.leaseGeneration, reason },
      });
      return "opened";
    }
    await tx.update(executionDispatchCheckpoints).set({ updatedAt: now }).where(eq(executionDispatchCheckpoints.runId, cp.runId));
    return "skipped";
  }

  let actionId: string | null = ownAction?.id ?? null;
  if (ownAction) {
    await tx.update(issueRecoveryActions).set({
      evidence: { ...ownAction.evidence, dispatchCheckpoint: evidence.dispatchCheckpoint }, updatedAt: now,
    }).where(eq(issueRecoveryActions.id, ownAction.id));
  } else if (eligibleOwner) {
    const [created] = await tx.insert(issueRecoveryActions).values({
      companyId: task.companyId,
      sourceIssueId: task.id,
      kind: "active_run_watchdog",
      ownerType: "board",
      returnOwnerAgentId: task.assigneeAgentId,
      cause: DISPATCH_RECOVERY_CAUSE,
      fingerprint: `${OWN_ACTION_FINGERPRINT_PREFIX}${run.id}`,
      evidence,
      nextAction: "Inspect the provider run and its recorded actions, then reconcile their outcomes before continuing. A superseded generation cannot commit further stages.",
      maxAttempts: 3,
    }).returning();
    actionId = created!.id;
  }
  await tx.update(executionDispatchCheckpoints).set({
    recoveryState: actionId ? "recovery_open" : "settled_no_continuation",
    recoveryActionId: actionId,
    updatedAt: now,
  }).where(eq(executionDispatchCheckpoints.runId, cp.runId));
  await persistActivity(tx, {
    companyId: task.companyId, actorType: "system", actorId: "dispatch-checkpoint",
    action: actionId ? "issue.dispatch_checkpoint_recovery_opened" : "issue.dispatch_checkpoint_settled_without_continuation",
    entityType: "issue", entityId: task.id, runId: run.id,
    details: { recoveryActionId: actionId, stage: cp.stage, leaseGeneration: cp.leaseGeneration, fenceGeneration, reason },
  });
  return "opened";
}

/**
 * Refuse a reconciliation that contradicts the durable checkpoint. A claim that
 * nothing was performed cannot stand when side effects were recorded, and a
 * non-idempotent recorded effect needs an explicit completed or mixed outcome.
 */
export async function assertReconciliationMatchesCheckpoint(db: Db, input: {
  companyId: string;
  runId: string;
  actionOutcome: "completed" | "not_performed" | "mixed";
}) {
  const [cp] = await db.select().from(executionDispatchCheckpoints).where(and(
    eq(executionDispatchCheckpoints.companyId, input.companyId),
    eq(executionDispatchCheckpoints.runId, input.runId),
  ));
  if (!cp) return null;
  if (input.actionOutcome === "not_performed" && cp.sideEffects.length > 0) {
    throw conflict("The dispatch checkpoint recorded external side effects; reconcile them as completed or mixed before continuing.");
  }
  if (input.actionOutcome === "not_performed" && stageRank(cp.stage) >= stageRank("provider_returned")) {
    throw conflict("The provider returned before the run stopped; a not-performed outcome is not supported by the checkpoint.");
  }
  return cp;
}

/**
 * Called after an operator decision is saved on a recovery action. Only a
 * checkpoint owned by this action (or an unswept checkpoint of the same run
 * whose action is this run's own reconciliation action) moves on, so settling
 * an unrelated action never marks this run reconciled.
 */
export async function markCheckpointContinuationPending(
  db: Db,
  runId: string,
  action: { id: string; cause?: string | null; fingerprint?: string | null },
) {
  const ownsUnswept = action.cause === DISPATCH_RECOVERY_CAUSE
    && action.fingerprint === `${OWN_ACTION_FINGERPRINT_PREFIX}${runId}`;
  await db.update(executionDispatchCheckpoints).set({
    recoveryState: "continuation_pending", recoveryActionId: action.id, updatedAt: new Date(),
  }).where(and(
    eq(executionDispatchCheckpoints.runId, runId),
    inArray(executionDispatchCheckpoints.recoveryState, ["none", "recovery_open"]),
    ownsUnswept
      ? or(isNull(executionDispatchCheckpoints.recoveryActionId), eq(executionDispatchCheckpoints.recoveryActionId, action.id))
      : eq(executionDispatchCheckpoints.recoveryActionId, action.id),
  ));
}

/** Called once the single reconciled continuation run exists. */
export async function markCheckpointContinued(db: Db, runId: string, continuationRunId: string) {
  await db.update(executionDispatchCheckpoints).set({
    recoveryState: "continued", continuationRunId, updatedAt: new Date(),
  }).where(and(
    eq(executionDispatchCheckpoints.runId, runId),
    isNull(executionDispatchCheckpoints.continuationRunId),
  ));
}

export async function listDispatchDiagnostics(
  db: Db,
  companyId: string,
  issueId: string,
  options: { includeProviderEvidence?: boolean } = {},
) {
  const includeProviderEvidence = options.includeProviderEvidence ?? false;
  const [fence] = await db.select().from(issueExecutionFences).where(and(
    eq(issueExecutionFences.companyId, companyId), eq(issueExecutionFences.issueId, issueId),
  ));
  const rows = await db.select({
    checkpoint: executionDispatchCheckpoints,
    runStatus: heartbeatRuns.status,
    agentName: agents.name,
  }).from(executionDispatchCheckpoints)
    .innerJoin(heartbeatRuns, eq(heartbeatRuns.id, executionDispatchCheckpoints.runId))
    .leftJoin(agents, eq(agents.id, executionDispatchCheckpoints.agentId))
    .where(and(eq(executionDispatchCheckpoints.companyId, companyId), eq(executionDispatchCheckpoints.issueId, issueId)))
    .orderBy(desc(executionDispatchCheckpoints.createdAt)).limit(50);
  return {
    issueId,
    fenceGeneration: fence?.generation ?? null,
    checkpoints: rows.map(({ checkpoint, runStatus, agentName }) => ({
      runId: checkpoint.runId,
      agentId: checkpoint.agentId,
      agentName: agentName ?? null,
      runStatus,
      providerRef: includeProviderEvidence ? checkpoint.providerRef : null,
      leaseGeneration: checkpoint.leaseGeneration,
      stale: checkpoint.recoveryState !== "none",
      stage: checkpoint.stage,
      idempotencyKey: includeProviderEvidence ? checkpoint.idempotencyKey : null,
      sideEffects: includeProviderEvidence ? checkpoint.sideEffects : [],
      sideEffectCount: checkpoint.sideEffects.length,
      recoveryState: checkpoint.recoveryState,
      recoveryActionId: checkpoint.recoveryActionId,
      continuationRunId: checkpoint.continuationRunId,
      createdAt: checkpoint.createdAt,
      updatedAt: checkpoint.updatedAt,
    })),
  };
}
