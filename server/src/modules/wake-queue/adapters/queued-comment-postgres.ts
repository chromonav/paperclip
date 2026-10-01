import { and, eq, inArray, sql } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import { agentWakeupRequests, agents, heartbeatRuns, issueComments, issues } from "@paperclipai/db";
import type { IssueComment, IssueQueuedCommentQueue } from "@paperclipai/shared";
import { isUuidLike } from "@paperclipai/shared";
import {
  buildQueuedCommentQueueSnapshot,
  decideQueuedCommentQueueSteering,
  queuedCommentIdsFromWakePayload,
  withQueuedCommentIdsInRunContext,
  withQueuedCommentIdsInWakePayload,
} from "../../../services/issue-queued-comment-queue.js";
import { logActivity as persistActivityLogRow, type ActivityPublication } from "../../../services/activity-log.js";
import { decideQueuedCommentWakeLookup } from "../domain/policy.js";
import { parseObject, readNonEmptyString } from "../domain/values.js";
import { QueuedCommentMutationError } from "../application/queued-comment-use-cases.js";
import type {
  LockedQueuedCommentState,
  QueuedCommentActivityLogInput,
  QueuedCommentActivityPublication,
  QueuedCommentIssueLockWriter,
  QueuedCommentQueueTransaction,
  QueuedCommentRunRow,
  QueuedCommentWakeRow,
} from "../application/queued-comment-ports.js";

type WakeRow = typeof agentWakeupRequests.$inferSelect;
type RunRow = typeof heartbeatRuns.$inferSelect;

function toWakeRow(row: WakeRow): QueuedCommentWakeRow {
  return { id: row.id, agentId: row.agentId, status: row.status, runId: row.runId, payload: parseObject(row.payload) };
}

function toRunRow(row: RunRow): QueuedCommentRunRow {
  return { id: row.id, status: row.status, runtimeMode: row.runtimeMode, contextSnapshot: parseObject(row.contextSnapshot) };
}

export type QueuedCommentQueuePostgresAdapterDeps = {
  /** `issueReferenceService(db).syncComment`; runs on the module's own transaction. */
  syncCommentReferences(commentId: string, tx: Db): Promise<void>;
  /** `issueReferenceService(db).deleteCommentSource`; runs on the module's own transaction. */
  deleteCommentReferenceSource(commentId: string, tx: Db): Promise<void>;
  /** `externalObjectService(db, opts).syncCommentSafely`; runs on the module's own transaction. */
  syncCommentExternalObjectsSafely(commentId: string, tx: Db): Promise<void>;
};

function buildTransaction(tx: Db, companyId: string, deps: QueuedCommentQueuePostgresAdapterDeps, linkedWake: WakeRow | null): QueuedCommentQueueTransaction {
  return {
    async updateCommentBody({ issueId, commentId, body, updatedAt }) {
      const updated = await tx
        .update(issueComments)
        .set({ body, updatedAt })
        .where(and(eq(issueComments.id, commentId), eq(issueComments.issueId, issueId), eq(issueComments.companyId, companyId)))
        .returning({ id: issueComments.id })
        .then((rows) => rows[0] ?? null);
      return updated !== null;
    },

    async touchIssueUpdatedAt({ issueId, updatedAt }) {
      await tx.update(issues).set({ updatedAt }).where(and(eq(issues.id, issueId), eq(issues.companyId, companyId)));
    },

    async updateWakeQueuedCommentIds({ wakeId, payload, ids, updatedAt }) {
      const row = await tx
        .update(agentWakeupRequests)
        .set({ payload: withQueuedCommentIdsInWakePayload(payload, ids), updatedAt })
        .where(and(eq(agentWakeupRequests.id, wakeId), eq(agentWakeupRequests.companyId, companyId)))
        .returning()
        .then((rows) => rows[0]);
      if (linkedWake) {
        // Narrow the original authorization; never add IDs or manufacture a
        // consumed receipt while editing its queued successor.
        const linkedIds = queuedCommentIdsFromWakePayload(linkedWake.payload);
        await tx.update(agentWakeupRequests).set({
          payload: withQueuedCommentIdsInWakePayload(linkedWake.payload, ids.filter((id) => linkedIds.includes(id))),
          updatedAt,
        }).where(and(eq(agentWakeupRequests.id, linkedWake.id), eq(agentWakeupRequests.companyId, companyId),
          eq(agentWakeupRequests.status, linkedWake.status)));
      }
      return toWakeRow(row);
    },

    async updateQueueRunCommentIds({ queueRunId, contextSnapshot, ids, updatedAt }) {
      const row = await tx
        .update(heartbeatRuns)
        .set({ contextSnapshot: withQueuedCommentIdsInRunContext(contextSnapshot, ids), updatedAt })
        .where(and(eq(heartbeatRuns.id, queueRunId), eq(heartbeatRuns.companyId, companyId), eq(heartbeatRuns.status, "queued")))
        .returning()
        .then((rows) => rows[0] ?? null);
      return row ? toRunRow(row) : null;
    },

    async deleteComment({ issueId, commentId }) {
      const row = await tx
        .delete(issueComments)
        .where(and(eq(issueComments.id, commentId), eq(issueComments.issueId, issueId), eq(issueComments.companyId, companyId)))
        .returning()
        .then((rows) => rows[0] ?? null);
      return row ? (row as IssueComment) : null;
    },

    async cancelWake({ wakeId, reason, now }) {
      await tx
        .update(agentWakeupRequests)
        .set({ status: "cancelled", finishedAt: now, error: reason, updatedAt: now })
        .where(and(eq(agentWakeupRequests.id, wakeId), eq(agentWakeupRequests.companyId, companyId)));
      if (linkedWake) {
        await tx.update(agentWakeupRequests).set({ status: "cancelled", finishedAt: now, error: reason, updatedAt: now })
          .where(and(eq(agentWakeupRequests.id, linkedWake.id), eq(agentWakeupRequests.companyId, companyId),
            eq(agentWakeupRequests.status, linkedWake.status)));
      }
    },

    async cancelQueueRun({ queueRunId, reason, now }) {
      const row = await tx
        .update(heartbeatRuns)
        .set({ status: "cancelled", finishedAt: now, error: reason, errorCode: "queued_comment_discarded", updatedAt: now })
        .where(and(eq(heartbeatRuns.id, queueRunId), eq(heartbeatRuns.companyId, companyId), eq(heartbeatRuns.status, "queued")))
        .returning({ id: heartbeatRuns.id })
        .then((rows) => rows[0] ?? null);
      return row ? { id: row.id } : null;
    },

    async clearExecutionLockAndTouchIssue({ issueId, executionRunId, updatedAt }) {
      await tx
        .update(issues)
        .set({ executionRunId: null, executionAgentNameKey: null, executionLockedAt: null, updatedAt })
        .where(and(eq(issues.id, issueId), eq(issues.companyId, companyId), eq(issues.executionRunId, executionRunId)));
    },

    async buildQueueSnapshot({ issue, actor, wake, state, queueRun, activeRun }): Promise<IssueQueuedCommentQueue> {
      const commentIds = queuedCommentIdsFromWakePayload(wake?.payload ?? null);
      const rows =
        commentIds.length > 0
          ? await tx
              .select()
              .from(issueComments)
              .where(
                and(
                  eq(issueComments.companyId, companyId),
                  eq(issueComments.issueId, issue.id),
                  inArray(issueComments.id, commentIds),
                ),
              )
          : [];
      const byId = new Map(rows.map((row) => [row.id, row]));
      const comments = commentIds.flatMap((id) => {
        const row = byId.get(id);
        return row && !row.deletedAt ? [row] : [];
      });

      const assignedAgent = issue.assigneeAgentId
        ? await tx
            .select({ adapterType: agents.adapterType })
            .from(agents)
            .where(and(eq(agents.id, issue.assigneeAgentId), eq(agents.companyId, companyId)))
            .limit(1)
            .then((agentRows) => agentRows[0] ?? null)
        : null;

      // A queue mutation never delivers same-turn steering itself, so this
      // adapter never probes the live runner: it answers
      // "temporarily_unavailable" wherever the shared rule says a caller
      // may probe. Only the read path probes the live provider.
      const steering = decideQueuedCommentQueueSteering({
        state,
        queueRunRuntimeMode: queueRun?.runtimeMode ?? null,
        activeRun,
        assignedAgentAdapterType: assignedAgent?.adapterType ?? null,
        queuedCommentCount: comments.length,
      });
      const steeringDisposition: IssueQueuedCommentQueue["steeringDisposition"] =
        steering.kind === "probe" ? "temporarily_unavailable" : steering.kind;

      return buildQueuedCommentQueueSnapshot({
        issueId: issue.id,
        queueId: wake?.id ?? null,
        state,
        activeRunId: activeRun?.id ?? null,
        protocol: steering.protocol,
        steeringDisposition,
        comments,
        actorType: actor.actorType,
        actorId: actor.actorId,
      });
    },

    async syncCommentReferences(commentId) {
      await deps.syncCommentReferences(commentId, tx);
    },
    async deleteCommentReferenceSource(commentId) {
      await deps.deleteCommentReferenceSource(commentId, tx);
    },
    async syncCommentExternalObjectsSafely(commentId) {
      await deps.syncCommentExternalObjectsSafely(commentId, tx);
    },

    async logActivity(input: QueuedCommentActivityLogInput): Promise<QueuedCommentActivityPublication> {
      const publications: ActivityPublication[] = [];
      await persistActivityLogRow(
        tx,
        {
          companyId,
          actorType: input.actorType,
          actorId: input.actorId,
          agentId: input.agentId,
          runId: input.runId,
          agentApiKeyId: input.agentApiKeyId,
          action: input.action,
          entityType: "issue",
          entityId: input.entityId,
          details: input.details,
        },
        publications,
      );
      return publications[0];
    },
  };
}

export function createQueuedCommentIssueLockWriter(db: Db, deps: QueuedCommentQueuePostgresAdapterDeps): QueuedCommentIssueLockWriter {
  return {
    async withLockedQueue(input, fn) {
      return db.transaction(async (rawTx) => {
        const tx = rawTx as unknown as Db;
        const companyId = input.issue.companyId;

        await tx
          .select({ id: issues.id })
          .from(issues)
          .where(and(eq(issues.id, input.issue.id), eq(issues.companyId, companyId)))
          .for("update");

        const wakeRow = await tx
          .select()
          .from(agentWakeupRequests)
          .where(
            and(
              eq(agentWakeupRequests.id, input.queueId),
              eq(agentWakeupRequests.companyId, companyId),
              input.issue.assigneeAgentId ? eq(agentWakeupRequests.agentId, input.issue.assigneeAgentId) : undefined,
            ),
          )
          .for("update")
          .limit(1)
          .then((rows) => rows[0] ?? null);

        const wakePayload = parseObject(wakeRow?.payload);
        const lookup = decideQueuedCommentWakeLookup({
          wakePresent: wakeRow !== null,
          wakeIssueIdMatches: readNonEmptyString(wakePayload.issueId) === input.issue.id,
          hasQueuedCommentIds: queuedCommentIdsFromWakePayload(wakeRow?.payload ?? null).length > 0,
          wakeStatus: wakeRow?.status ?? null,
          wakeHasRunId: Boolean(wakeRow?.runId),
        });

        if (lookup.kind === "not_pending") {
          throw new QueuedCommentMutationError("queued_comment_not_pending", "The queued message is no longer pending");
        }
        if (lookup.kind === "already_dispatching") {
          throw new QueuedCommentMutationError("queued_comment_already_dispatching", "The queued message is already being dispatched");
        }

        // Unreachable: `decideQueuedCommentWakeLookup` only returns "deferred" or "check_queue_run" when the wake row is present.
        if (!wakeRow) throw new Error("wake-queue: queued-comment lookup resolved without a wake row");

        // The interrupt's source receipt and delivery wake are distinct rows.
        // Lock both under the task lock before touching either one or the run.
        const interruptPrefix = "queued-comment-interrupt:";
        let linkedWakeRow: WakeRow | null = null;
        let runWake = wakeRow;
        if (lookup.kind === "deferred" && wakePayload.queuedCommentInterrupt) {
          // Older admissions could leave the original receipt unbound. Its
          // exact successor still belongs to the discard; never revive it.
          linkedWakeRow = await tx.select().from(agentWakeupRequests).where(and(
            eq(agentWakeupRequests.companyId, companyId),
            eq(agentWakeupRequests.agentId, wakeRow.agentId),
            eq(agentWakeupRequests.idempotencyKey, `${interruptPrefix}${wakeRow.id}`),
            sql`${agentWakeupRequests.payload}->>'issueId' = ${input.issue.id}`,
            inArray(agentWakeupRequests.status, ["queued", "claimed"]),
          )).for("update").limit(1).then((rows) => rows[0] ?? null);
          if (linkedWakeRow) runWake = linkedWakeRow;
        } else if (lookup.kind === "check_queue_run" && wakeRow.idempotencyKey?.startsWith(interruptPrefix)) {
          const receiptId = wakeRow.idempotencyKey.slice(interruptPrefix.length);
          if (isUuidLike(receiptId)) {
            linkedWakeRow = await tx.select().from(agentWakeupRequests).where(and(
              eq(agentWakeupRequests.id, receiptId), eq(agentWakeupRequests.companyId, companyId),
              eq(agentWakeupRequests.agentId, wakeRow.agentId), eq(agentWakeupRequests.runId, wakeRow.runId!),
              eq(agentWakeupRequests.status, "coalesced"),
              sql`${agentWakeupRequests.payload}->>'issueId' = ${input.issue.id}`,
            )).for("update").limit(1).then((rows) => rows[0] ?? null);
          }
        }

        let state: "deferred" | "queued";
        let queueRunRow: RunRow | null = null;

        if (lookup.kind === "deferred" && !linkedWakeRow) {
          state = "deferred";
        } else {
          // Resolve only the exact delivery wake's still-queued successor.
          queueRunRow = await tx
            .select()
            .from(heartbeatRuns)
            .where(
              and(
                eq(heartbeatRuns.id, runWake.runId!),
                eq(heartbeatRuns.companyId, companyId),
                eq(heartbeatRuns.agentId, runWake.agentId),
                eq(heartbeatRuns.wakeupRequestId, runWake.id),
                linkedWakeRow ? sql`${heartbeatRuns.contextSnapshot}->>'issueId' = ${input.issue.id}` : undefined,
              ),
            )
            .for("update")
            .limit(1)
            .then((rows) => rows[0] ?? null);
          if (!queueRunRow || queueRunRow.status !== "queued" || runWake.status !== "queued" ||
              (lookup.kind === "deferred" && (queueRunRow.runtimeMode !== "legacy" || queueRunRow.startedAt || queueRunRow.processPid))) {
            throw new QueuedCommentMutationError(
              "queued_comment_already_dispatching",
              "The queued message is already being dispatched",
            );
          }
          state = lookup.kind === "deferred" ? "deferred" : "queued";
        }

        const activeRunId = state === "deferred" ? input.issue.executionRunId : null;
        const activeRunRow = activeRunId
          ? await tx
              .select()
              .from(heartbeatRuns)
              .where(and(eq(heartbeatRuns.id, activeRunId), eq(heartbeatRuns.companyId, companyId), eq(heartbeatRuns.status, "running")))
              .for("update")
              .limit(1)
              .then((rows) => rows[0] ?? null)
          : null;

        const transaction = buildTransaction(tx, companyId, deps, linkedWakeRow);
        const wake = toWakeRow(wakeRow);
        const queueRun = queueRunRow ? toRunRow(queueRunRow) : null;
        const activeRun = activeRunRow ? toRunRow(activeRunRow) : null;

        const queue = await transaction.buildQueueSnapshot({
          issue: input.issue,
          actor: input.actor,
          wake,
          state,
          queueRun,
          activeRun,
        });

        const locked: LockedQueuedCommentState = { wake, state, queueRun, activeRun, queue };
        return fn(locked, transaction);
      });
    },
  };
}
