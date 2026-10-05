import { sql } from "drizzle-orm";
import {
  bigint,
  index,
  jsonb,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from "drizzle-orm/pg-core";
import { agents } from "./agents.js";
import { companies } from "./companies.js";
import { heartbeatRuns } from "./heartbeat_runs.js";
import { issueRecoveryActions } from "./issue_recovery_actions.js";
import { issues } from "./issues.js";

export const issueExecutionFences = pgTable(
  "issue_execution_fences",
  {
    issueId: uuid("issue_id").primaryKey().references(() => issues.id, { onDelete: "cascade" }),
    companyId: uuid("company_id").notNull().references(() => companies.id, { onDelete: "cascade" }),
    generation: bigint("generation", { mode: "number" }).notNull().default(1),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
);

export const executionDispatchCheckpoints = pgTable(
  "execution_dispatch_checkpoints",
  {
    runId: uuid("run_id").primaryKey().references(() => heartbeatRuns.id, { onDelete: "cascade" }),
    companyId: uuid("company_id").notNull().references(() => companies.id, { onDelete: "cascade" }),
    agentId: uuid("agent_id").references(() => agents.id, { onDelete: "set null" }),
    issueId: uuid("issue_id").notNull().references(() => issues.id, { onDelete: "cascade" }),
    idempotencyKey: text("idempotency_key").notNull(),
    leaseGeneration: bigint("lease_generation", { mode: "number" }).notNull(),
    stage: text("stage").notNull().default("intent"),
    providerRef: text("provider_ref"),
    sideEffects: jsonb("side_effects").$type<Record<string, unknown>[]>().notNull().default([]),
    recoveryState: text("recovery_state").notNull().default("none"),
    recoveryActionId: uuid("recovery_action_id").references(() => issueRecoveryActions.id, { onDelete: "set null" }),
    continuationRunId: uuid("continuation_run_id").references(() => heartbeatRuns.id, { onDelete: "set null" }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => ({
    idempotencyKeyUq: uniqueIndex("execution_dispatch_checkpoints_idempotency_uq").on(table.companyId, table.idempotencyKey),
    issueStageIdx: index("execution_dispatch_checkpoints_issue_idx").on(table.companyId, table.issueId, table.createdAt),
    openIdx: index("execution_dispatch_checkpoints_open_idx").on(table.stage, table.recoveryState)
      .where(sql`${table.stage} <> 'completed' and ${table.recoveryState} in ('none', 'continuation_pending')`),
  }),
);
