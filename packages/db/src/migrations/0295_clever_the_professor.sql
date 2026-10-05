CREATE TABLE IF NOT EXISTS "execution_dispatch_checkpoints" (
	"run_id" uuid PRIMARY KEY NOT NULL,
	"company_id" uuid NOT NULL,
	"agent_id" uuid,
	"issue_id" uuid NOT NULL,
	"idempotency_key" text NOT NULL,
	"lease_generation" bigint NOT NULL,
	"stage" text DEFAULT 'intent' NOT NULL,
	"provider_ref" text,
	"side_effects" jsonb DEFAULT '[]'::jsonb NOT NULL,
	"recovery_state" text DEFAULT 'none' NOT NULL,
	"recovery_action_id" uuid,
	"continuation_run_id" uuid,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "issue_execution_fences" (
	"issue_id" uuid PRIMARY KEY NOT NULL,
	"company_id" uuid NOT NULL,
	"generation" bigint DEFAULT 1 NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'execution_dispatch_checkpoints_run_id_heartbeat_runs_id_fk' AND conrelid = 'execution_dispatch_checkpoints'::regclass) THEN
    ALTER TABLE "execution_dispatch_checkpoints" ADD CONSTRAINT "execution_dispatch_checkpoints_run_id_heartbeat_runs_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."heartbeat_runs"("id") ON DELETE cascade ON UPDATE no action;
  END IF;
END $$;--> statement-breakpoint
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'execution_dispatch_checkpoints_company_id_companies_id_fk' AND conrelid = 'execution_dispatch_checkpoints'::regclass) THEN
    ALTER TABLE "execution_dispatch_checkpoints" ADD CONSTRAINT "execution_dispatch_checkpoints_company_id_companies_id_fk" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE cascade ON UPDATE no action;
  END IF;
END $$;--> statement-breakpoint
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'execution_dispatch_checkpoints_agent_id_agents_id_fk' AND conrelid = 'execution_dispatch_checkpoints'::regclass) THEN
    ALTER TABLE "execution_dispatch_checkpoints" ADD CONSTRAINT "execution_dispatch_checkpoints_agent_id_agents_id_fk" FOREIGN KEY ("agent_id") REFERENCES "public"."agents"("id") ON DELETE set null ON UPDATE no action;
  END IF;
END $$;--> statement-breakpoint
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'execution_dispatch_checkpoints_issue_id_issues_id_fk' AND conrelid = 'execution_dispatch_checkpoints'::regclass) THEN
    ALTER TABLE "execution_dispatch_checkpoints" ADD CONSTRAINT "execution_dispatch_checkpoints_issue_id_issues_id_fk" FOREIGN KEY ("issue_id") REFERENCES "public"."issues"("id") ON DELETE cascade ON UPDATE no action;
  END IF;
END $$;--> statement-breakpoint
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'execution_dispatch_checkpoints_recovery_action_id_issue_recovery_actions_id_fk' AND conrelid = 'execution_dispatch_checkpoints'::regclass) THEN
    ALTER TABLE "execution_dispatch_checkpoints" ADD CONSTRAINT "execution_dispatch_checkpoints_recovery_action_id_issue_recovery_actions_id_fk" FOREIGN KEY ("recovery_action_id") REFERENCES "public"."issue_recovery_actions"("id") ON DELETE set null ON UPDATE no action;
  END IF;
END $$;--> statement-breakpoint
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'execution_dispatch_checkpoints_continuation_run_id_heartbeat_runs_id_fk' AND conrelid = 'execution_dispatch_checkpoints'::regclass) THEN
    ALTER TABLE "execution_dispatch_checkpoints" ADD CONSTRAINT "execution_dispatch_checkpoints_continuation_run_id_heartbeat_runs_id_fk" FOREIGN KEY ("continuation_run_id") REFERENCES "public"."heartbeat_runs"("id") ON DELETE set null ON UPDATE no action;
  END IF;
END $$;--> statement-breakpoint
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'issue_execution_fences_issue_id_issues_id_fk' AND conrelid = 'issue_execution_fences'::regclass) THEN
    ALTER TABLE "issue_execution_fences" ADD CONSTRAINT "issue_execution_fences_issue_id_issues_id_fk" FOREIGN KEY ("issue_id") REFERENCES "public"."issues"("id") ON DELETE cascade ON UPDATE no action;
  END IF;
END $$;--> statement-breakpoint
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'issue_execution_fences_company_id_companies_id_fk' AND conrelid = 'issue_execution_fences'::regclass) THEN
    ALTER TABLE "issue_execution_fences" ADD CONSTRAINT "issue_execution_fences_company_id_companies_id_fk" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE cascade ON UPDATE no action;
  END IF;
END $$;--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "execution_dispatch_checkpoints_idempotency_uq" ON "execution_dispatch_checkpoints" USING btree ("company_id","idempotency_key");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "execution_dispatch_checkpoints_issue_idx" ON "execution_dispatch_checkpoints" USING btree ("company_id","issue_id","created_at");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "execution_dispatch_checkpoints_open_idx" ON "execution_dispatch_checkpoints" USING btree ("stage","recovery_state") WHERE "execution_dispatch_checkpoints"."stage" <> 'completed' and "execution_dispatch_checkpoints"."recovery_state" in ('none', 'continuation_pending');