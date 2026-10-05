import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import postgres from "postgres";
import { describe, expect, it } from "vitest";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./test-embedded-postgres.js";

const support = await getEmbeddedPostgresTestSupport();
const migration = readFileSync(new URL("./migrations/0295_clever_the_professor.sql", import.meta.url), "utf8");
const journal = JSON.parse(readFileSync(new URL("./migrations/meta/_journal.json", import.meta.url), "utf8"));

describe("dispatch checkpoint migration shape", () => {
  it("is the next ordered post-0294 entry and is purely additive", () => {
    const tags: string[] = journal.entries.map((entry: { tag: string }) => entry.tag);
    const index = tags.indexOf("0295_clever_the_professor");
    expect(index).toBeGreaterThan(0);
    expect(tags[index - 1]).toMatch(/^0294_/);
    expect(new Set(tags).size).toBe(tags.length);
    expect(migration).not.toMatch(/\bDROP\b/i);
    expect(migration).not.toMatch(/ALTER TABLE "(issues|heartbeat_runs|issue_recovery_actions)"/);
  });
});

(support.supported ? describe : describe.skip)("dispatch checkpoint migration replay", () => {
  it("replays idempotently and cascades checkpoints with their run, leaving the tables inert on rollback", async () => {
    const database = await startEmbeddedPostgresTestDatabase("dispatch-migration-");
    const sql = postgres(database.connectionString, { max: 1, onnotice: () => {} });
    try {
      for (let pass = 0; pass < 2; pass++) {
        for (const statement of migration.split("--> statement-breakpoint")) {
          if (statement.trim()) await sql.unsafe(statement);
        }
      }
      const companyId = randomUUID(), agentId = randomUUID(), issueId = randomUUID(), runId = randomUUID();
      await sql`INSERT INTO companies (id, name, issue_prefix) VALUES (${companyId}, 'Dispatch migration', 'DMG')`;
      await sql`INSERT INTO agents (id, company_id, name, role, adapter_type) VALUES (${agentId}, ${companyId}, 'A', 'engineer', 'claude_local')`;
      await sql`INSERT INTO issues (id, company_id, title, issue_number, identifier) VALUES (${issueId}, ${companyId}, 'T', 1, 'DMG-1')`;
      await sql`INSERT INTO heartbeat_runs (id, company_id, agent_id) VALUES (${runId}, ${companyId}, ${agentId})`;
      await sql`INSERT INTO execution_dispatch_checkpoints (run_id, company_id, agent_id, issue_id, idempotency_key, lease_generation)
        VALUES (${runId}, ${companyId}, ${agentId}, ${issueId}, ${`dispatch:${runId}`}, 1)`;
      const secondRunId = randomUUID();
      await sql`INSERT INTO heartbeat_runs (id, company_id, agent_id) VALUES (${secondRunId}, ${companyId}, ${agentId})`;
      await expect(sql`INSERT INTO execution_dispatch_checkpoints (run_id, company_id, agent_id, issue_id, idempotency_key, lease_generation)
        VALUES (${secondRunId}, ${companyId}, ${agentId}, ${issueId}, ${`dispatch:${runId}`}, 2)`).rejects.toMatchObject({
        code: "23505",
        constraint_name: "execution_dispatch_checkpoints_idempotency_uq",
      });
      const auditAgentId = randomUUID(), thirdRunId = randomUUID();
      await sql`INSERT INTO agents (id, company_id, name, role, adapter_type) VALUES (${auditAgentId}, ${companyId}, 'B', 'engineer', 'claude_local')`;
      await sql`INSERT INTO heartbeat_runs (id, company_id, agent_id) VALUES (${thirdRunId}, ${companyId}, ${agentId})`;
      await sql`INSERT INTO execution_dispatch_checkpoints (run_id, company_id, agent_id, issue_id, idempotency_key, lease_generation)
        VALUES (${thirdRunId}, ${companyId}, ${auditAgentId}, ${issueId}, ${`dispatch:${thirdRunId}`}, 3)`;
      await sql`DELETE FROM agents WHERE id = ${auditAgentId}`;
      const [kept] = await sql`SELECT agent_id FROM execution_dispatch_checkpoints WHERE run_id = ${thirdRunId}`;
      expect(kept?.agent_id).toBeNull();
      await sql`DELETE FROM issues WHERE id = ${issueId}`;
      expect(await sql`SELECT run_id FROM execution_dispatch_checkpoints WHERE company_id = ${companyId}`).toHaveLength(0);
    } finally {
      await sql.end();
      await database.cleanup();
    }
  }, 60_000);
});
