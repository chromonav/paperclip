import express from "express";
import { createServer, type Server } from "node:http";
import { randomUUID } from "node:crypto";
import request from "supertest";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { eq } from "drizzle-orm";
import {
  authSessions, boardApiKeys, companies, companyMemberships, createDb, goals,
  type Db, type EmbeddedPostgresTestDatabase,
} from "@paperclipai/db";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";
import { createBetterAuthHandler, createBetterAuthInstance, resolveBetterAuthSession } from "../auth/better-auth.js";
import { actorMiddleware } from "../middleware/auth.js";
import { boardMutationGuard } from "../middleware/board-mutation-guard.js";
import { errorHandler } from "../middleware/error-handler.js";
import { goalRoutes } from "../routes/goals.js";
import { boardAuthService } from "../services/board-auth.js";
import { createReaderApp } from "../reader.js";
import type { Config } from "../config.js";

const ORIGIN = "http://127.0.0.1:41999";
const PASSWORD = "reader-integration-password";
const support = await getEmbeddedPostgresTestSupport();
const describePostgres = support.supported ? describe : describe.skip;

async function listen(server: Server) {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing test server address");
  return `http://127.0.0.1:${address.port}`;
}

function close(server: Server) {
  return new Promise<void>((resolve) => server.close(() => resolve()));
}

function cookieFor(response: request.Response) {
  const raw = response.headers["set-cookie"];
  const cookies = Array.isArray(raw) ? raw : raw ? [raw] : [];
  const token = cookies.find((cookie: string) => cookie.includes("session_token="));
  if (!token) throw new Error("Sign-in did not return a session cookie");
  return token.split(";")[0]!;
}

describePostgres("reader authentication and PostgreSQL read-only boundary", () => {
  let database: EmbeddedPostgresTestDatabase;
  let db: Db;
  let readDb: Db;
  let coordinator: Server;
  let readers: Server[];
  let config: Config;
  let userId: string;
  let companyA: string;
  let companyB: string;
  let hiddenGoalId: string;
  const email = "reader-member@example.com";
  const extraReaders: Server[] = [];

  beforeAll(async () => {
    vi.stubEnv("BETTER_AUTH_SECRET", "reader-integration-shared-auth-secret");
    vi.stubEnv("PAPERCLIP_AUTH_RATE_LIMIT_ENABLED", "false");
    vi.stubEnv("PAPERCLIP_PUBLIC_URL", ORIGIN);
    database = await startEmbeddedPostgresTestDatabase("paperclip-reader-auth-");
    db = createDb(database.connectionString);
    const readUrl = new URL(database.connectionString);
    readUrl.searchParams.set("default_transaction_read_only", "on");
    readDb = createDb(readUrl.href);
    config = {
      deploymentMode: "authenticated", deploymentExposure: "private", host: "127.0.0.1",
      authBaseUrlMode: "explicit", authPublicBaseUrl: ORIGIN,
      authDisableSignUp: false, allowedHostnames: ["127.0.0.1"], port: 41999, serveUi: false,
    } as Config;
    const auth = createBetterAuthInstance(db, config, [ORIGIN]);
    const app = express();
    app.all("/api/auth/{*authPath}", createBetterAuthHandler(auth));
    app.use(express.json(), actorMiddleware(db, {
      deploymentMode: "authenticated", resolveSession: (req) => resolveBetterAuthSession(auth, req),
    }), boardMutationGuard());
    app.use("/api", goalRoutes(db));
    app.use(errorHandler);
    coordinator = createServer(app);
    const origin = await listen(coordinator);
    readers = [
      createServer(createReaderApp(readDb, config, origin).app),
      createServer(createReaderApp(readDb, config, origin).app),
    ];
    await Promise.all(readers.map(listen));
    const signUp = await request(readers[0]!).post("/api/auth/sign-up/email").set("Origin", ORIGIN)
      .send({ email, password: PASSWORD, name: "Reader member" });
    expect(signUp.status).toBe(200);
    expect(signUp.headers["x-paperclip-request-owner"]).toBe("coordinator");
    userId = signUp.body.user.id;
    companyA = randomUUID();
    companyB = randomUUID();
    await db.insert(companies).values([
      { id: companyA, name: "Accessible", issuePrefix: "RDA" },
      { id: companyB, name: "Hidden", issuePrefix: "RDB" },
    ]);
    await db.insert(companyMemberships).values({ companyId: companyA, principalType: "user", principalId: userId, membershipRole: "owner" });
    hiddenGoalId = randomUUID();
    await db.insert(goals).values({ id: hiddenGoalId, companyId: companyB, title: "Hidden goal" });
  }, 90_000);

  afterAll(async () => {
    for (const server of [...(readers ?? []), ...extraReaders]) await close(server);
    if (coordinator) await close(coordinator);
    await readDb?.$client.end({ timeout: 5 });
    await database?.cleanup();
    vi.unstubAllEnvs();
  });

  async function signIn() {
    const response = await request(readers[0]!).post("/api/auth/sign-in/email").set("Origin", ORIGIN)
      .send({ email, password: PASSWORD });
    expect(response.status).toBe(200);
    return cookieFor(response);
  }

  it("shares a real signed session while preserving company scope on either reader", async () => {
    const cookie = await signIn();
    for (const reader of readers) {
      const list = await request(reader).get("/api/companies?scope=accessible").set("Cookie", cookie);
      expect(list.status).toBe(200);
      expect(list.headers["x-paperclip-request-owner"]).toBe("reader");
      expect(list.body.map((company: { id: string }) => company.id)).toEqual([companyA]);
      expect((await request(reader).get(`/api/companies/${companyB}`).set("Cookie", cookie)).status).toBe(403);
      const hidden = await request(reader).get(`/api/goals/${hiddenGoalId}`).set("Cookie", cookie);
      const missing = await request(reader).get(`/api/goals/${randomUUID()}`).set("Cookie", cookie);
      expect(hidden.status).toBe(404);
      expect(missing.status).toBe(404);
      expect(hidden.body).toEqual(missing.body);
      expect((await request(reader).get(`/api/companies/${companyA}`)).status).toBe(401);
    }
  });

  it("sends writes to the coordinator and makes the committed result visible on the other reader", async () => {
    const cookie = await signIn();
    const path = `/api/companies/${companyA}/goals`;
    const denied = await request(readers[0]!).post(path).set("Cookie", cookie).set("Origin", "https://untrusted.invalid")
      .send({ title: "Untrusted write", level: "task", status: "planned" });
    expect(denied.status).toBe(403);
    const created = await request(readers[0]!).post(path).set("Cookie", cookie).set("Origin", ORIGIN)
      .send({ title: "Committed through coordinator", level: "task", status: "planned" });
    expect(created.status).toBe(201);
    expect(created.headers["x-paperclip-request-owner"]).toBe("coordinator");
    const observed = await request(readers[1]!).get(`/api/goals/${created.body.id}`).set("Cookie", cookie);
    expect(observed.status).toBe(200);
    expect(observed.body.title).toBe("Committed through coordinator");
    const rows = await db.select().from(goals).where(eq(goals.companyId, companyA));
    expect(rows.map((goal) => goal.title)).toEqual(["Committed through coordinator"]);
  });

  it("does not refresh or delete sessions, and immediately rejects expiry and revocation", async () => {
    const cookie = await signIn();
    const old = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);
    await db.update(authSessions).set({ createdAt: old, updatedAt: old }).where(eq(authSessions.userId, userId));
    const before = await db.select().from(authSessions).where(eq(authSessions.userId, userId));
    expect((await request(readers[0]!).get(`/api/companies/${companyA}`).set("Cookie", cookie)).status).toBe(200);
    expect(await db.select().from(authSessions).where(eq(authSessions.userId, userId))).toEqual(before);
    await db.update(authSessions).set({ expiresAt: old }).where(eq(authSessions.userId, userId));
    for (const reader of readers) {
      expect((await request(reader).get(`/api/companies/${companyA}`).set("Cookie", cookie)).status).toBe(401);
    }
    // The expired row is retained: Better Auth must not try DELETE on the reader connection.
    expect((await db.select().from(authSessions).where(eq(authSessions.userId, userId))).map((row) => row.id)).toEqual(before.map((row) => row.id));
    const freshCookie = await signIn();
    await db.delete(authSessions).where(eq(authSessions.userId, userId));
    expect((await request(readers[1]!).get(`/api/companies/${companyA}`).set("Cookie", freshCookie)).status).toBe(401);
  });

  it("validates board keys without last-use writes and does not cache revoked access", async () => {
    const boardAuth = boardAuthService(db);
    const key = await boardAuth.createNamedBoardApiKey({ userId, name: "read replica proof" });
    for (const reader of readers) {
      expect((await request(reader).get(`/api/companies/${companyA}`).auth(key.token, { type: "bearer" })).status).toBe(200);
    }
    const [stored] = await db.select().from(boardApiKeys).where(eq(boardApiKeys.id, key.id));
    expect(stored?.lastUsedAt).toBeNull();
    await db.update(companyMemberships).set({ status: "inactive" }).where(eq(companyMemberships.principalId, userId));
    try {
      expect((await request(readers[1]!).get(`/api/companies/${companyA}`).auth(key.token, { type: "bearer" })).status).toBe(403);
    } finally {
      await db.update(companyMemberships).set({ status: "active" }).where(eq(companyMemberships.principalId, userId));
    }
    await boardAuth.revokeBoardApiKey(key.id);
    for (const reader of readers) {
      expect((await request(reader).get(`/api/companies/${companyA}`).auth(key.token, { type: "bearer" })).status).toBe(401);
    }
  });

  it("keeps audited reads available during coordinator outage but never runs writes or agent traffic locally", async () => {
    const cookie = await signIn();
    const stopped = createServer();
    const origin = await listen(stopped);
    await close(stopped);
    const reader = createServer(createReaderApp(readDb, config, origin).app);
    extraReaders.push(reader);
    await listen(reader);
    const safe = await request(reader).get(`/api/companies/${companyA}/goals`).set("Cookie", cookie);
    expect(safe.status).toBe(200);
    expect(safe.headers["x-paperclip-request-owner"]).toBe("reader");
    const write = await request(reader).post(`/api/companies/${companyA}/goals`).set("Cookie", cookie).set("Origin", ORIGIN)
      .send({ title: "Must not execute locally", level: "task", status: "planned" });
    expect(write.status).toBe(503);
    expect(write.body.code).toBe("COORDINATOR_UNAVAILABLE");
    expect((await request(reader).get(`/api/companies/${companyA}/agents`).set("Cookie", cookie)).status).toBe(503);
    expect((await request(reader).get(`/api/companies/${companyA}/goals`).auth("pcp_agent_example", { type: "bearer" })).status).toBe(503);
    expect((await db.select().from(goals).where(eq(goals.title, "Must not execute locally")))).toEqual([]);
  });
});
