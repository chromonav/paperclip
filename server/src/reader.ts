/// <reference path="./types/express.d.ts" />
// Deliberately independent of index.ts/createApp: importing or constructing the
// coordinator would start plugin workers, recovery and other execution owners.
import express from "express";
import { createServer } from "node:http";
import { hostname } from "node:os";
import type { Socket } from "node:net";
import path from "node:path";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { createDb, inspectMigrations, type Db } from "@paperclipai/db";
import { sql } from "drizzle-orm";
import { loadConfig, type Config } from "./config.js";
import { createBetterAuthInstance, deriveAuthTrustedOrigins, resolveBetterAuthSession } from "./auth/better-auth.js";
import { actorMiddleware } from "./middleware/auth.js";
import { errorHandler } from "./middleware/error-handler.js";
import { httpLogger, logger } from "./middleware/logger.js";
import { applyTrustProxy, parseTrustProxyEnv } from "./middleware/trust-proxy.js";
import { privateHostnameGuard } from "./middleware/private-hostname-guard.js";
import { apiCompression } from "./middleware/api-compression.js";
import { companyReadRoutes } from "./routes/company-reads.js";
import { goalReadRoutes } from "./routes/goals.js";
import { folderReadRoutes } from "./routes/folders.js";
import { dashboardRoutes } from "./routes/dashboard.js";
import { createCoordinatorProxy } from "./coordinator-proxy.js";
import { isOffloadedRead } from "./read-offload-policy.js";
import { readBrandedStaticIndexHtml } from "./static-index-html.js";
import { staticUiCacheControl } from "./static-ui-cache.js";
import { companyReadService } from "./services/company-reads.js";
import { goalService } from "./services/goals.js";
import { folderService } from "./services/folders.js";

export function createReaderApp(db: Db, config: Config, coordinatorUrl: string) {
  if (config.deploymentMode !== "authenticated") throw new Error("Read replicas require authenticated mode");
  const app = express();
  const proxy = createCoordinatorProxy(coordinatorUrl);
  applyTrustProxy(app, parseTrustProxyEnv(process.env.TRUST_PROXY));
  app.use(httpLogger);
  app.use(privateHostnameGuard({
    enabled: config.deploymentExposure === "private",
    allowedHostnames: config.allowedHostnames,
    bindHost: config.host,
  }));
  app.get("/_paperclip/reader-health", async (_req, res) => {
    await db.execute(sql`select 1`);
    res.json({ status: "ok", role: "reader", replica: hostname() });
  });
  const auth = createBetterAuthInstance(db, config, deriveAuthTrustedOrigins(config), { readOnly: true });
  app.use("/api", (req, res, next) => {
    if (!isOffloadedRead(req.method, req.originalUrl, req.headers)) {
      proxy.request(req, res, next);
      return;
    }
    res.set("X-Paperclip-Request-Owner", "reader").set("X-Paperclip-Reader", hostname()).set("Cache-Control", "no-store");
    next();
  }, actorMiddleware(db, {
    deploymentMode: "authenticated",
    readOnlyBoard: true,
    resolveSession: (req) => resolveBetterAuthSession(auth, req),
  }), apiCompression());
  app.use("/api/companies", companyReadRoutes(companyReadService(db)));
  app.use("/api", goalReadRoutes(goalService(db)), folderReadRoutes(folderService(db)), dashboardRoutes(db));
  // Fail closed if allowlist and handler registration ever diverge. Do not
  // retry a failed reader handler on the coordinator.
  app.use("/api", (_req, res) => res.status(404).json({ error: "Read route not found" }));

  if (config.serveUi) {
    const directory = path.dirname(fileURLToPath(import.meta.url));
    const uiDist = [path.resolve(directory, "../ui-dist"), path.resolve(directory, "../../ui/dist")]
      .find((candidate) => fs.existsSync(path.join(candidate, "index.html")));
    if (!uiDist) throw new Error("Read replica requires the built UI; set SERVE_UI=false for API-only operation");
    app.use("/assets", express.static(path.join(uiDist, "assets"), { maxAge: "1y", immutable: true }));
    app.get(["/", "/index.html"], (_req, res) => {
      res.type("html").set("Cache-Control", "no-cache").send(readBrandedStaticIndexHtml(uiDist));
    });
    app.use(express.static(uiDist, {
      index: false,
      maxAge: "1h",
      setHeaders(res, filePath) {
        const override = staticUiCacheControl(filePath);
        if (override) res.setHeader("Cache-Control", override);
      },
    }));
    // Only board navigation HTML is local. Plugin UI, MCP, files, unknown
    // endpoints and all non-GET traffic retain the coordinator as owner.
    app.get(/^\/(?:login|signup|onboarding|settings|[A-Za-z0-9_-]+\/(?:dashboard|issues|agents|projects|goals|costs|activity|chats))(?:\/.*)?$/, (_req, res) => {
      res.type("html").set("Cache-Control", "no-cache").send(readBrandedStaticIndexHtml(uiDist));
    });
  }
  app.use(proxy.request);
  app.use(errorHandler);
  return { app, upgrade: proxy.upgrade };
}

export async function startReaderServer() {
  if (process.env.PAPERCLIP_SERVER_ROLE !== "reader") throw new Error("Reader entrypoint requires PAPERCLIP_SERVER_ROLE=reader");
  const config = loadConfig();
  if (!config.databaseUrl) throw new Error("Readers require authoritative PostgreSQL; embedded databases are not supported");
  const coordinatorUrl = process.env.PAPERCLIP_COORDINATOR_URL;
  if (!coordinatorUrl) throw new Error("PAPERCLIP_COORDINATOR_URL is required");
  // PostgreSQL applies this setting on every pooled connection, including the
  // read-only schema inspection client. No migration/repair/bootstrap is run.
  const databaseUrl = new URL(config.databaseUrl);
  databaseUrl.searchParams.set("default_transaction_read_only", "on");
  if (!["postgres:", "postgresql:"].includes(databaseUrl.protocol)) throw new Error("Readers require a PostgreSQL connection URL");
  databaseUrl.searchParams.set("application_name", "paperclip-reader");
  const state = await inspectMigrations(databaseUrl.href);
  if (state.status !== "upToDate") throw new Error("Reader schema is not ready; migrations belong to the coordinator deployment");
  const db = createDb(databaseUrl.href);
  const closeDatabase = () => db.$client.end({ timeout: 5 });
  try {
    const { app, upgrade } = createReaderApp(db, config, coordinatorUrl);
    const server = createServer(app);
    const sockets = new Set<Socket>();
    server.on("connection", (socket) => {
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
    });
    server.on("upgrade", upgrade);
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(config.port, config.host, () => {
        server.off("error", reject);
        resolve();
      });
    });
    let closing: Promise<void> | undefined;
    const shutdown = () => closing ??= new Promise<void>((resolve) => {
      // Drain ordinary reads; disconnect long-lived streams without cancelling
      // their coordinator-owned executions. server.close does not close WS.
      const deadline = setTimeout(() => {
        for (const socket of sockets) socket.destroy();
      }, 10_000).unref();
      server.close(() => {
        clearTimeout(deadline);
        for (const socket of sockets) socket.destroy();
        resolve();
      });
    }).then(closeDatabase);
    logger.info({ role: "reader", host: config.host, port: config.port }, "Paperclip read replica listening");
    return { server, shutdown };
  } catch (error) {
    await closeDatabase();
    throw error;
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  void startReaderServer().then(({ shutdown }) => {
    process.once("SIGTERM", () => { void shutdown().then(() => process.exit(0)); });
    process.once("SIGINT", () => { void shutdown().then(() => process.exit(0)); });
  }).catch((error) => {
    logger.error({ err: error }, "Paperclip read replica failed to start");
    process.exitCode = 1;
  });
}
