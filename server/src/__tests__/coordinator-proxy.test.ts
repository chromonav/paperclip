import express from "express";
import { createServer, type Server } from "node:http";
import { once } from "node:events";
import { afterEach, describe, expect, it } from "vitest";
import request from "supertest";
import WebSocket, { WebSocketServer } from "ws";
import { createCoordinatorProxy } from "../coordinator-proxy.js";

const servers: Server[] = [];
const clients: WebSocket[] = [];
const webSockets: WebSocketServer[] = [];

async function listen(server: Server) {
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing listener address");
  return `http://127.0.0.1:${address.port}`;
}

async function reader(origin: string) {
  const app = express();
  const proxy = createCoordinatorProxy(origin);
  app.use(proxy.request);
  const server = createServer(app);
  server.on("upgrade", proxy.upgrade);
  return { app, origin: await listen(server) };
}

afterEach(async () => {
  for (const client of clients.splice(0)) client.terminate();
  for (const wss of webSockets.splice(0)) {
    for (const socket of wss.clients) socket.terminate();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  }
  for (const server of servers.splice(0).reverse()) {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

describe("coordinator proxy execution transport", () => {
  it("does not replay an ambiguously committed write after losing its response", async () => {
    let committedAssignments = 0;
    const coordinator = createServer((req, res) => {
      req.resume();
      req.on("end", () => {
        committedAssignments += 1;
        res.destroy();
      });
    });
    const proxy = await reader(await listen(coordinator));
    const result = await request(proxy.app).post("/api/issues/assigned").send({ assigneeAgentId: "single-owner" });
    expect(result.status).toBe(503);
    expect(result.body.code).toBe("COORDINATOR_UNAVAILABLE");
    expect(committedAssignments).toBe(1);
  });

  it("streams events before completion and releases the upstream connection on client disconnect", async () => {
    let remoteClosed!: () => void;
    const closed = new Promise<void>((resolve) => { remoteClosed = resolve; });
    const coordinator = createServer((_req, res) => {
      res.writeHead(200, { "Content-Type": "text/event-stream" });
      res.write("data: running\n\n");
      res.once("close", remoteClosed);
    });
    const proxy = await reader(await listen(coordinator));
    const abort = new AbortController();
    try {
      const response = await fetch(`${proxy.origin}/api/run/events`, { signal: abort.signal });
      expect(response.status).toBe(200);
      const stream = response.body!.getReader();
      const first = await stream.read();
      expect(first.done).toBe(false);
      expect(new TextDecoder().decode(first.value)).toBe("data: running\n\n");
      abort.abort();
      await closed;
    } finally {
      abort.abort();
    }
  });

  it("preserves WebSocket authentication denial and passes live events only after coordinator authorization", async () => {
    const coordinator = createServer();
    const wss = new WebSocketServer({ noServer: true });
    webSockets.push(wss);
    coordinator.on("upgrade", (req, socket, head) => {
      if (req.headers.cookie !== "session=authorized") {
        socket.end("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        return;
      }
      wss.handleUpgrade(req, socket, head, (ws) => {
        ws.once("message", () => ws.send(JSON.stringify({ sequence: 1, status: "running" })));
      });
    });
    const proxy = await reader(await listen(coordinator));
    const url = proxy.origin.replace("http:", "ws:") + "/api/live";
    const denied = new WebSocket(url);
    clients.push(denied);
    const [_, denial] = await once(denied, "unexpected-response");
    expect(denial.statusCode).toBe(401);
    denied.terminate();
    denied.on("error", () => {});
    const allowed = new WebSocket(url, { headers: { Cookie: "session=authorized" } });
    clients.push(allowed);
    await once(allowed, "open");
    const event = once(allowed, "message");
    allowed.send("subscribe");
    const [data] = await event;
    expect(JSON.parse(data.toString())).toEqual({ sequence: 1, status: "running" });
  });
});
