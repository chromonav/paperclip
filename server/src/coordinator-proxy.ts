import http, { type IncomingHttpHeaders, type IncomingMessage } from "node:http";
import https from "node:https";
import type { Duplex } from "node:stream";
import type { RequestHandler } from "express";
import { logger } from "./middleware/logger.js";

const HOP_HEADERS = ["connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade"];

function endToEndHeaders(headers: IncomingHttpHeaders): IncomingHttpHeaders {
  const copy = { ...headers };
  const nominated = (headers.connection ?? "").split(",").map((name) => name.trim().toLowerCase());
  for (const name of [...HOP_HEADERS, ...nominated]) delete copy[name];
  return copy;
}

export interface CoordinatorProxy {
  request: RequestHandler;
  upgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void;
}

export function createCoordinatorProxy(origin: string): CoordinatorProxy {
  const target = new URL(origin);
  if (!["http:", "https:"].includes(target.protocol) || target.username || target.password || target.pathname !== "/" || target.search || target.hash) {
    throw new Error("PAPERCLIP_COORDINATOR_URL must be an HTTP(S) origin without credentials, path, query or fragment");
  }
  const transport = target.protocol === "https:" ? https : http;

  // No retries, redirects, buffering, credential substitution or local fallback:
  // a lost write response is ambiguous and must never replay an assignment.
  const request: RequestHandler = (req, res) => {
    res.setHeader("X-Paperclip-Request-Owner", "coordinator");
    const headers = endToEndHeaders(req.headers);
    headers["x-forwarded-for"] = req.ip ?? req.socket.remoteAddress;
    headers["x-forwarded-host"] = req.get("host");
    headers["x-forwarded-proto"] = req.protocol;
    const upstream = transport.request(target, { method: req.method, path: req.originalUrl, headers, servername: target.hostname }, (response) => {
      res.writeHead(response.statusCode ?? 502, {
        ...endToEndHeaders(response.headers),
        "x-paperclip-request-owner": "coordinator",
      });
      response.on("error", () => res.destroy());
      response.pipe(res);
    });
    upstream.on("error", (error: NodeJS.ErrnoException) => {
      logger.warn({ code: error.code }, "Coordinator proxy connection failed");
      if (res.headersSent) res.destroy();
      else res.status(503).json({ error: "Coordinator unavailable", code: "COORDINATOR_UNAVAILABLE" });
    });
    req.on("aborted", () => upstream.destroy());
    res.on("close", () => { if (!res.writableFinished) upstream.destroy(); });
    req.pipe(upstream);
  };

  const upgrade = (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    const headers = endToEndHeaders(req.headers);
    headers.connection = "Upgrade";
    headers.upgrade = req.headers.upgrade;
    const upstream = transport.request(target, { method: req.method, path: req.url, headers, servername: target.hostname });
    upstream.on("upgrade", (response, remote, remoteHead) => {
      const responseHeaders = Object.entries(response.headers).flatMap(([name, value]) =>
        (Array.isArray(value) ? value : [value]).filter((item) => item !== undefined).map((item) => `${name}: ${item}`),
      );
      socket.write(`HTTP/1.1 ${response.statusCode} ${response.statusMessage}\r\n${responseHeaders.join("\r\n")}\r\n\r\n`);
      if (remoteHead.length) socket.write(remoteHead);
      if (head.length) remote.write(head);
      socket.on("error", () => remote.destroy());
      remote.on("error", () => socket.destroy());
      socket.on("close", () => remote.destroy());
      remote.on("close", () => socket.destroy());
      socket.pipe(remote).pipe(socket);
    });
    upstream.on("response", (response) => {
      // Preserve a coordinator auth denial rather than turning it into a 101.
      socket.write(`HTTP/1.1 ${response.statusCode} ${response.statusMessage}\r\nConnection: close\r\n\r\n`);
      response.pipe(socket);
    });
    upstream.on("error", () => {
      if (!socket.destroyed) socket.end("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    });
    socket.on("error", () => upstream.destroy());
    socket.on("close", () => upstream.destroy());
    upstream.end();
  };

  return { request, upgrade };
}
