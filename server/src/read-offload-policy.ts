import type { IncomingHttpHeaders } from "node:http";

// Audited database-only handlers, shared with the coordinator. A new GET is
// coordinator-owned until explicitly reviewed here; paths are never prefixes.
// Company detail/stats: company-reads.ts (including monthly spend aggregation).
// Goals/folders: read routers only. Dashboard/recovery: SELECT-only reports.
const UUID = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
export const READ_OFFLOAD_PATHS = [
  /^\/api\/companies\/?$/,
  /^\/api\/companies\/stats\/?$/,
  new RegExp(`^/api/companies/${UUID}/?$`),
  new RegExp(`^/api/companies/${UUID}/(?:goals|folders|dashboard|recovery-observability)/?$`),
  new RegExp(`^/api/goals/${UUID}/?$`),
] as const;

export function isOffloadedRead(method: string | undefined, url: string, headers: IncomingHttpHeaders): boolean {
  if (method !== "GET") return false;
  // Agent keys, run JWTs and Cloud trusted-header identity can write audit or
  // synchronization state even on GET. They always go to the coordinator.
  if (headers["x-paperclip-run-id"] || headers["x-paperclip-cloud-tenant-token"] || headers.upgrade) return false;
  if (headers.authorization && !/^Bearer pcp_board_[a-f0-9]{48}$/i.test(headers.authorization)) return false;
  const pathname = url.split("?", 1)[0];
  return READ_OFFLOAD_PATHS.some((pattern) => pattern.test(pathname));
}
