import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const MAX_IMAGE_BYTES = 10 * 1024 * 1024;
const MAX_TOTAL_BYTES = 20 * 1024 * 1024;
const MAX_IMAGES = 8;
const formats: Record<string, string> = {
  "image/png": ".png",
  "image/jpeg": ".jpg",
  "image/gif": ".gif",
  "image/webp": ".webp",
};
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function matchesMagic(bytes: Buffer, mime: string): boolean {
  if (mime === "image/png") return bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]));
  if (mime === "image/jpeg") return bytes.length >= 4 && bytes[0] === 255 && bytes[1] === 216 && bytes[2] === 255 && bytes.at(-2) === 255 && bytes.at(-1) === 217;
  if (mime === "image/gif") return bytes.subarray(0, 6).toString("ascii") === "GIF87a" || bytes.subarray(0, 6).toString("ascii") === "GIF89a";
  return mime === "image/webp" && bytes.subarray(0, 4).toString("ascii") === "RIFF" && bytes.subarray(8, 12).toString("ascii") === "WEBP";
}

export async function stageWakeImages(wake: unknown, apiBase: string | undefined, token: string | undefined): Promise<{ files: string[]; cleanup: () => Promise<void> }> {
  const payload = wake && typeof wake === "object" ? wake as Record<string, unknown> : {};
  const comments = Array.isArray(payload.comments) ? payload.comments : [];
  const images: Array<{ id: string; contentPath: string; contentType: string; byteSize: number }> = [];
  for (const comment of comments) {
    if (!comment || typeof comment !== "object") continue;
    const attachments = (comment as Record<string, unknown>).attachments;
    if (!Array.isArray(attachments)) continue;
    for (const item of attachments) {
      if (!item || typeof item !== "object") continue;
      const descriptor = item as Record<string, unknown>;
      const { id, contentPath, contentType, byteSize } = descriptor;
      if (typeof contentType !== "string" || !contentType.startsWith("image/")) continue;
      if (typeof id !== "string" || !uuid.test(id) || contentPath !== `/api/attachments/${id}/content` ||
          !formats[contentType] || typeof byteSize !== "number" || !Number.isSafeInteger(byteSize) || byteSize < 1 || byteSize > MAX_IMAGE_BYTES) {
        throw new Error("Invalid wake image attachment descriptor");
      }
      images.push({ id, contentPath, contentType, byteSize });
    }
  }
  if (!images.length) return { files: [], cleanup: async () => {} };
  if (images.length > MAX_IMAGES || images.reduce((sum, image) => sum + image.byteSize, 0) > MAX_TOTAL_BYTES) {
    throw new Error("Wake image attachment limit exceeded");
  }
  if (!apiBase || !token) throw new Error("Wake image attachment requires Paperclip API credentials");
  let base: URL;
  try {
    base = new URL(apiBase);
    if (!(["http:", "https:"].includes(base.protocol)) || base.username || base.password || base.search || base.hash || !["/", "/api", "/api/"].includes(base.pathname)) throw new Error();
  } catch {
    throw new Error("Invalid Paperclip API origin for wake image attachment");
  }
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "paperclip-opencode-images-"));
  const cleanup = async () => { await fs.rm(dir, { recursive: true, force: true }); };
  const files: string[] = [];
  try {
    for (const [index, image] of images.entries()) {
      const url = new URL(image.contentPath, base.origin);
      if (url.origin !== base.origin || url.pathname !== image.contentPath) throw new Error("Invalid wake image attachment URL");
      const response = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` },
        redirect: "error",
        signal: AbortSignal.timeout(30_000),
      });
      if (!response.ok || response.redirected || (response.url && new URL(response.url).origin !== base.origin) || !response.body || response.headers.get("content-type")?.split(";")[0].trim().toLowerCase() !== image.contentType ||
          (response.headers.has("content-length") && Number(response.headers.get("content-length")) > image.byteSize)) {
        await response.body?.cancel();
        throw new Error("Wake image attachment download rejected");
      }
      const chunks: Buffer[] = [];
      let size = 0;
      for await (const chunk of response.body) {
        size += chunk.byteLength;
        if (size > image.byteSize || size > MAX_IMAGE_BYTES) throw new Error("Wake image attachment exceeds byte limit");
        chunks.push(Buffer.from(chunk));
      }
      const bytes = Buffer.concat(chunks);
      if (size !== image.byteSize || !matchesMagic(bytes, image.contentType)) throw new Error("Wake image attachment content mismatch");
      const file = path.join(dir, `${index}${formats[image.contentType]}`);
      await fs.writeFile(file, bytes, { flag: "wx", mode: 0o600 });
      files.push(file);
    }
    return { files, cleanup };
  } catch {
    await cleanup();
    throw new Error("Unable to stage wake image attachment");
  }
}
