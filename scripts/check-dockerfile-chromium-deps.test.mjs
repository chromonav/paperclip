import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dockerfile = readFileSync(path.join(repoRoot, "Dockerfile"), "utf8");

function stage(name) {
  const lines = dockerfile.split("\n");
  const out = [];
  let inStage = false;
  for (const line of lines) {
    if (/^FROM /i.test(line.trim())) {
      if (inStage) break;
      inStage = new RegExp(`\\sAS ${name}$`, "i").test(line.trim());
      continue;
    }
    if (inStage) out.push(line);
  }
  return out.join("\n");
}

const REQUIRED_CHROMIUM_LIBS = [
  "libasound2t64",
  "libatk-bridge2.0-0t64",
  "libatk1.0-0t64",
  "libatspi2.0-0t64",
  "libcairo2",
  "libcups2t64",
  "libdbus-1-3",
  "libdrm2",
  "libgbm1",
  "libglib2.0-0t64",
  "libnspr4",
  "libnss3",
  "libpango-1.0-0",
  "libx11-6",
  "libxcb1",
  "libxcomposite1",
  "libxdamage1",
  "libxext6",
  "libxfixes3",
  "libxkbcommon0",
  "libxrandr2",
  "fonts-liberation",
  "fonts-noto-color-emoji",
];

test("production stage installs headless Chromium system libraries", () => {
  const production = stage("production");
  assert.ok(production.length > 0, "production stage not found");
  for (const lib of REQUIRED_CHROMIUM_LIBS) {
    assert.match(production, new RegExp(`\\b${lib.replace(/[.+]/g, "\\$&")}\\b`), `missing ${lib}`);
  }
});

test("Chromium libraries are installed with --no-install-recommends and apt lists are cleaned", () => {
  const production = stage("production");
  assert.match(production, /apt-get install -y --no-install-recommends[^&]*libnss3/s);
  assert.match(production, /rm -rf \/var\/lib\/apt\/lists\/\*/);
});

test("Chromium deps do not use playwright install-deps (needs root at runtime, pulls extras)", () => {
  assert.doesNotMatch(stage("production"), /playwright install-deps/);
});
