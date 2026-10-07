import { createHash } from "node:crypto";
import { readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";

// Only the already-rehearsed provider installation is supported. Never update
// dependencies or overwrite its separately repaired native file-sync transport.
const [installation, ...extra] = process.argv.slice(2);
if (!installation || extra.length) throw new Error("Expected exactly one Kubernetes provider installation path");
const manifest = JSON.parse(await readFile(join(installation, "package.json"), "utf8"));
if (manifest.name !== "@paperclipai/plugin-kubernetes" || manifest.version !== "2026.1005.0") {
  throw new Error("Unexpected Kubernetes provider package; refusing to install the cancellation overlay");
}
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const syncHash = hash(await readFile(join(installation, "dist/file-sync.js")));
if (syncHash !== "77929528d3138a00d6e9112cf1cff469a5c5a5783aa4cfc74f63d690adfd4221") {
  throw new Error("Unexpected native file-sync implementation; refusing to overwrite the provider");
}
const originals = {
  "plugin.js": "f893486495ec17bfe730921cb8e54ece872c8163b3ef740bac835cb7bb63c24a",
  "lease-lifecycle.js": "a8d4b3f33a8a36b2ecce4df1db435d2297b6287ed3b2d5a6242c55221fab9621",
};
const replacements = [];
// Verify the complete boundary before writing either file. Accepting the new
// hash makes an interrupted/repeated init safe without accepting arbitrary code.
for (const [name, originalHash] of Object.entries(originals)) {
  const destination = join(installation, "dist", name);
  const contents = await readFile(new URL(`./kubernetes-overlay/${name}`, import.meta.url));
  const replacementHash = hash(contents);
  const currentHash = hash(await readFile(destination));
  if (currentHash !== originalHash && currentHash !== replacementHash) {
    throw new Error(`Unexpected installed ${name}; refusing to overwrite the provider`);
  }
  replacements.push({ name, destination, contents, replacementHash, unchanged: currentHash === replacementHash });
}
for (const replacement of replacements) {
  if (replacement.unchanged) continue;
  const temporary = `${replacement.destination}.role-split.tmp`;
  await writeFile(temporary, replacement.contents, { mode: 0o644 });
  await rename(temporary, replacement.destination);
}
console.log(JSON.stringify({
  providerVersion: manifest.version,
  fileSyncSha256: syncHash,
  cancellationOverlay: Object.fromEntries(replacements.map(({ name, replacementHash }) => [name, replacementHash])),
}));
