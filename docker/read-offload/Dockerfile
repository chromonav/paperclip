# This backport deliberately keeps the deployed dependency graph, UI, CLI tools
# and native runner. It is not an application/schema upgrade.
FROM ghcr.io/paperclipai/paperclip@sha256:a02ac35ac41df911af477422ea0e781cf41d2b2c600c66f0a5ac9d8c63f52c2c
WORKDIR /app
RUN node -e 'if (require("./server/dist/build-info.json").commit !== "d554c4789ed3930f8a53ac9fdf6503b3187097da") throw new Error("Unexpected backport image baseline")'
COPY --chown=node:node server/src/ server/src/
COPY --chown=node:node packages/adapter-utils/src/ packages/adapter-utils/src/
COPY --chown=node:node packages/adapters/codex-local/src/server/execute.ts packages/adapters/codex-local/src/server/execute.ts
COPY --chown=node:node packages/adapters/claude-local/src/server/execute.ts packages/adapters/claude-local/src/server/execute.ts
COPY --chown=node:node packages/adapters/opencode-local/src/server/execute.ts packages/adapters/opencode-local/src/server/execute.ts
RUN rm packages/adapter-utils/src/acpx-engine/startup-cancellation.ts
COPY --chown=node:node packages/plugins/sandbox-providers/kubernetes/src/plugin.ts packages/plugins/sandbox-providers/kubernetes/src/lease-lifecycle.ts /tmp/read-offload-kubernetes/
COPY --chown=node:node docker/read-offload/install-kubernetes-overlay.mjs docker/read-offload/install-kubernetes-overlay.mjs
RUN node -e 'const fs = require("node:fs"), { createRequire } = require("node:module"); const esbuild = createRequire(require.resolve("tsx/package.json", { paths: ["./server"] }))("esbuild"); const output = "docker/read-offload/kubernetes-overlay"; fs.mkdirSync(output, { recursive: true }); for (const name of ["plugin", "lease-lifecycle"]) fs.writeFileSync(`${output}/${name}.js`, esbuild.transformSync(fs.readFileSync(`/tmp/read-offload-kubernetes/${name}.ts`, "utf8"), { loader: "ts", format: "esm", target: "es2023" }).code);'
ARG READ_OFFLOAD_BUILD_COMMIT
ENV PAPERCLIP_BUILD_COMMIT=${READ_OFFLOAD_BUILD_COMMIT}
RUN test -n "$PAPERCLIP_BUILD_COMMIT" \
    && ./server/node_modules/.bin/tsc --project server/tsconfig.json \
    && node server/scripts/write-build-stamp.mjs
# Coordinator remains the inherited default command. Reader deployments select
# reader.js explicitly; the coordinator entrypoint refuses the reader role.
