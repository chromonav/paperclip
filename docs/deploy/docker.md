---
title: Docker
summary: Docker Compose quickstart
---

Run Paperclip in Docker without installing Node or pnpm locally.

## Compose Quickstart (Recommended)

```sh
docker compose -f docker/docker-compose.quickstart.yml up --build
```

Open [http://localhost:3100](http://localhost:3100).

Defaults:

- Host port: `3100`
- Data directory: `./data/docker-paperclip`

Override with environment variables:

```sh
PAPERCLIP_PORT=3200 PAPERCLIP_DATA_DIR=../data/pc \
  docker compose -f docker/docker-compose.quickstart.yml up --build
```

**Note:** `PAPERCLIP_DATA_DIR` is resolved relative to the compose file (`docker/`), so `../data/pc` maps to `data/pc` in the project root.

## Manual Docker Build

```sh
docker build -t paperclip-local .
docker run --name paperclip \
  -p 3100:3100 \
  -e HOST=0.0.0.0 \
  -e PAPERCLIP_HOME=/paperclip \
  -v "$(pwd)/data/docker-paperclip:/paperclip" \
  paperclip-local
```

## Data Persistence

All data is persisted under the bind mount (`./data/docker-paperclip`):

- Embedded PostgreSQL data
- Uploaded assets
- Local secrets key
- Agent workspace data

## Stateful Coordinator and Stateless Readers

The Kubernetes renderer at `scripts/render-read-offload.mjs` keeps one coordinator responsible for execution and workspace state, with two stateless readers in front of it. The readers share authoritative PostgreSQL data and the coordinator's authentication secret, instance ID, and public origin; they do not mount its workspace PVC or initialize migrations, execution plugins, schedulers, or recovery loops.

Only the exact database-only routes in `server/src/read-offload-policy.ts` are offloaded: company listings/details/stats, goals, folders, dashboard, and recovery reports. Other requests—including agent and issue reads, mutations, assignments, Stop, workspace access, and WebSocket execution streams—are forwarded once to the coordinator. When it is unavailable, only the audited reads remain available; coordinator-dependent requests return `503`, and a cold UI load may remain incomplete. This is not a highly available coordinator.

Render manifests using a validated immutable image and an existing Secret containing `DATABASE_URL` and `BETTER_AUTH_SECRET`:

```sh
node scripts/render-read-offload.mjs \
  --image "$VALIDATED_IMAGE_DIGEST" \
  --public-url https://paperclip.example.com \
  --coordinator-name paperclip-app \
  --secret paperclip-app-secrets
```

This prints manifests only. By default it neither creates nor replaces the coordinator/PVC, and it never changes Ingress routing or copies credentials. Preserve the existing production 200Gi workspace volume. Review the generated resources and routing separately; do not introduce NFS or use object storage as a workspace filesystem.

For a **new isolated rehearsal**, `--create-coordinator` additionally requires an explicit block `--storage-class`; `--storage-size` defaults to `200Gi`. Use distinct rehearsal names and cloned data, not the production coordinator's name. Saved files rely on the existing sandbox sync-back path, not a persistent sandbox volume; work not yet synchronized can be lost in a crash.

`docker/read-offload/Dockerfile` is a deployed-baseline overlay, not a general replacement for the root Dockerfile: its pinned image must report commit `d554c4789ed3930f8a53ac9fdf6503b3187097da`. Build it from the matching backport with `READ_OFFLOAD_BUILD_COMMIT` set to the reviewed commit. It compiles the full server project using that image's existing dependencies.

The optional `--install-kubernetes-cancellation-overlay` flag is limited to an explicitly created rehearsal coordinator. Its init container checks the installed Kubernetes provider's `2026.1005.0` version and exact original/replacement file hashes before installing the two-file termination-receipt repair; it also verifies that file-sync code is unchanged. It is not a general plugin upgrade. Preserve the original provider files as well as the workload/routing manifests: reverting the application image alone does not undo files patched on the coordinator PVC. Drain work before any coordinator replacement or provider rollback.

## Local Adapter CLIs in Docker

The Docker image pre-installs these agent CLIs so their `*_local` adapters can run inside the container:

- `claude` (Anthropic Claude Code CLI) — `claude_local`
- `codex` (OpenAI Codex CLI) — `codex_local`
- `opencode` (OpenCode multi-provider CLI) — `opencode_local`
- `gemini` (Google Gemini CLI) — `gemini_local` (experimental)

Pass API keys to enable local adapter runs inside the container:

```sh
docker run --name paperclip \
  -p 3100:3100 \
  -e HOST=0.0.0.0 \
  -e PAPERCLIP_HOME=/paperclip \
  -e OPENAI_API_KEY=sk-... \
  -e ANTHROPIC_API_KEY=sk-... \
  -e GEMINI_API_KEY=... \
  -v "$(pwd)/data/docker-paperclip:/paperclip" \
  paperclip-local
```

Each adapter reads its provider's standard credentials — for example `ANTHROPIC_API_KEY` (Claude), `OPENAI_API_KEY` (Codex), and `GEMINI_API_KEY` or `GOOGLE_API_KEY` (Gemini). OpenCode is multi-provider and uses whichever provider key you supply.

> **Gemini key restrictions:** Google requires Gemini API keys to be *restricted* to the Gemini API (scoped in the Google Cloud console); unrestricted keys are blocked and `gemini_local` runs will fail with an auth error. Create a restricted key, or authenticate with `gemini auth login` (OAuth) and persist `~/.gemini` via the data volume so the credential survives container restarts.

The image sets `GEMINI_SANDBOX=false` so the Gemini CLI does not try to launch its own (Docker-in-Docker) sandbox inside the container. The `gemini_local` adapter already passes `--sandbox=none` per run, so this env var only matters if you invoke `gemini` manually inside the container; override it if you have nested-container support and want CLI-level sandboxing.

Without API keys, the app runs normally — adapter environment checks will surface missing prerequisites.
