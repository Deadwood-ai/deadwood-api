# Environment And Access Model

This file defines where agent-facing rules, app env, local access notes, and
credentials belong. It intentionally contains no real secrets.

## File Classes

| Class | Examples | Tracked? | May contain real secrets? |
| --- | --- | --- | --- |
| Agent rules | `AGENTS.md`, `frontend/AGENTS.md`, `docs/agents/*.md` | yes | no |
| Safe playbooks | `docs/playbooks/*.md` | yes | no |
| Local ops notes | `docs/ops/*`, `.codex/local-access.md` | no | yes |
| Codex MCP config | `.codex/config.toml` | no | yes |
| Backend/runtime env | `.env` | no | yes |
| Backend/runtime example | `.env.example` | yes | no |
| Frontend local profiles | `frontend/.env.dev.local`, `frontend/.env.prod.local` | no | public client keys only |
| Frontend example | `frontend/.env.local.example` | yes | no |
| Local keys/assets | `.local/`, `assets/`, `data/` | no | yes or large data |

## Production Database Reads

Use [trusted analyst access](../playbooks/analyst-database-access.md) for routine
production SQL. It defines the runtime credential contract, direct client and
required preflight independently of any colleague's credential provider or host.
Keep analyst credentials separate from application env and frontend profiles.
Existing purpose-specific monitor connections and local MCP configuration remain
separate; a configured connector or copied local file does not verify access on
this host. Missing access is a coverage gap, not a reason to use an administrator.

## Root `.env`

Use root `.env` for application/runtime variables loaded by Docker Compose,
`python-dotenv`, and `shared.settings`.

Expected app/runtime keys include:

- `ENV`
- `DEV_MODE`
- `COMPOSE_PROJECT_NAME`
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `PROCESSOR_PASSWORD`
- `STORAGE_SERVER_USERNAME`
- `STORAGE_SERVER_DATA_PATH`
- `SSH_PRIVATE_KEY_PASSPHRASE`
- `SSH_KNOWN_HOSTS_PATH`
- `DEADTREES_USER`
- `DEADTREES_PASSWORD`
- `REFERENCE_EXPORT_UUID`
- `NGINX_COG_URL`
- `LOGFIRE_TOKEN`
- `POSTHOG_API_KEY`
- `POSTHOG_PROJECT_ID`
- `POSTHOG_HOST`
- `ZULIP_EMAIL`
- `ZULIP_API_KEY`
- `ZULIP_SITE`
- `ZULIP_STREAM`
- `ZULIP_TOPIC`
- `LINEAR_ENABLED`
- `LINEAR_API_KEY`
- `LINEAR_TEAM_ID`
- `FREIDATA_TOKEN`
- `BREVO_API_KEY`
- `NOTIFICATION_SENDER_EMAIL`
- `NOTIFICATION_SENDER_NAME`
- `PROCESSING_EMAIL_NOTIFICATIONS_ENABLED`
- `PROCESSING_FAILURE_EMAIL_HOLIDAY_NOTE_UNTIL`

Do not add VPN passwords, manual SSH passwords, personal tokens, or explanatory
access notes to root `.env`. Put those in local-only access notes or the user
machine's secure credential store.

## Frontend Env

Vite exposes `VITE_*` values to browser code. Only browser-safe public client
configuration belongs in `frontend/.env*`.

Use:

- `frontend/.env.dev.local` for local Supabase/API development
- `frontend/.env.prod.local` for a local frontend pointed at production services
- `frontend/.env.local.example` as the tracked key schema

Never put service-role keys, SSH credentials, VPN credentials, MCP bearer tokens,
or private API keys in frontend env files.

## Codex Local Files

`.codex/config.toml` is for repo-local MCP/server configuration and may contain
database URLs or bearer tokens. It is ignored and should remain local.

`.codex/local-access.md` is for Codex-specific access notes that future local
Codex sessions can read. It may contain credentials, so it must stay ignored.

When inspecting either file, report only the presence, purpose, and line numbers
of risks. Do not print credential values.

## Human Access Notes

`docs/ops/*` is local-only operational documentation. It can contain machine
names, private access workflows, passwords, or links to restricted docs.

Safe tracked docs may point to `docs/ops/*`, but must not copy credentials out of
that directory.

## Worktrees

`scripts/setup-worktree.sh` copies ignored local files from the primary checkout
when available:

- `.codex/config.toml`
- `.codex/local-access.md`
- `.codex/environments`
- `docs/ops`
- frontend env profiles

This keeps new Codex worktrees usable without committing secrets. If a copied
local file is stale, update the primary checkout's local-only file rather than
adding secrets to tracked docs.

The setup script fetches `origin` and fails before installing dependencies when
the worktree does not include current `origin/main`. Use `--base-ref REF` when
the user explicitly names another base. `--allow-stale-base` is reserved for
intentional older-base work, and `--skip-git-fetch` is for deliberate offline
use with a trusted cached ref. To run only this check:

```bash
bash scripts/setup-worktree.sh --git-preflight-only
```

Start implementation, QA, and broad test work from current `origin/main` unless
the user names another base:

```bash
git fetch origin main --prune
git worktree add <path> -b <branch-name> origin/main
```

### Isolated Dev/Test Stack

Use the lightweight setup only when the task does not need a running app stack.
Exploration, code reading, docs edits, static analysis, narrow unit tests, lint,
and review-only work usually do not need Docker, Supabase, API, Mailpit, or Vite.

Use the isolated per-worktree stack before any work that needs a realistic local
application environment:

- full API or broad test-suite work,
- local E2E tests,
- browser validation,
- QA-agent playbooks,
- Supabase/Auth/Mailpit flows,
- upload, download, storage, or fixture-backed API flows,
- debugging behavior that depends on frontend/API/Supabase integration.

Bootstrap and validate the stack in this order:

```bash
bash scripts/setup-worktree.sh --skip-assets
scripts/qa/env.sh up
scripts/qa/env.sh reset
scripts/qa/validate-isolated-env.sh
```

When running commands manually against the isolated stack, source the generated
env last so shell commands, Docker Compose, Vite, Playwright, and test helpers
use the worktree-specific ports and data root:

```bash
set -a
source .local/supabase/current.env
set +a
```

Useful follow-up commands:

```bash
scripts/qa/env.sh status
scripts/qa/run-agent-qa.sh --dry-run --parallel 4
scripts/qa/env.sh down
```

`scripts/qa/env.sh up` starts Docker containers and Vite, so do not run it as a
default step for every exploratory thread. Run it when the user asks for a full
test suite, local QA, browser/app verification, or a fix that should be tested
against the integrated local environment. After finishing, stop the stack with
`scripts/qa/env.sh down` unless the user wants to keep it running.

Codex environment cleanup is wired to `scripts/qa/env.sh cleanup`, which tears
down an already-rendered QA stack without creating new runtime state. Treat that
as a last-resort closeout hook; still run `scripts/qa/env.sh down` explicitly
before abandoning a worktree or thread.

## Secret Hygiene

- Redact command output that includes `password`, `token`, `secret`, `key`,
  `Authorization`, or database URLs.
- Avoid broad `codex mcp list` or config dumps unless output is capped and redacted.
- Rotate credentials if they were printed into an agent session, shared log, PR,
  or tracked file.
