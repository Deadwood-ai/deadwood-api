# DeadTrees Agent Instructions

Codex is the primary coding agent for this repository. Start here, then open
the linked docs that match the task.

## Source Of Truth

- Repo-wide agent guide: `docs/agents/README.md`
- Core engineering rules: `docs/agents/rules.md`
- Project structure guidance: `docs/agents/project-structure.md`
- Environment and access model: `docs/agents/environment-and-access.md`
- Platform status playbook: `docs/playbooks/platform-status-check.md`
- Frontend-specific agent rules: `frontend/AGENTS.md`

## Safety

- Do not commit, push, open a PR, or mutate production unless the user explicitly asks.
- Do not print or copy credentials into chat, tracked docs, examples, or `frontend/.env*`.
- Keep large searches, logs, tests, and diagnostics capped and summarized. Prefer targeted
  `rg`, `jq`, `head`, `tail`, and explicit output limits.
- Production code changes should land through PR review and the merge-to-main deploy path.
  Manual production intervention requires explicit user approval.
- For routine production reads, follow [trusted analyst access](docs/playbooks/analyst-database-access.md).
  For local database work, use the configured local Supabase/Postgres MCP or documented
  isolated database path. Production writes require explicit authorization for a specific write.

## Local Test Environment

- For exploration, code reading, docs edits, static analysis, and narrow unit or
  lint checks, do not start the full local dev stack unless the task needs it.
- For full test-suite work, API tests against Supabase, local E2E, browser
  validation, QA-agent runs, Auth/Mailpit flows, upload/download flows, or any
  work that needs the app running, first bootstrap and validate the isolated
  per-worktree environment:

```bash
bash scripts/setup-worktree.sh --skip-assets
scripts/qa/env.sh up
scripts/qa/env.sh reset
scripts/qa/validate-isolated-env.sh
```

- Use the generated `.local/supabase/current.env` endpoints for frontend, API,
  Supabase, Mailpit, Docker Compose, Playwright, and QA agents. Do not use the
  default shared Supabase ports for full QA/test work.
- See `docs/agents/environment-and-access.md` for the routing table, manual
  `source` command, and teardown guidance.

## Review guidelines

- Review the PR merge diff and inspect affected callers, tests, contracts, and
  surrounding code before reporting a finding. Verify each finding against the
  actual code path; do not report speculative edge cases.
- Report only high-confidence P0/P1 issues with a concrete correctness, security,
  data-loss, production, testability, or maintainability risk introduced by the PR.
- Do not approve merely because behavior appears correct or tests pass. The PR must
  not leave the changed code structurally worse, more tangled, or materially harder
  to understand, test, and change.
- Be ambitious about structural simplification. For every meaningful change, ask
  whether a "code judo" reframing could delete whole branches, flags, wrappers,
  modes, duplicate flows, conditionals, concepts, or layers while preserving
  behavior. Prefer deleting complexity over rearranging or distributing it.
- Prioritize structural regressions and clear missed simplifications before local
  cleanup: spaghetti growth, confused ownership, abstraction or boundary leaks,
  unnecessary concepts, and oversized modules matter more than cosmetic issues.
- Treat readability, maintainability, and testability as review concerns when they
  create concrete future risk: duplicated workflow logic, unclear ownership, hidden
  side effects, brittle coupling, hard-to-test flows, oversized unstructured
  functions, or confusing domain names.
- Flag ad-hoc conditionals, one-off booleans, nullable modes, or feature checks
  scattered through unrelated or shared flows when they make behavior materially
  harder to reason about. Prefer a focused abstraction, typed model, dispatcher, or
  canonical owner that makes branches disappear.
- For architecture-sensitive changes, check whether the PR spreads behavior across
  callers, weakens locality, leaks feature logic into shared paths, duplicates an
  existing canonical helper, or introduces shallow pass-through modules that make
  future changes harder to verify.
- Flag thin wrappers, identity abstractions, unnecessary generic mechanisms, and
  cast-heavy or overly optional contracts when they obscure the real invariant
  without simplifying the API.
- For changed interfaces, check that callers do not need to know hidden ordering,
  config, error modes, permission rules, storage details, or deployment assumptions
  that should stay behind one module interface.
- Treat the following as presumptive P1 blockers unless the PR provides a compelling
  structural justification:
  - a file crossing from below 1000 lines to above 1000 lines;
  - new special-case branches, nullable modes, or feature checks tangled into an
    already busy or unrelated flow;
  - feature-specific behavior scattered across shared code instead of owned by one
    canonical module;
  - an unnecessary wrapper, pass-through abstraction, generic mechanism, cast, or
    optional contract that makes the real design less direct;
  - duplicated logic or a bespoke helper where an existing canonical helper or
    layer already owns the concept;
  - a refactor that moves complexity around without reducing the concepts a reader
    must understand.
- Flag related updates that can leave state partially applied, and flag sequential
  orchestration of independent work when it introduces avoidable failure coupling
  or materially complicates the flow.
- For database, storage, deployment, or production-connected changes, require evidence of repo-approved validation and flag unsafe production assumptions.
- For security, flag changes that weaken auth, authorization, RLS/policies, tenancy isolation, signed URL handling, upload validation, secret handling, logging of sensitive data, CORS/cookie/session controls, or privilege boundaries.
- Treat credentials, bearer tokens, database URLs, private keys, raw production access notes, and PII in tracked files or logs as P1.
- Prefer a small number of high-conviction findings over a long list of nits. Each
  finding should identify the concrete risk, the smallest reasonable fix, and the
  validation that would prove the fix.
- Do not flag subjective style preferences, formatting, naming-only suggestions, or
  broad cleanup ideas unless they affect correctness, security, maintainability, or
  testability in the changed code.

## Git And PRs

- Before creating a worktree/branch for implementation, QA, or broad tests,
  fetch `origin` and base it on current `origin/main` unless told otherwise.
- Do not create draft PRs for this workspace. Open normal PRs when asked.
- PR titles must pass `.github/workflows/pr-title-check.yml`.
- Use Conventional Commit format: `type(scope): short summary`.
- Allowed types: `feat`, `fix`, `docs`, `chore`, `refactor`, `perf`, `test`, `build`, `ci`, `revert`.
- Good example: `fix(frontend): align tree and deadwood cover labels`.
- Use [`.github/pull_request_template.md`](.github/pull_request_template.md) for PR descriptions and keep the body synchronized with the final diff and validation evidence.
- Add the applicable release-grouping labels from [`.github/release.yml`](.github/release.yml).

## Local-Only Access

Credentials and machine-specific access notes are intentionally not tracked.

- Human-readable local notes: `docs/ops/local-access.md` if present
- Codex-local notes and MCP credentials: `.codex/local-access.md` and `.codex/config.toml`
- App/runtime env: `.env`
- Browser-facing frontend profiles: `frontend/.env.dev.local`, `frontend/.env.prod.local`

If a local-only file is missing, use `docs/agents/environment-and-access.md` to understand
where it belongs. Ask the user for credentials instead of inventing or moving secrets.
