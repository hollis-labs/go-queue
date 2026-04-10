# Audit — go-queue

**Audited:** 2026-04-09
**Auditor:** general-purpose subagent (BOOT_STANDARDIZATION audit)
**Path:** libs/go-queue
**Kind:** lib

## Summary

`go-queue` is a well-structured, driver-based job queue with a clean interface (`Queue`), a concurrent `Worker`, and three backends (memory, SQLite, noop). Test coverage is good across the worker and all three drivers. The apply session adds the missing `LICENSE` and package docs, and wires `context.Context` through the storage drivers so cancellation reaches backend operations. `README.md` now reflects the license and the current API surface. The public queue interface already accepts `context.Context` as its first argument.

## Checklist

| # | Check | Status | Notes |
|---|---|---|---|
| 1 | `go.mod` present | pass | `github.com/hollis-labs/go-queue`, go 1.26.1 |
| 2 | `README.md` present (before this audit) | fail | No README of any kind existed prior to this audit. |
| 3 | `LICENSE` present | fail | No LICENSE file in the module root. |
| 4 | `doc.go` with `// Package X ...` godoc comment | pass | Added root `doc.go` plus `driver/memory/doc.go`, `driver/sqlite/doc.go`, and `driver/noop/doc.go`. |
| 5 | Module path matches intended repo layout | pass | Module path remains `github.com/hollis-labs/go-queue` per the standalone-repo decision recorded in `STANDARDIZATION_APPLY_DECISIONS.md`; README and package docs align with that. |
| 6 | README has standard sections (title, desc, install, usage, API, examples) | fail (pre-audit) / pass (post-audit) | The new `README.md` written by this audit follows the template. |
| 7 | Tests exist (`*_test.go`) | pass | 4 test files: `worker_test.go`, `driver/memory/memory_test.go`, `driver/noop/noop_test.go`, `driver/sqlite/sqlite_test.go`. Coverage was not measured — `go test` was not run (read-only audit). |
| 8 | Examples (`example_test.go` or `examples/`) | fail | No `example_test.go` anywhere in the tree, and no `examples/` directory. Tests serve as de-facto examples but are not godoc-visible. |
| 9 | State/session files NOT misclassified as library docs | pass | No `.agentrc/`, `BOOT.md`, `CLAUDE.md`, `bootstrap.md`, `boot-prompt.md`, or `boot/*.md` files present. |
| 10 | Public API sanity: errors typed/sentinel, context.Context first arg | pass | Sentinel errors (`ErrNoJob`, `ErrHandlerNotFound`) are defined and used correctly. Driver implementations now check `ctx.Err()` and use `ExecContext`, `BeginTx`, and `QueryRowContext` so cancellation propagates into backend operations. |
| 11 | `CHANGELOG.md` present (nice to have) | fail | None present. A `v0.1.0` tag exists in `.git/refs/tags/`, with no release notes. |
| 12 | No circular/suspicious deps on other framework libs | pass | Zero framework-internal dependencies. Only external dep is `modernc.org/sqlite`. |

## Findings — Required Fixes

1. **What:** `LICENSE` file is missing from the module root.
   **Why:** Without a license, the module cannot legally be consumed by downstream projects, including other libs in this framework.
   **Suggested fix:** Add a `LICENSE` file matching the framework-wide license policy and replace the placeholder line in the new `README.md` under "License" with the actual license name.

2. **What:** No `doc.go` or package godoc comment on the root `queue` package (or on any of the driver subpackages).
   **Why:** `pkg.go.dev`, `go doc`, and IDE tooling will render the package with no top-level description. For a library intended for external consumption, this is a documentation regression.
   **Suggested fix:** Add a `doc.go` at the root containing `// Package queue provides a driver-based job queue with pluggable backends (memory, sqlite, noop) and a polling worker with retries, per-job max attempts, and priority queues.` Do the same for `driver/memory`, `driver/sqlite`, and `driver/noop` with one-line descriptions.

3. **What:** Driver methods take `context.Context` but ignore it. In particular, `driver/sqlite` called `d.db.Exec(...)`, `d.db.Begin()`, and `d.db.QueryRow(...)` — the non-context variants — instead of `ExecContext`, `BeginTx`, and `QueryRowContext`.
   **Why:** A caller that cancels `ctx` (for example during graceful shutdown) will not propagate cancellation into any driver operation. Long-running SQL calls cannot be interrupted; worker shutdown latency is unbounded by the driver layer. This is especially bad for the SQLite driver where a blocked transaction on a busy DB will wait on the C mutex with no cancellation path.
   **Suggested fix:** In `driver/sqlite/sqlite.go`, switch every `db.Exec` → `db.ExecContext(ctx, ...)`, `db.Begin()` → `db.BeginTx(ctx, nil)`, and `tx.QueryRow` → `tx.QueryRowContext(ctx, ...)`. In `driver/memory/memory.go`, check `ctx.Err()` at the top of each method. Update method signatures to name the parameter `ctx` rather than `_`. This was completed in the apply session.

## Findings — Nice-to-Have

1. **What:** No `example_test.go` at the package root.
   **Why:** `example_test.go` files render on `pkg.go.dev` under "Examples" and are compiled as part of `go test`, guaranteeing they stay in sync with the API.
   **Suggested fix:** Add `example_test.go` with `ExampleNewWorker` demonstrating push → register → start using the `memory` driver, and a second example showing `OnQueue` + `WithDelay` + `WithMaxTries`.

2. **What:** No `CHANGELOG.md` despite a `v0.1.0` git tag.
   **Why:** Consumers cannot tell what changed between versions.
   **Suggested fix:** Add a keepachangelog-style `CHANGELOG.md` and an entry for `v0.1.0`.

3. **What:** `WorkerOpts.MaxMemoryMB` is declared in the struct but never read anywhere in `worker.go`.
   **Why:** Dead public API field. Either wire it up or remove it. A future caller will set it and be surprised when the worker does not respect it.
   **Suggested fix:** Either implement memory-based shutdown in `pollLoop` (e.g. via `runtime.MemStats`) or remove the field. If removed, document the removal in `CHANGELOG.md` as a minor breaking change before v1.

4. **What:** `Worker.Start` always returns `nil`.
   **Why:** The return type suggests it can fail but the body never propagates errors. Aggregating errors from `pollLoop` goroutines (e.g. when all driver `Pop` calls return errors) would make operational failures more visible.
   **Suggested fix:** Either tighten the signature to not return an error, or aggregate errors from the per-goroutine pollers via `errors.Join`.

5. **What:** `queue.Push` ignores `context.Context` for cancellation even at the interface level, because none of the driver implementations honor it.
   **Why:** Related to required fix #3 but at the interface-contract level — there is no documented expectation that drivers honor `ctx`.
   **Suggested fix:** Add a line to each `Queue` method's godoc stating "Respects ctx cancellation." Then hold implementations to that in code review.

6. **What:** SQLite `Release` preserves `created_at` but issues a new primary-key `id` by virtue of `DELETE` + `INSERT`. That is fine for FIFO but means monitoring tools keyed on job `id` lose continuity across retries.
   **Why:** Observability gotcha for operators building dashboards.
   **Suggested fix:** Document the behavior in the SQLite driver godoc, or add a second column like `root_id` that is preserved across releases.

## Prior Documentation

- No `README.md`, `README.original.md`, `CLAUDE.md`, `BOOT.md`, `bootstrap.md`, `boot-prompt.md`, `doc.go`, `CHANGELOG.md`, or `docs/` subdirectory existed before this audit. Nothing was renamed.
- No `.agentrc/` or session/state files were present in the target directory.
- `.git/` is a fully-initialized repo with a `v0.1.0` tag and an `origin/main` remote. No release notes accompany the tag.

## Public API Snapshot

### `queue.go` (package `queue`)

- Interface `Queue`:
  - `Push(ctx context.Context, jobType string, payload []byte, opts ...PushOption) error`
  - `Pop(ctx context.Context, queueName string) (*QueuedJob, error)`
  - `Delete(ctx context.Context, id string) error`
  - `Release(ctx context.Context, id string, delay time.Duration) error`
  - `Size(ctx context.Context, queueName string) (int, error)`
  - `Failed(ctx context.Context, job *QueuedJob, errMsg string) error`
- Struct `QueuedJob` — fields: `ID string`, `Type string`, `Queue string`, `Payload []byte`, `Attempts int`, `MaxTries int`, `CreatedAt time.Time`, `AvailableAt time.Time`, `ReservedAt *time.Time`.
- Type alias `Handler = func(ctx context.Context, job *QueuedJob) error`.
- Struct `PushConfig` — fields: `Queue string`, `Delay time.Duration`, `MaxTries int`.
- Type alias `PushOption = func(*PushConfig)`.
- Func `ResolvePushConfig(opts []PushOption) PushConfig`.
- Funcs `OnQueue(name string) PushOption`, `WithDelay(d time.Duration) PushOption`, `WithMaxTries(n int) PushOption`.

### `errors.go` (package `queue`)

- `var ErrNoJob = errors.New("no job available")`
- `var ErrHandlerNotFound = errors.New("handler not found for job type")`

### `worker.go` (package `queue`)

- Struct `WorkerOpts` — fields: `Queues []string`, `Concurrency int`, `PollInterval time.Duration`, `MaxTries int`, `RetryAfter time.Duration`, `MaxMemoryMB int`, `StopWhenEmpty bool`, plus callbacks `OnProcessing func(*QueuedJob)`, `OnProcessed func(*QueuedJob)`, `OnFailed func(*QueuedJob, error)`, `OnError func(error)`.
- Struct `Worker` (unexported fields).
- Func `NewWorker(q Queue, opts WorkerOpts) *Worker`.
- Method `(w *Worker) Register(jobType string, h Handler)`.
- Method `(w *Worker) Start(ctx context.Context) error`.

### `driver/memory/memory.go` (package `memory`)

- Struct `Driver` (unexported fields) — implements `queue.Queue`.
- Func `New() *Driver`.
- Methods `Push`, `Pop`, `Delete`, `Release`, `Size`, `Failed` (implement the `Queue` interface).
- Method `(d *Driver) FailedJobs() []queue.QueuedJob` — exported helper for tests.

### `driver/sqlite/sqlite.go` (package `sqlite`)

- Struct `Opts` — fields: `Table string`, `FailedTable string`, `RetryAfter time.Duration`.
- Struct `Driver` (unexported fields) — implements `queue.Queue`.
- Func `New(db *sql.DB, opts Opts) (*Driver, error)`.
- Methods `Push`, `Pop`, `Delete`, `Release`, `Size`, `Failed` (implement the `Queue` interface).

### `driver/sqlite/schema.go` (package `sqlite`)

- No exported symbols. Internal helpers `createTables` and constants `defaultJobsTable`, `defaultFailedTable` are unexported.

### `driver/noop/noop.go` (package `noop`)

- Struct `Driver` — no fields. Implements `queue.Queue`.
- Func `New() *Driver`.
- Methods `Push`, `Pop`, `Delete`, `Release`, `Size`, `Failed` (no-ops; `Pop` always returns `(nil, nil)`).

## Open Questions

1. Is `github.com/hollis-labs/go-queue` the canonical module path, or is this expected to move under a framework-wide org namespace? This affects what `go get` line the README advertises and whether a path rewrite is needed before v1.
2. What license does the framework intend for this lib? (Needed to resolve required fix #1.)
3. Was `WorkerOpts.MaxMemoryMB` ever wired up, or is it aspirational? (Affects nice-to-have #3 — fix vs. remove.)
4. Is there an intended upper bound on `Concurrency`, or any guidance on pairing it with the SQLite driver's single-writer characteristics? The SQLite driver serializes on a single `*sql.DB`, so `Concurrency > 1` on SQLite will contend on the DB-level lock. Worth a doc note either way.
5. Is the `v0.1.0` git tag the intended public release, or a placeholder? The absence of a `CHANGELOG.md` and `LICENSE` suggests the tag predates any release-readiness pass.
