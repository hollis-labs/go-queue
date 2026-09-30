# Changelog

All notable changes to `go-queue` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Fixed

- The SQLite driver's `Pop`, `Release` and `Failed` now take the writer lock when
  they begin (`BEGIN IMMEDIATE`, issued on a dedicated connection) instead of racing
  for it at their first write. They used a deferred `BEGIN`, so two connections
  could both read the same row and then collide on upgrade; under WAL that is
  `SQLITE_BUSY_SNAPSHOT`, which `busy_timeout` does not retry, and callers saw
  `database is locked`. It only showed up with more than one connection to a
  file database, so the existing `:memory:` single-connection concurrency test
  never exercised it. The driver no longer depends on how the caller opened the
  pool (no `_txlock=immediate` needed). Internal locking only: no change to the
  `Queue` interface, `Opts`, `New`, or the module's dependencies.

## v0.2.0 — 2026-09-11

Additive. `nil` behaviour is identical to `v0.1.0`, so upgrading requires no
source change.

### Added

- `WorkerOpts.CanReserve func(context.Context) bool` — an optional gate asked
  once per poll cycle, before the worker reserves a job. Returning false skips
  the cycle: nothing is reserved, the worker sleeps `PollInterval`, and it asks
  again. `nil` means always eligible, which is how every worker behaved before
  this option existed.

  Written for deference between processes sharing a queue — a leader that owns
  the work, a maintenance window, a drain-down before shutdown.

  It gates only *new* reservations: a job already held runs to completion under
  a live context. That is the distinction from cancelling the worker's context,
  which aborts the handler mid-flight and then fails the bookkeeping after it,
  leaving the job reserved and its attempt spent.

### Notes

- **`CanReserve` and `StopWhenEmpty` interact.** A gated cycle never inspects
  the queue, so it cannot conclude the queue is drained and will not trigger
  `StopWhenEmpty`. A worker told it may not reserve is deferring, not finished,
  and waits rather than exiting.
- The gate is called on the polling goroutine and must not block for long or
  panic. This package recovers panics nowhere — not in handlers either — so one
  raised in the gate takes down the polling goroutine.
- `WorkerOpts.MaxMemoryMB` remains declared but not honoured, unchanged from
  `v0.1.0`.

## v0.1.0 — 2026-05-10

First public release. The previous private `v0.1.0` / `v0.1.1` / `v0.1.2`
tags from the pre-public-prep window were not published through
`proxy.golang.org` and are reset to this commit at release time; consumers
should treat `v0.1.0` (this entry) as the canonical first release.

### Added

- `Queue` interface (`Push`, `Pop`, `Delete`, `Release`, `Size`, `Failed`)
  with `context.Context` as the first argument on every method.
- `Worker` with poll-loop dispatch, per-job retry counting, configurable
  `MaxTries` / `RetryAfter`, priority queues via `WorkerOpts.Queues`
  ordering, `StopWhenEmpty` for one-shot drains, and lifecycle callbacks
  (`OnProcessing`, `OnProcessed`, `OnFailed`, `OnError`).
- `Handler` function type and per-`jobType` registration via
  `Worker.Register`.
- Push options: `OnQueue`, `WithDelay`, `WithMaxTries`. Resolved values
  exposed via `PushConfig` / `ResolvePushConfig` for custom drivers.
- `driver/memory` — mutex-guarded in-process driver with a `FailedJobs()`
  helper for tests.
- `driver/sqlite` — pure-Go SQLite-backed driver (via `modernc.org/sqlite`)
  with configurable `Table` / `FailedTable` names, transactional `Pop` /
  `Release` / `Failed`, and `Opts.RetryAfter`-bounded reservation reclaim.
- `driver/noop` — accepts and drops jobs; `Pop` always returns `(nil, nil)`.
  For seams where queueing is disabled at runtime.
- Sentinel errors `ErrNoJob` and `ErrHandlerNotFound`.
- `LICENSE` (MIT, Hollis Labs), `README.md`, `CHANGELOG.md`, root `doc.go`,
  and `examples/inmemory` runnable example.

### Notes

- `WorkerOpts.MaxMemoryMB` is currently declared but not honoured; tracked
  for either implementation or removal in a subsequent release.
- All driver methods now respect `ctx` cancellation: the SQLite driver
  uses `ExecContext` / `BeginTx` / `QueryRowContext`, and the in-memory
  driver checks `ctx.Err()` at method entry. This is the contract every
  driver — first-party or third-party — must uphold.
