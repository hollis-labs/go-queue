# Changelog

All notable changes to `go-queue` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
