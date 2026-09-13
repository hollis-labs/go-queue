# go-queue

A lightweight, driver-based job queue for Go. `go-queue` defines a small `Queue` interface and ships three backends — SQLite, in-memory, and no-op — along with a `Worker` that polls one or more named queues and dispatches jobs to registered handlers with retry, per-job max attempts, priority ordering across queues, and graceful shutdown via `context.Context`. It is intended for services that want Laravel-style background jobs without a broker such as Redis or RabbitMQ.

## Status

Pre-1.0 (`v0.2.x`). The public API is stable in shape — `Queue`, `Worker`, `WorkerOpts`, the driver packages — but minor breaks may still happen between `v0.x` releases. See [`CHANGELOG.md`](CHANGELOG.md) for per-release detail and pin a version in your `go.mod`.

Documentation: [pkg.go.dev/github.com/hollis-labs/go-queue](https://pkg.go.dev/github.com/hollis-labs/go-queue).

## Install

```bash
go get github.com/hollis-labs/go-queue
```

## Usage

Minimal example using the in-memory driver (adapted from `worker_test.go`):

```go
package main

import (
    "context"
    "fmt"
    "time"

    queue "github.com/hollis-labs/go-queue"
    "github.com/hollis-labs/go-queue/driver/memory"
)

func main() {
    ctx := context.Background()
    q := memory.New()

    // Enqueue a job.
    if err := q.Push(ctx, "greet", []byte(`"hello"`)); err != nil {
        panic(err)
    }

    // Build a worker and register a handler for the "greet" job type.
    w := queue.NewWorker(q, queue.WorkerOpts{
        StopWhenEmpty: true,
        PollInterval:  10 * time.Millisecond,
        MaxTries:      3,
    })
    w.Register("greet", func(_ context.Context, job *queue.QueuedJob) error {
        fmt.Printf("got job %s: %s\n", job.ID, string(job.Payload))
        return nil
    })

    // Blocks until the queue is drained (because of StopWhenEmpty) or ctx cancels.
    if err := w.Start(ctx); err != nil {
        panic(err)
    }
}
```

For a persistent backend, construct the SQLite driver with an open `*sql.DB`:

```go
import (
    "database/sql"

    qsqlite "github.com/hollis-labs/go-queue/driver/sqlite"
    _ "modernc.org/sqlite"
)

db, _ := sql.Open("sqlite", "jobs.db")
db.Exec("PRAGMA journal_mode=WAL")
q, err := qsqlite.New(db, qsqlite.Opts{})
```

Push options cover the common scheduling knobs:

```go
q.Push(ctx, "email", payload,
    queue.OnQueue("high"),
    queue.WithDelay(30*time.Second),
    queue.WithMaxTries(5),
)
```

## API Overview

Root package (`github.com/hollis-labs/go-queue`):

- `Queue` — interface every driver implements: `Push`, `Pop`, `Delete`, `Release`, `Size`, `Failed`.
- `QueuedJob` — struct returned by `Pop`, carrying `ID`, `Type`, `Queue`, `Payload`, `Attempts`, `MaxTries`, and timestamps.
- `Handler` — `func(ctx context.Context, job *QueuedJob) error`; registered on a `Worker` by job-type string.
- `Worker` / `NewWorker(q, opts)` — polls one or more queues and dispatches jobs to handlers.
- `WorkerOpts` — `Queues`, `Concurrency`, `PollInterval`, `MaxTries`, `RetryAfter`, `MaxMemoryMB`, `StopWhenEmpty`, `CanReserve`, optional `Controller`, plus `OnProcessing`, `OnProcessed`, `OnFailed`, `OnError` lifecycle callbacks.
- `PushOption` helpers: `OnQueue(name)`, `WithDelay(d)`, `WithMaxTries(n)`.
- `PushConfig` / `ResolvePushConfig(opts)` — exported so custom drivers can consume the resolved push options.
- Sentinels: `ErrNoJob`, `ErrHandlerNotFound`.

Drivers:

- `driver/memory` — `memory.New()` returns a mutex-protected, slice-backed `*Driver`. Adds `FailedJobs()` for test inspection.
- `driver/sqlite` — `sqlite.New(db, sqlite.Opts{...})` creates the `jobs` and `failed_jobs` tables (names configurable via `Opts.Table` / `Opts.FailedTable`) and returns a `*Driver`. `Opts.RetryAfter` (default 60s) controls how long a reserved job must sit before `Pop` reclaims it.
- `driver/noop` — `noop.New()` returns a `*Driver` that accepts pushes and drops them silently; `Pop` always returns `(nil, nil)`. Used when queueing is disabled.

## Architecture Notes

### Reversible worker drain

Pass a `*CycleController` to opt into whole-cycle pause and settlement tracking.
The zero value is ready to use:

```go
controller := &queue.CycleController{}
worker := queue.NewWorker(q, queue.WorkerOpts{Controller: controller})
// Register handlers and run worker.Start(workerCtx) in its owning goroutine.

pause := controller.Pause()
report, err := controller.WaitQuiescent(waitCtx, pause)
if err != nil {
    // Inspect report internally. This resumes only the pause, not a fault.
    _ = controller.Resume(pause)
    return err
}
// This worker has no active cycles or unknown dispositions while pause is held.
// Keep the handle held for the operation that requires quiescence.
return controller.Resume(pause)
```

A cycle enrolls before `CanReserve` and `Pop`, and remains active through the
handler, `Delete`/`Release`/`Failed`, and lifecycle callbacks. Thus pause cannot
miss a poll that passed eligibility but has not reserved yet, or a handler
whose result is not durably recorded. It does not cancel `workerCtx`. A canceled
or expired `waitCtx` stops only the wait; the caller decides whether to resume.
Callbacks must return before quiescence and must not wait on their own cycle.

`Pause` is nonnesting: concurrent/repeated calls while paused share one handle.
The caller must serialize ownership of `Resume`; one caller resuming releases
that pause for every holder. A zero, foreign or retired handle returns
`ErrStalePause`. One controller belongs permanently to one Worker, and concurrent
Starts or reuse by another worker return `ErrControllerInUse`. A paused worker
does not treat the queue as empty for `StopWhenEmpty`. Normal idle polling still
obeys `PollInterval`; resume does not promise to interrupt an existing poll sleep.

Managed cycles classify every driver error conservatively as `unknown`, even
`Pop(nil, err)` or a read-only `Size` error: the existing driver interface has
no richer outcome contract. An uncertain Pop may already have reserved a job,
and retrying Release/Failed after a lost acknowledgement could repeat an effect.
New cycles are quarantined as soon as the error is known; other enrolled cycles
finish. There is no automatic settlement replay or fault reset, including across
Starts of the same Worker. `Resume` may release a pause but cannot restore
polling after a fault. Plan reconciliation separately instead of discarding the
controller to claim a successful drain.

`WaitQuiescent` returns `ErrUnknownDisposition` after all active cycles finish
when any outcome remains uncertain. `CycleReport.Unknown` retains at most one
record per polling goroutine; `Last` is the most recent completed cycle. Results
name the operation, diagnostic cycle number, optional opaque job ID and original
error. Reports are detached snapshots; they may contain private driver details
and are **not safe wire/log payloads**. A report without a held pause is only an
observation. There is no claim that the shared queue is empty or other workers
are idle.

In managed mode, settlement errors reach `OnError`; `OnProcessed` and `OnFailed`
run only when their corresponding Delete/Failed succeeded. A missing handler
still reaches `OnError` with `ErrHandlerNotFound`, even if Failed also errors.
With `Controller:nil`, the prior behavior (including callbacks and handling of
driver errors) is unchanged. Drivers, retry limits, FIFO/reclaim semantics and
schema are unchanged. This API does not install a shutdown watcher or exit a
consumer process.

### Driver and worker ownership

The split between `Queue` (driver contract) and `Worker` (dispatch loop) means consumers depend on the interface, not on a concrete backend. The worker owns policy — poll interval, concurrency, retry counting, priority ordering — while drivers own storage and reservation semantics.

Priority is implemented by `WorkerOpts.Queues` order: `popNextJob` walks the slice and returns the first non-nil job, so listing `["high", "low"]` drains `high` before looking at `low` (see `TestWorkerPriorityQueues`). Per-job `WithMaxTries` overrides `WorkerOpts.MaxTries` when greater than zero (`TestWorkerPerJobMaxTries`).

The SQLite driver uses a monotonically increasing `id` with explicit `queue`, `reserved_at`, and `available_at` columns, and reclaims stuck reservations whose `reserved_at` is older than `Opts.RetryAfter`. All mutations in `Pop`, `Release`, and `Failed` run inside a transaction. `Release` deletes and re-inserts the row so FIFO ordering is preserved across retries while the attempt count is carried over. The in-memory driver mirrors this semantics in-process with a `sync.Mutex`-guarded slice.

## Dependencies

Direct:

- [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) — pure-Go SQLite driver used by `driver/sqlite` and its tests. Pinned in `go.mod`.

All other entries in `go.sum` are transitive dependencies of `modernc.org/sqlite`. The root package and the `memory`/`noop` drivers have no external dependencies.

## Testing

```bash
go test ./...
```

The SQLite driver tests use `:memory:` databases via the pure-Go `modernc.org/sqlite` driver, so no CGO toolchain or external SQLite install is required. No environment variables, fixtures, or external services are needed.

For runnable end-to-end examples, see the [`examples/`](examples/) directory.

## License

[MIT](LICENSE) © Hollis Labs.
