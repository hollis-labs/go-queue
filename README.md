# go-queue

A lightweight, driver-based job queue for Go. `go-queue` defines a small `Queue` interface and ships three backends — SQLite, in-memory, and no-op — along with a `Worker` that polls one or more named queues and dispatches jobs to registered handlers with retry, per-job max attempts, priority ordering across queues, and graceful shutdown via `context.Context`. It is intended for services that want Laravel-style background jobs without a broker such as Redis or RabbitMQ.

## Status

Beta. The public API appears stable, the module is tagged `v0.1.0`, and all three drivers plus the worker are covered by tests. No `CHANGELOG.md` or public release notes are present, so API churn guarantees are unclear.

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
- `WorkerOpts` — `Queues`, `Concurrency`, `PollInterval`, `MaxTries`, `RetryAfter`, `MaxMemoryMB`, `StopWhenEmpty`, plus `OnProcessing`, `OnProcessed`, `OnFailed`, `OnError` lifecycle callbacks.
- `PushOption` helpers: `OnQueue(name)`, `WithDelay(d)`, `WithMaxTries(n)`.
- `PushConfig` / `ResolvePushConfig(opts)` — exported so custom drivers can consume the resolved push options.
- Sentinels: `ErrNoJob`, `ErrHandlerNotFound`.

Drivers:

- `driver/memory` — `memory.New()` returns a mutex-protected, slice-backed `*Driver`. Adds `FailedJobs()` for test inspection.
- `driver/sqlite` — `sqlite.New(db, sqlite.Opts{...})` creates the `jobs` and `failed_jobs` tables (names configurable via `Opts.Table` / `Opts.FailedTable`) and returns a `*Driver`. `Opts.RetryAfter` (default 60s) controls how long a reserved job must sit before `Pop` reclaims it.
- `driver/noop` — `noop.New()` returns a `*Driver` that accepts pushes and drops them silently; `Pop` always returns `(nil, nil)`. Used when queueing is disabled.

## Architecture Notes

The split between `Queue` (driver contract) and `Worker` (dispatch loop) means consumers depend on the interface, not on a concrete backend. The worker owns policy — poll interval, concurrency, retry counting, priority ordering — while drivers own storage and reservation semantics.

Priority is implemented by `WorkerOpts.Queues` order: `popNextJob` walks the slice and returns the first non-nil job, so listing `["high", "low"]` drains `high` before looking at `low` (see `TestWorkerPriorityQueues`). Per-job `WithMaxTries` overrides `WorkerOpts.MaxTries` when greater than zero (`TestWorkerPerJobMaxTries`).

The SQLite driver uses a monotonically increasing `id` with explicit `queue`, `reserved_at`, and `available_at` columns, and reclaims stuck reservations whose `reserved_at` is older than `Opts.RetryAfter`. All mutations in `Pop`, `Release`, and `Failed` run inside a transaction. `Release` deletes and re-inserts the row so FIFO ordering is preserved across retries while the attempt count is carried over. The in-memory driver mirrors this semantics in-process with a `sync.Mutex`-guarded slice.

## Dependencies

Framework-internal: none.

External (direct):

- `modernc.org/sqlite` v1.48.1 — pure-Go SQLite driver used by `driver/sqlite` and its tests.

All other entries in `go.sum` are transitive dependencies of `modernc.org/sqlite`.

## Testing

```bash
go test ./...
```

The SQLite driver tests use `:memory:` databases via the pure-Go `modernc.org/sqlite` driver, so no CGO toolchain or external SQLite install is required. No environment variables, fixtures, or external services are needed.

## License

MIT License
