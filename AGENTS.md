# go-queue

A driver-based background job queue: a small `Queue` interface, three backends
(SQLite, in-memory, no-op), and a `Worker` that polls named queues and
dispatches to registered handlers with retry, per-job max attempts, priority
across queues and context-driven graceful shutdown. It targets services that
want Laravel-style jobs without running a broker — there is no Redis, no
AMQP, and no distributed coordination.

## Start Here

- `README.md` has the minimal worker example.
- `queue.go` declares `Queue`, `QueuedJob`, `Handler` and the `Push` options.
- `worker.go` owns polling, dispatch, retry and shutdown.
- `driver/sqlite/sqlite.go` and `driver/sqlite/schema.go` are the durable
  backend; `driver/memory` and `driver/noop` are the other two.
- `errors.go` holds the sentinels.
- `examples/inmemory` is runnable.

## Commands

```bash
gofmt -l .
go vet ./...
go test -race -count=1 ./...
```

There is no CI workflow or Makefile in this repo.

## Boundaries

Consumers depend on the `Queue` interface, never on a concrete driver. Every
behavior below is asserted separately against both the memory and SQLite
drivers — when you add a driver, add the matching cases rather than assuming
the interface is enough.

FIFO ordering must survive a release: a job returned to the queue after a
failed attempt re-enters in order, not at the front (`TestReleaseFIFOOrdering`,
`TestSQLiteReleaseFIFO`). Getting this wrong turns one poison job into a hot
loop that starves everything behind it.

The SQLite driver is the only one that is durable and concurrent. Its pop must
stay safe across workers (`TestSQLiteConcurrentPop`), and stuck jobs — claimed
by a worker that died — are reclaimed rather than lost
(`TestSQLiteStuckJobReclaim`). It also creates its table on demand
(`TestSQLiteTableAutoCreate`), so there is no separate migration step to keep
in sync.

`WithMaxTries(0)` means inherit from `WorkerOpts.MaxTries`, not "never retry".
The resolved value is stored on the job at push time
(`TestMaxTriesStoredOnJob`), so changing the worker default does not
retroactively change jobs already queued.

The no-op driver must remain a real implementation of the interface, not a
panic stub — it is what lets a service disable queueing without branching at
every call site.
