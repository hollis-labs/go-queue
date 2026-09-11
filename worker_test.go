package queue_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	queue "github.com/hollis-labs/go-queue"
	"github.com/hollis-labs/go-queue/driver/memory"
	qsqlite "github.com/hollis-labs/go-queue/driver/sqlite"
	_ "modernc.org/sqlite"
)

func TestWorkerDispatch(t *testing.T) {
	q := memory.New()
	ctx := context.Background()

	if err := q.Push(ctx, "greet", []byte(`"hello"`)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	var received atomic.Value

	w := queue.NewWorker(q, queue.WorkerOpts{
		StopWhenEmpty: true,
		PollInterval:  10 * time.Millisecond,
	})
	w.Register("greet", func(_ context.Context, job *queue.QueuedJob) error {
		received.Store(string(job.Payload))
		return nil
	})

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	got, ok := received.Load().(string)
	if !ok || got != `"hello"` {
		t.Fatalf("expected payload %q, got %q", `"hello"`, got)
	}
}

func TestWorkerRetryOnError(t *testing.T) {
	q := memory.New()
	ctx := context.Background()

	if err := q.Push(ctx, "flaky", []byte(`{}`)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	var attempts atomic.Int32

	w := queue.NewWorker(q, queue.WorkerOpts{
		StopWhenEmpty: true,
		PollInterval:  10 * time.Millisecond,
		MaxTries:      3,
		RetryAfter:    0,
	})
	w.Register("flaky", func(_ context.Context, job *queue.QueuedJob) error {
		attempts.Add(1)
		return errors.New("always fails")
	})

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}

	failed := q.FailedJobs()
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed job, got %d", len(failed))
	}
}

func TestWorkerUnregisteredHandler(t *testing.T) {
	q := memory.New()
	ctx := context.Background()

	if err := q.Push(ctx, "unknown", []byte(`{}`)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	var onErrorCalled atomic.Bool

	w := queue.NewWorker(q, queue.WorkerOpts{
		StopWhenEmpty: true,
		PollInterval:  10 * time.Millisecond,
		OnError: func(err error) {
			onErrorCalled.Store(true)
		},
	})

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !onErrorCalled.Load() {
		t.Fatal("expected OnError to be called for unregistered handler")
	}

	failed := q.FailedJobs()
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed job, got %d", len(failed))
	}
}

func TestWorkerGracefulShutdown(t *testing.T) {
	q := memory.New()
	ctx, cancel := context.WithCancel(context.Background())

	if err := q.Push(ctx, "slow", []byte(`{}`)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	var started sync.WaitGroup
	started.Add(1)

	w := queue.NewWorker(q, queue.WorkerOpts{
		PollInterval: 10 * time.Millisecond,
	})
	w.Register("slow", func(hCtx context.Context, job *queue.QueuedJob) error {
		started.Done()
		select {
		case <-hCtx.Done():
		case <-time.After(100 * time.Millisecond):
		}
		return nil
	})

	done := make(chan error, 1)
	go func() {
		done <- w.Start(ctx)
	}()

	// Wait for handler to start, then cancel.
	started.Wait()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}

func TestWorkerCallbacks(t *testing.T) {
	q := memory.New()
	ctx := context.Background()

	if err := q.Push(ctx, "cb-test", []byte(`{}`)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	var processingCalled, processedCalled atomic.Bool

	w := queue.NewWorker(q, queue.WorkerOpts{
		StopWhenEmpty: true,
		PollInterval:  10 * time.Millisecond,
		OnProcessing: func(job *queue.QueuedJob) {
			processingCalled.Store(true)
		},
		OnProcessed: func(job *queue.QueuedJob) {
			processedCalled.Store(true)
		},
	})
	w.Register("cb-test", func(_ context.Context, job *queue.QueuedJob) error {
		return nil
	})

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !processingCalled.Load() {
		t.Fatal("expected OnProcessing to be called")
	}
	if !processedCalled.Load() {
		t.Fatal("expected OnProcessed to be called")
	}
}

func TestWorkerOnFailedCallback(t *testing.T) {
	q := memory.New()
	ctx := context.Background()

	if err := q.Push(ctx, "doomed", []byte(`{}`)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	var failedJobType atomic.Value

	w := queue.NewWorker(q, queue.WorkerOpts{
		StopWhenEmpty: true,
		PollInterval:  10 * time.Millisecond,
		MaxTries:      1,
		OnFailed: func(job *queue.QueuedJob, err error) {
			failedJobType.Store(job.Type)
		},
	})
	w.Register("doomed", func(_ context.Context, job *queue.QueuedJob) error {
		return errors.New("doomed to fail")
	})

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	got, ok := failedJobType.Load().(string)
	if !ok || got != "doomed" {
		t.Fatalf("expected OnFailed with job type %q, got %q", "doomed", got)
	}
}

func TestWorkerPriorityQueues(t *testing.T) {
	q := memory.New()
	ctx := context.Background()

	// Push to low first, then high, then low again.
	if err := q.Push(ctx, "task", []byte(`"low-1"`), queue.OnQueue("low")); err != nil {
		t.Fatalf("Push low-1: %v", err)
	}
	if err := q.Push(ctx, "task", []byte(`"high-1"`), queue.OnQueue("high")); err != nil {
		t.Fatalf("Push high-1: %v", err)
	}
	if err := q.Push(ctx, "task", []byte(`"low-2"`), queue.OnQueue("low")); err != nil {
		t.Fatalf("Push low-2: %v", err)
	}

	var mu sync.Mutex
	var order []string

	w := queue.NewWorker(q, queue.WorkerOpts{
		Queues:        []string{"high", "low"},
		StopWhenEmpty: true,
		PollInterval:  10 * time.Millisecond,
	})
	w.Register("task", func(_ context.Context, job *queue.QueuedJob) error {
		mu.Lock()
		order = append(order, string(job.Payload))
		mu.Unlock()
		return nil
	})

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(order) < 1 {
		t.Fatal("no jobs processed")
	}
	if order[0] != `"high-1"` {
		t.Fatalf("expected high-1 first, got %s (full order: %v)", order[0], order)
	}
}

func TestWorkerWithSQLite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	db.Exec("PRAGMA journal_mode=WAL")

	q, err := qsqlite.New(db, qsqlite.Opts{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var results []string
	var mu sync.Mutex

	w := queue.NewWorker(q, queue.WorkerOpts{
		PollInterval:  10 * time.Millisecond,
		MaxTries:      2,
		RetryAfter:    0,
		StopWhenEmpty: true,
	})
	w.Register("process", func(_ context.Context, job *queue.QueuedJob) error {
		mu.Lock()
		results = append(results, string(job.Payload))
		mu.Unlock()
		return nil
	})

	_ = q.Push(ctx, "process", []byte("a"))
	_ = q.Push(ctx, "process", []byte("b"))
	_ = q.Push(ctx, "process", []byte("c"))

	_ = w.Start(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(results) != 3 {
		t.Fatalf("processed %d jobs, want 3", len(results))
	}
	for i, want := range []string{"a", "b", "c"} {
		if results[i] != want {
			t.Errorf("results[%d] = %q, want %q", i, results[i], want)
		}
	}
}

func TestWorkerPerJobMaxTries(t *testing.T) {
	q := memory.New()
	ctx := context.Background()

	// WorkerOpts.MaxTries=10 (high), but job has WithMaxTries(2).
	if err := q.Push(ctx, "bounded", []byte(`{}`), queue.WithMaxTries(2)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	var attempts atomic.Int32

	w := queue.NewWorker(q, queue.WorkerOpts{
		StopWhenEmpty: true,
		PollInterval:  10 * time.Millisecond,
		MaxTries:      10,
		RetryAfter:    0,
	})
	w.Register("bounded", func(_ context.Context, job *queue.QueuedJob) error {
		attempts.Add(1)
		return errors.New("always fails")
	})

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected 2 attempts (per-job MaxTries), got %d", got)
	}

	failed := q.FailedJobs()
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed job, got %d", len(failed))
	}
}

// TestWorkerCanReserveGatesReservation covers the gate's three states against
// the durable driver: closed takes nothing, open takes work, and nil behaves
// exactly as a worker built before the option existed.
//
// The assertion is on `attempts`, which the driver increments inside Pop. That
// makes it a direct observation of whether this worker RESERVED anything,
// rather than an inference from whether a handler happened to run.
func TestWorkerCanReserveGatesReservation(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:canreserve?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	q, err := qsqlite.New(db, qsqlite.Opts{})
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}
	if err := q.Push(ctx, "gated", []byte(`{}`)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	var open atomic.Bool
	var handled atomic.Int64
	w := queue.NewWorker(q, queue.WorkerOpts{
		PollInterval: 5 * time.Millisecond,
		CanReserve:   func(context.Context) bool { return open.Load() },
	})
	w.Register("gated", func(context.Context, *queue.QueuedJob) error {
		handled.Add(1)
		return nil
	})

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Start(runCtx) }()

	// Closed: the job must sit untouched. attempts stays 0 because Pop is never
	// reached.
	time.Sleep(80 * time.Millisecond)
	var attempts, present int
	if err := db.QueryRow(`SELECT COUNT(*), COALESCE(MAX(attempts), 0) FROM jobs WHERE type = 'gated'`).
		Scan(&present, &attempts); err != nil {
		t.Fatalf("read job state: %v", err)
	}
	// Three ways this can be wrong, and each gets said plainly: the job was
	// reserved, the job was run, or the job is gone entirely because it was
	// reserved AND run while the gate was closed.
	if present == 0 {
		t.Fatalf("the gated job left the queue entirely (handled=%d): it was reserved and completed "+
			"while CanReserve returned false, so the gate is not consulted before reservation",
			handled.Load())
	}
	if attempts != 0 || handled.Load() != 0 {
		t.Fatalf("a gated worker reserved a job: attempts=%d handled=%d. CanReserve is not being "+
			"consulted before reservation", attempts, handled.Load())
	}

	// Open: it must take the job promptly.
	open.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && handled.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if handled.Load() == 0 {
		t.Fatal("the worker never took the job after the gate opened; a gate that cannot reopen is " +
			"a stop, not a gate")
	}
	cancel()
	<-done
}

// TestWorkerCanReserveNilIsUnchangedBehavior is the additive guarantee.
//
// Every worker built before this option existed passes nil by construction, so
// nil has to mean exactly what no-option meant. If this ever diverges, the
// change stopped being additive and every existing caller's behaviour moved.
func TestWorkerCanReserveNilIsUnchangedBehavior(t *testing.T) {
	ctx := context.Background()
	q := memory.New()
	if err := q.Push(ctx, "plain", []byte(`{}`)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	var handled atomic.Int64
	w := queue.NewWorker(q, queue.WorkerOpts{
		StopWhenEmpty: true,
		PollInterval:  5 * time.Millisecond,
	})
	w.Register("plain", func(context.Context, *queue.QueuedJob) error {
		handled.Add(1)
		return nil
	})
	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if handled.Load() != 1 {
		t.Fatalf("a worker with no CanReserve handled %d jobs, want 1", handled.Load())
	}
}

// TestWorkerCanReserveDoesNotAbandonWorkInFlight is the property the gate
// exists to provide, and the reason it is a gate rather than a context cancel.
//
// The gate closes while a handler is running. That handler must finish, and its
// completion must be recorded — the job deleted, not left reserved with an
// attempt spent. Cancelling the worker's context instead would abort the
// handler mid-call and then fail the Delete that follows it, because pollLoop
// hands the same context to both.
func TestWorkerCanReserveDoesNotAbandonWorkInFlight(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:canreserveinflight?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	q, err := qsqlite.New(db, qsqlite.Opts{})
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}
	if err := q.Push(ctx, "slow", []byte(`{}`)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	open := atomic.Bool{}
	open.Store(true)
	started := make(chan struct{})
	finished := atomic.Bool{}

	w := queue.NewWorker(q, queue.WorkerOpts{
		PollInterval: 5 * time.Millisecond,
		CanReserve:   func(context.Context) bool { return open.Load() },
	})
	w.Register("slow", func(hctx context.Context, _ *queue.QueuedJob) error {
		close(started)
		// The gate closes underneath this handler.
		time.Sleep(120 * time.Millisecond)
		if hctx.Err() != nil {
			t.Errorf("the handler's context was cancelled mid-job: %v. A closing gate must not "+
				"abandon work already reserved", hctx.Err())
		}
		finished.Store(true)
		return nil
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Start(runCtx) }()

	<-started
	open.Store(false)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !finished.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	if !finished.Load() {
		t.Fatal("the in-flight handler never completed after the gate closed")
	}

	// And the completion was recorded: the job is gone rather than left
	// reserved for the driver to reclaim later with an attempt spent.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE type = 'slow'`).Scan(&n); err == nil && n == 0 {
			cancel()
			<-done
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the completed job was still on the queue; its Delete did not run, so the attempt was " +
		"spent and the job will be reclaimed and re-run")
}
