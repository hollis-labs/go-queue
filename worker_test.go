package queue_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	queue "github.com/hollis-labs/go-queue"
	"github.com/hollis-labs/go-queue/driver/memory"
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
