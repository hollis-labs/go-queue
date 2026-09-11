package queue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// WorkerOpts configures worker behavior.
type WorkerOpts struct {
	// Queues to poll in priority order. Default: ["default"].
	Queues []string

	// Concurrency is the number of polling goroutines. Default: 1.
	Concurrency int

	// PollInterval is the sleep duration between empty polls. Default: 3s.
	PollInterval time.Duration

	// MaxTries is the default max attempts per job. Jobs with
	// WithMaxTries override this. 0 = unlimited. Default: 3.
	MaxTries int

	// RetryAfter is the delay before a failed job is retried.
	// 0 means no delay (immediate retry). Default: 0.
	RetryAfter time.Duration

	// MaxMemoryMB stops the worker if memory exceeds this. 0 = no limit.
	MaxMemoryMB int

	// StopWhenEmpty exits after the queue is drained. Default: false.
	StopWhenEmpty bool

	// CanReserve, when non-nil, is asked once per poll cycle whether this
	// worker may take work right now. Returning false skips the cycle: nothing
	// is reserved, the worker sleeps PollInterval, and it asks again.
	//
	// It gates ONLY new reservations. A job already held runs to completion
	// under a live context, which is the difference between "stop taking work"
	// and "abandon work in progress" — and the reason this is a gate rather
	// than something a caller can approximate by cancelling the worker's
	// context. Cancelling aborts the handler mid-flight and then fails the
	// bookkeeping that follows it, leaving the job reserved and its attempt
	// spent.
	//
	// Written for deference between processes that share a queue: a leader that
	// owns the work, a maintenance window, or a drain-down before shutdown. The
	// canonical case is a long-lived service and short-lived helpers on one
	// database, where only the service should run jobs while it is alive.
	//
	// nil means always eligible, which is exactly how every worker behaved
	// before this option existed.
	//
	// Called on the polling goroutine, so it should be cheap and must not
	// block for long — a slow check delays every cycle. It must not panic:
	// this package does not recover panics anywhere, here or in handlers, and
	// one raised here would take down the polling goroutine.
	//
	// Interaction with StopWhenEmpty, stated because it is a real corner: a
	// gated cycle never inspects the queue, so it cannot conclude the queue is
	// drained and will not trigger StopWhenEmpty. A worker told it may not
	// reserve is deferring, not finished, and it waits rather than exiting.
	CanReserve func(ctx context.Context) bool

	// Lifecycle callbacks.
	OnProcessing func(job *QueuedJob)
	OnProcessed  func(job *QueuedJob)
	OnFailed     func(job *QueuedJob, err error)
	OnError      func(err error)
}

// Worker polls a Queue and dispatches jobs to registered handlers.
type Worker struct {
	queue    Queue
	opts     WorkerOpts
	handlers map[string]Handler
	mu       sync.RWMutex
}

// NewWorker creates a Worker bound to the given Queue.
func NewWorker(q Queue, opts WorkerOpts) *Worker {
	if len(opts.Queues) == 0 {
		opts.Queues = []string{"default"}
	}
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = 3 * time.Second
	}
	if opts.MaxTries == 0 {
		opts.MaxTries = 3
	}
	// Note: RetryAfter=0 means no delay (immediate retry). There is no
	// enforced default so callers can opt into zero-delay retry in tests.
	return &Worker{
		queue:    q,
		opts:     opts,
		handlers: make(map[string]Handler),
	}
}

// Register adds a handler for the given job type.
func (w *Worker) Register(jobType string, h Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[jobType] = h
}

// Start begins polling. Blocks until ctx is cancelled or StopWhenEmpty
// triggers. Returns nil on clean shutdown.
func (w *Worker) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < w.opts.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.pollLoop(ctx)
		}()
	}
	wg.Wait()
	return nil
}

func (w *Worker) pollLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		// Ask before reserving. Checked here rather than after popNextJob so a
		// gated worker never holds a job it is not allowed to run.
		if w.opts.CanReserve != nil && !w.opts.CanReserve(ctx) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.opts.PollInterval):
				continue
			}
		}

		job := w.popNextJob(ctx)
		if job == nil {
			if w.opts.StopWhenEmpty {
				if w.allQueuesEmpty(ctx) {
					return
				}
				// Jobs may have been released back; short sleep then retry.
				select {
				case <-ctx.Done():
					return
				case <-time.After(w.opts.PollInterval):
					continue
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.opts.PollInterval):
				continue
			}
		}

		w.processJob(ctx, job)
	}
}

func (w *Worker) allQueuesEmpty(ctx context.Context) bool {
	for _, qName := range w.opts.Queues {
		n, err := w.queue.Size(ctx, qName)
		if err != nil || n > 0 {
			return false
		}
	}
	return true
}

func (w *Worker) popNextJob(ctx context.Context) *QueuedJob {
	for _, qName := range w.opts.Queues {
		job, err := w.queue.Pop(ctx, qName)
		if err != nil {
			if w.opts.OnError != nil {
				w.opts.OnError(err)
			}
			continue
		}
		if job != nil {
			return job
		}
	}
	return nil
}

func (w *Worker) processJob(ctx context.Context, job *QueuedJob) {
	w.mu.RLock()
	handler, ok := w.handlers[job.Type]
	w.mu.RUnlock()

	if !ok {
		// No handler registered — permanent failure.
		_ = w.queue.Failed(ctx, job, fmt.Sprintf("%v: %s", ErrHandlerNotFound, job.Type))
		if w.opts.OnError != nil {
			w.opts.OnError(fmt.Errorf("%w: %s", ErrHandlerNotFound, job.Type))
		}
		return
	}

	if w.opts.OnProcessing != nil {
		w.opts.OnProcessing(job)
	}

	err := handler(ctx, job)

	if err == nil {
		_ = w.queue.Delete(ctx, job.ID)
		if w.opts.OnProcessed != nil {
			w.opts.OnProcessed(job)
		}
		return
	}

	// Determine effective maxTries: per-job overrides worker default.
	maxTries := w.opts.MaxTries
	if job.MaxTries > 0 {
		maxTries = job.MaxTries
	}

	if maxTries > 0 && job.Attempts >= maxTries {
		_ = w.queue.Failed(ctx, job, err.Error())
		if w.opts.OnFailed != nil {
			w.opts.OnFailed(job, err)
		}
		return
	}

	// Retry: release with delay.
	_ = w.queue.Release(ctx, job.ID, w.opts.RetryAfter)
}
