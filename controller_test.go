package queue_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	queue "github.com/hollis-labs/go-queue"
	"github.com/hollis-labs/go-queue/driver/memory"
	"github.com/hollis-labs/go-queue/driver/noop"
	qsqlite "github.com/hollis-labs/go-queue/driver/sqlite"
)

func controllerDriver(t *testing.T, backend string) queue.Queue {
	t.Helper()
	if backend == "memory" {
		return memory.New()
	}
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	q, err := qsqlite.New(db, qsqlite.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

type cycleBarrier struct {
	entered, release chan struct{}
	once             sync.Once
}

func barrier() *cycleBarrier {
	return &cycleBarrier{entered: make(chan struct{}), release: make(chan struct{})}
}
func (b *cycleBarrier) hold(ctx context.Context) {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
	case <-ctx.Done():
	}
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("barrier was not reached")
	}
}

type controlledRun struct {
	done chan struct{}
	err  error
}

func runControlled(t *testing.T, w *queue.Worker) *controlledRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &controlledRun{done: make(chan struct{})}
	go func() { r.err = w.Start(ctx); close(r.done) }()
	t.Cleanup(func() {
		cancel()
		await(t, r.done)
		if r.err != nil {
			t.Errorf("Start: %v", r.err)
		}
	})
	return r
}
func waitPaused(t *testing.T, c *queue.CycleController, h queue.PauseHandle) (queue.CycleReport, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return c.WaitQuiescent(ctx, h)
}

type cycleQueue struct {
	queue.Queue
	pop     func(context.Context, string) (*queue.QueuedJob, error)
	del     func(context.Context, string) error
	release func(context.Context, string, time.Duration) error
	failed  func(context.Context, *queue.QueuedJob, string) error
	size    func(context.Context, string) (int, error)
}

func (q *cycleQueue) Pop(ctx context.Context, name string) (*queue.QueuedJob, error) {
	if q.pop != nil {
		return q.pop(ctx, name)
	}
	return q.Queue.Pop(ctx, name)
}
func (q *cycleQueue) Delete(ctx context.Context, id string) error {
	if q.del != nil {
		return q.del(ctx, id)
	}
	return q.Queue.Delete(ctx, id)
}
func (q *cycleQueue) Release(ctx context.Context, id string, d time.Duration) error {
	if q.release != nil {
		return q.release(ctx, id, d)
	}
	return q.Queue.Release(ctx, id, d)
}
func (q *cycleQueue) Failed(ctx context.Context, job *queue.QueuedJob, message string) error {
	if q.failed != nil {
		return q.failed(ctx, job, message)
	}
	return q.Queue.Failed(ctx, job, message)
}
func (q *cycleQueue) Size(ctx context.Context, name string) (int, error) {
	if q.size != nil {
		return q.size(ctx, name)
	}
	return q.Queue.Size(ctx, name)
}

func TestControllerHoldsEveryCycleBoundary(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, stage := range []string{"eligibility", "pop", "handler", "delete", "callback"} {
			t.Run(backend+"/"+stage, func(t *testing.T) {
				base := controllerDriver(t, backend)
				if err := base.Push(context.Background(), "job", nil); err != nil {
					t.Fatal(err)
				}
				b := barrier()
				q := &cycleQueue{Queue: base}
				q.pop = func(ctx context.Context, name string) (*queue.QueuedJob, error) {
					if stage == "pop" {
						b.hold(ctx)
					}
					return base.Pop(ctx, name)
				}
				q.del = func(ctx context.Context, id string) error {
					if stage == "delete" {
						b.hold(ctx)
					}
					if ctx.Err() != nil {
						t.Errorf("pause canceled settlement: %v", ctx.Err())
					}
					return base.Delete(ctx, id)
				}
				var c queue.CycleController
				var handlerCtx context.Context
				w := queue.NewWorker(q, queue.WorkerOpts{Controller: &c, StopWhenEmpty: true,
					CanReserve: func(ctx context.Context) bool {
						if stage == "eligibility" {
							b.hold(ctx)
						}
						return true
					},
					OnProcessed: func(*queue.QueuedJob) {
						if stage == "callback" {
							b.hold(handlerCtx)
						}
					},
				})
				w.Register("job", func(ctx context.Context, _ *queue.QueuedJob) error {
					handlerCtx = ctx
					if stage == "handler" {
						b.hold(ctx)
					}
					if ctx.Err() != nil {
						t.Errorf("pause canceled handler: %v", ctx.Err())
					}
					return nil
				})
				run := runControlled(t, w)
				await(t, b.entered)
				h := c.Pause()
				if c.Pause() != h {
					t.Fatal("pause is not idempotent")
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				r, err := c.WaitQuiescent(ctx, h)
				if !errors.Is(err, context.Canceled) || r.Active != 1 {
					t.Fatalf("early drain: %+v, %v", r, err)
				}
				close(b.release)
				r, err = waitPaused(t, &c, h)
				if err != nil || r.Active != 0 || r.Last.Operation != queue.OperationDelete || r.Last.Disposition != queue.DispositionSettled {
					t.Fatalf("drain: %+v, %v", r, err)
				}
				n, err := base.Size(context.Background(), "default")
				if err != nil || n != 0 {
					t.Fatalf("job not durably deleted: %d, %v", n, err)
				}
				if err := c.Resume(h); err != nil {
					t.Fatal(err)
				}
				await(t, run.done)
			})
		}
	}
}

func TestControllerFaultsRemainUnknownAfterResume(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, op := range []queue.CycleOperation{queue.OperationPop, queue.OperationDelete, queue.OperationRelease, queue.OperationFailed, queue.OperationSize} {
			t.Run(backend+"/"+string(op), func(t *testing.T) {
				base := controllerDriver(t, backend)
				if op != queue.OperationSize {
					if err := base.Push(context.Background(), "job", nil); err != nil {
						t.Fatal(err)
					}
				}
				uncertain := errors.New("fixture: effect happened but acknowledgement failed")
				q := &cycleQueue{Queue: base}
				var pops, processed, failed atomic.Int32
				q.pop = func(ctx context.Context, name string) (*queue.QueuedJob, error) {
					pops.Add(1)
					job, err := base.Pop(ctx, name)
					if op == queue.OperationPop && err == nil {
						return nil, uncertain
					}
					return job, err
				}
				q.del = func(ctx context.Context, id string) error {
					err := base.Delete(ctx, id)
					if op == queue.OperationDelete && err == nil {
						return uncertain
					}
					return err
				}
				q.release = func(ctx context.Context, id string, d time.Duration) error {
					err := base.Release(ctx, id, d)
					if op == queue.OperationRelease && err == nil {
						return uncertain
					}
					return err
				}
				q.failed = func(ctx context.Context, j *queue.QueuedJob, msg string) error {
					err := base.Failed(ctx, j, msg)
					if op == queue.OperationFailed && err == nil {
						return uncertain
					}
					return err
				}
				q.size = func(ctx context.Context, name string) (int, error) {
					if op == queue.OperationSize {
						return 0, uncertain
					}
					return base.Size(ctx, name)
				}
				var c queue.CycleController
				onError := barrier()
				var callbackCtx context.Context
				maxTries := 1
				if op == queue.OperationRelease {
					maxTries = 3
				}
				w := queue.NewWorker(q, queue.WorkerOpts{Controller: &c, StopWhenEmpty: true, MaxTries: maxTries,
					CanReserve: func(ctx context.Context) bool { callbackCtx = ctx; return true },
					OnError: func(err error) {
						if !errors.Is(err, uncertain) {
							t.Errorf("error lost: %v", err)
						}
						onError.hold(callbackCtx)
					},
					OnProcessed: func(*queue.QueuedJob) { processed.Add(1) },
					OnFailed:    func(*queue.QueuedJob, error) { failed.Add(1) },
				})
				w.Register("job", func(context.Context, *queue.QueuedJob) error {
					if op == queue.OperationFailed || op == queue.OperationRelease {
						return errors.New("handler fixture failure")
					}
					return nil
				})
				runControlled(t, w)
				await(t, onError.entered)
				h := c.Pause()
				r := c.Report()
				if r.Active != 1 || len(r.Unknown) != 1 || r.Unknown[0].Operation != op || !errors.Is(r.Unknown[0].Err, uncertain) {
					t.Fatalf("fault not latched before callback: %+v", r)
				}
				r.Unknown[0].Operation = "mutated"
				close(onError.release)
				r, err := waitPaused(t, &c, h)
				if !errors.Is(err, queue.ErrUnknownDisposition) || r.Active != 0 || r.Unknown[0].Operation != op {
					t.Fatalf("false settled report: %+v, %v", r, err)
				}
				if processed.Load() != 0 || failed.Load() != 0 {
					t.Fatal("false successful settlement callback")
				}
				if err := c.Resume(h); err != nil {
					t.Fatal(err)
				}
				h2 := c.Pause()
				if err := c.Resume(h); !errors.Is(err, queue.ErrStalePause) {
					t.Fatalf("stale handle: %v", err)
				}
				r, err = waitPaused(t, &c, h2)
				if !errors.Is(err, queue.ErrUnknownDisposition) || len(r.Unknown) != 1 || pops.Load() != 1 {
					t.Fatalf("fault cleared/replayed: %+v %v pops=%d", r, err, pops.Load())
				}
			})
		}
	}
}

func TestControllerBindsOneWorkerAndStart(t *testing.T) {
	var c, other queue.CycleController
	h := c.Pause()
	if _, err := waitPaused(t, &other, h); !errors.Is(err, queue.ErrStalePause) {
		t.Fatal(err)
	}
	if err := c.Resume(queue.PauseHandle{}); !errors.Is(err, queue.ErrStalePause) {
		t.Fatal(err)
	}
	if _, err := waitPaused(t, &c, h); err != nil {
		t.Fatal(err)
	}
	b := barrier()
	w := queue.NewWorker(noop.New(), queue.WorkerOpts{Controller: &c, StopWhenEmpty: true, CanReserve: func(ctx context.Context) bool { b.hold(ctx); return true }})
	run := runControlled(t, w)
	if err := c.Resume(h); err != nil {
		t.Fatal(err)
	}
	await(t, b.entered)
	if err := w.Start(context.Background()); !errors.Is(err, queue.ErrControllerInUse) {
		t.Fatalf("overlapping Start: %v", err)
	}
	w2 := queue.NewWorker(noop.New(), queue.WorkerOpts{Controller: &c})
	if err := w2.Start(context.Background()); !errors.Is(err, queue.ErrControllerInUse) {
		t.Fatalf("other worker: %v", err)
	}
	close(b.release)
	await(t, run.done)
	if err := w2.Start(context.Background()); !errors.Is(err, queue.ErrControllerInUse) {
		t.Fatalf("controller transferred after stop: %v", err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("same worker restart: %v", err)
	}
}

func TestControllerConcurrentCyclesAndBoundedFaults(t *testing.T) {
	const n = 4
	entered := make(chan struct{}, n)
	release := make(chan struct{})
	failed := make(chan struct{}, n)
	q := &cycleQueue{Queue: noop.New(), pop: func(context.Context, string) (*queue.QueuedJob, error) { return nil, errors.New("unknown reservation") }}
	var c queue.CycleController
	w := queue.NewWorker(q, queue.WorkerOpts{Controller: &c, Concurrency: n,
		CanReserve: func(ctx context.Context) bool {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return true
		},
		OnError: func(error) { failed <- struct{}{} },
	})
	runControlled(t, w)
	for i := 0; i < n; i++ {
		await(t, entered)
	}
	h := c.Pause()
	if got := c.Report().Active; got != n {
		t.Fatalf("active=%d", got)
	}
	close(release)
	for i := 0; i < n; i++ {
		await(t, failed)
	}
	r, err := waitPaused(t, &c, h)
	if !errors.Is(err, queue.ErrUnknownDisposition) || r.Active != 0 || len(r.Unknown) != n {
		t.Fatalf("concurrent drain: %+v %v", r, err)
	}
	ids := map[uint64]bool{}
	for _, f := range r.Unknown {
		if ids[f.Cycle] {
			t.Fatal("duplicate cycle")
		}
		ids[f.Cycle] = true
	}
}

func TestControllerPreservesReleaseOrderAndAttempts(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			q := controllerDriver(t, backend)
			for _, name := range []string{"a", "b"} {
				if err := q.Push(context.Background(), name, nil, queue.WithMaxTries(2)); err != nil {
					t.Fatal(err)
				}
			}
			var c queue.CycleController
			w := queue.NewWorker(q, queue.WorkerOpts{Controller: &c, MaxTries: 10, StopWhenEmpty: true})
			var order []string
			var attempts []int
			for _, name := range []string{"a", "b"} {
				w.Register(name, func(_ context.Context, j *queue.QueuedJob) error {
					order = append(order, j.Type)
					attempts = append(attempts, j.Attempts)
					return errors.New("retry fixture")
				})
			}
			if err := w.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(order, []string{"a", "b", "a", "b"}) || !reflect.DeepEqual(attempts, []int{1, 1, 2, 2}) {
				t.Fatalf("order=%v attempts=%v", order, attempts)
			}
			if _, err := waitPaused(t, &c, c.Pause()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestControllerPausedEligibilityDoesNotInspectEmptyQueue(t *testing.T) {
	var c queue.CycleController
	var allow atomic.Bool
	var pops atomic.Int32
	b := barrier()
	q := &cycleQueue{Queue: noop.New(), pop: func(context.Context, string) (*queue.QueuedJob, error) { pops.Add(1); return nil, nil }}
	w := queue.NewWorker(q, queue.WorkerOpts{Controller: &c, StopWhenEmpty: true, PollInterval: time.Millisecond,
		CanReserve: func(ctx context.Context) bool { b.hold(ctx); return allow.Load() },
	})
	run := runControlled(t, w)
	await(t, b.entered)
	h := c.Pause()
	close(b.release)
	r, err := waitPaused(t, &c, h)
	if err != nil || r.Last.Operation != queue.OperationEligibility || r.Last.Disposition != queue.DispositionNoReservation || pops.Load() != 0 {
		t.Fatalf("gate bypass: %+v %v pops=%d", r, err, pops.Load())
	}
	allow.Store(true)
	if err := c.Resume(h); err != nil {
		t.Fatal(err)
	}
	await(t, run.done)
	if pops.Load() != 1 {
		t.Fatalf("resume never reserved/checked queue: %d", pops.Load())
	}
}

func TestControllerMissingHandlerSettlesFailure(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			q := controllerDriver(t, backend)
			if err := q.Push(context.Background(), "unregistered", nil); err != nil {
				t.Fatal(err)
			}
			var c queue.CycleController
			var pause queue.PauseHandle
			// The callback publishes the handle before finish; use a channel to
			// synchronize publication, then WaitQuiescent joins that callback.
			ready := make(chan struct{})
			w := queue.NewWorker(q, queue.WorkerOpts{Controller: &c, OnError: func(err error) {
				if !errors.Is(err, queue.ErrHandlerNotFound) {
					t.Errorf("unexpected error: %v", err)
				}
				pause = c.Pause()
				close(ready)
			}})
			runControlled(t, w)
			await(t, ready)
			r, err := waitPaused(t, &c, pause)
			if err != nil || r.Last.Operation != queue.OperationFailed || r.Last.Disposition != queue.DispositionSettled {
				t.Fatalf("missing handler result: %+v %v", r, err)
			}
			if n, err := q.Size(context.Background(), "default"); err != nil || n != 0 {
				t.Fatalf("failed job still active: %d %v", n, err)
			}
		})
	}
}

func TestNilControllerPreservesSettlementErrorCallbacks(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, op := range []queue.CycleOperation{queue.OperationDelete, queue.OperationFailed} {
			t.Run(backend+"/"+string(op), func(t *testing.T) {
				base := controllerDriver(t, backend)
				if err := base.Push(context.Background(), "job", nil); err != nil {
					t.Fatal(err)
				}
				uncertain := errors.New("fixture: effect happened but acknowledgement failed")
				q := &cycleQueue{Queue: base}
				q.del = func(ctx context.Context, id string) error {
					if err := base.Delete(ctx, id); err != nil {
						return err
					}
					return uncertain
				}
				q.failed = func(ctx context.Context, job *queue.QueuedJob, message string) error {
					if err := base.Failed(ctx, job, message); err != nil {
						return err
					}
					return uncertain
				}
				var processed, failed, reported int
				w := queue.NewWorker(q, queue.WorkerOpts{StopWhenEmpty: true, MaxTries: 1,
					OnProcessed: func(*queue.QueuedJob) { processed++ },
					OnFailed:    func(*queue.QueuedJob, error) { failed++ },
					OnError:     func(error) { reported++ },
				})
				w.Register("job", func(context.Context, *queue.QueuedJob) error {
					if op == queue.OperationFailed {
						return errors.New("handler fixture failure")
					}
					return nil
				})
				run := runControlled(t, w)
				await(t, run.done)
				if reported != 0 || (op == queue.OperationDelete && (processed != 1 || failed != 0)) || (op == queue.OperationFailed && (processed != 0 || failed != 1)) {
					t.Fatalf("legacy callbacks changed: processed=%d failed=%d errors=%d", processed, failed, reported)
				}
			})
		}
	}
}
