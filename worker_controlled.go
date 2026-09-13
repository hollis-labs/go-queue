package queue

import (
	"context"
	"sync"
	"time"
)

func (w *Worker) startControlled(ctx context.Context) error {
	c := w.opts.Controller
	if err := c.start(w); err != nil {
		return err
	}
	defer c.stopped()
	var wg sync.WaitGroup
	for i := 0; i < w.opts.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				id, ok := c.begin(ctx)
				if !ok {
					return
				}
				stop, idle := w.controlledCycle(ctx, id)
				if stop {
					return
				}
				if idle {
					timer := time.NewTimer(w.opts.PollInterval)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

func (w *Worker) controlledCycle(ctx context.Context, id uint64) (stop, idle bool) {
	c := w.opts.Controller
	r := CycleResult{Cycle: id, Operation: OperationEligibility, Disposition: DispositionNoReservation}
	defer func() { c.finish(r) }()
	fault := func(op CycleOperation, err error) {
		r.Operation, r.Disposition, r.Err = op, DispositionUnknown, err
		c.fault(r)
		if w.opts.OnError != nil {
			w.opts.OnError(err)
		}
	}
	if w.opts.CanReserve != nil && !w.opts.CanReserve(ctx) {
		return false, true
	}
	for _, name := range w.opts.Queues {
		r.Operation = OperationPop
		job, err := w.queue.Pop(ctx, name)
		if job != nil {
			r.JobID = job.ID
		}
		if err != nil {
			// Even (nil, err) may follow an attempted reservation commit. Stop
			// this cycle immediately; do not try another queue or settlement.
			fault(OperationPop, err)
			return false, false
		}
		if job != nil {
			w.processJob(ctx, job, func(op CycleOperation, err error) {
				r.Operation, r.Disposition = op, DispositionSettled
				if err != nil {
					fault(op, err)
				}
			})
			return false, false
		}
	}
	if w.opts.StopWhenEmpty {
		r.Operation = OperationSize
		for _, name := range w.opts.Queues {
			n, err := w.queue.Size(ctx, name)
			if err != nil {
				// Conservatively quarantine every driver error in managed mode.
				fault(OperationSize, err)
				return false, false
			}
			if n > 0 {
				return false, true
			}
		}
		return true, false
	}
	return false, true
}
