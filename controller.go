package queue

import (
	"context"
	"errors"
	"sync"
)

// Controller errors are comparable with errors.Is. Driver errors remain in
// CycleReport rather than being folded into these fixed messages.
var (
	ErrStalePause         = errors.New("queue: pause handle is not current")
	ErrUnknownDisposition = errors.New("queue: a cycle has an unknown disposition")
	ErrControllerInUse    = errors.New("queue: controller already belongs to another worker or active start")
)

// CycleOperation identifies the last operation in a polling cycle.
type CycleOperation string

const (
	OperationEligibility CycleOperation = "eligibility"
	OperationPop         CycleOperation = "pop"
	OperationSize        CycleOperation = "size"
	OperationDelete      CycleOperation = "delete"
	OperationRelease     CycleOperation = "release"
	OperationFailed      CycleOperation = "failed"
)

// CycleDisposition describes what is known about a completed cycle.
type CycleDisposition string

const (
	DispositionNoReservation CycleDisposition = "no_reservation"
	DispositionSettled       CycleDisposition = "settled"
	DispositionUnknown       CycleDisposition = "unknown"
)

// CycleResult carries internal evidence, not a safe-to-publish diagnostic.
// Err may contain driver details; JobID is opaque and Payload is never retained.
// Cycle is a diagnostic sequence, not an authorization token.
type CycleResult struct {
	Cycle       uint64
	Operation   CycleOperation
	Disposition CycleDisposition
	JobID       string
	Err         error
}

// CycleReport is a snapshot. Only a successful WaitQuiescent with a still-held
// pause proves quiescence. Last is the most recently completed cycle; Unknown
// retains every uncertain cycle, bounded by the worker's polling concurrency.
type CycleReport struct {
	Active  int
	Last    *CycleResult
	Unknown []CycleResult
}

// PauseHandle belongs to one controller and one uninterrupted pause. Copies
// refer to the same pause; a handle cannot resume a later pause.
type PauseHandle struct {
	controller *CycleController
	epoch      *pauseEpoch
}

// Nonzero size ensures distinct live epoch pointers compare unequal.
type pauseEpoch struct{ marker byte }

// CycleController gates entire polling cycles without canceling their contexts.
// Its zero value is ready to use. It belongs permanently to one Worker and must
// not be copied after use. Driver errors permanently quarantine new cycles for
// this controller, including across Starts. Resume never clears uncertainty.
// There is no reset API.
// Callbacks are part of a cycle and must not wait for their own quiescence.
type CycleController struct {
	mu      sync.Mutex
	changed chan struct{}
	owner   *Worker
	running bool
	paused  *pauseEpoch
	next    uint64
	active  int
	last    *CycleResult
	unknown []CycleResult
}

// Pause closes cycle admission. Repeated calls during the same pause return the
// same handle. It may be called before Worker.Start.
func (c *CycleController) Pause() PauseHandle {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.paused == nil {
		c.paused = &pauseEpoch{}
		c.notifyLocked()
	}
	return PauseHandle{controller: c, epoch: c.paused}
}

// Resume reopens this pause only. Uncertainty still quarantines the worker.
func (c *CycleController) Resume(h PauseHandle) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.validLocked(h) {
		return ErrStalePause
	}
	c.paused = nil
	c.notifyLocked()
	return nil
}

// WaitQuiescent waits through eligibility, reservation, handlers, settlement
// and callbacks. A deadline stops only this wait. The caller must keep the
// pause held while relying on success. Unknown outcomes are reported only
// after active cycles finish; they never become a successful idle report.
func (c *CycleController) WaitQuiescent(ctx context.Context, h PauseHandle) (CycleReport, error) {
	for {
		c.mu.Lock()
		r := c.reportLocked()
		if !c.validLocked(h) {
			c.mu.Unlock()
			return r, ErrStalePause
		}
		if c.active == 0 {
			c.mu.Unlock()
			if len(r.Unknown) != 0 {
				return r, ErrUnknownDisposition
			}
			return r, nil
		}
		changed := c.changedLocked()
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return c.Report(), ctx.Err()
		case <-changed:
		}
	}
}

// Report returns detached result/slice values. It does not close admission.
func (c *CycleController) Report() CycleReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reportLocked()
}

func (c *CycleController) reportLocked() CycleReport {
	r := CycleReport{Active: c.active, Unknown: append([]CycleResult(nil), c.unknown...)}
	if c.last != nil {
		last := *c.last
		r.Last = &last
	}
	return r
}

func (c *CycleController) validLocked(h PauseHandle) bool {
	return h.controller == c && h.epoch != nil && h.epoch == c.paused
}

func (c *CycleController) changedLocked() <-chan struct{} {
	if c.changed == nil {
		c.changed = make(chan struct{})
	}
	return c.changed
}

func (c *CycleController) notifyLocked() {
	if c.changed != nil {
		close(c.changed)
	}
	c.changed = make(chan struct{})
}

func (c *CycleController) start(w *Worker) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if (c.owner != nil && c.owner != w) || c.running {
		return ErrControllerInUse
	}
	c.owner, c.running = w, true
	return nil
}

func (c *CycleController) stopped() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.running = false
	c.notifyLocked()
}

func (c *CycleController) begin(ctx context.Context) (uint64, bool) {
	for {
		c.mu.Lock()
		if ctx.Err() != nil {
			c.mu.Unlock()
			return 0, false
		}
		if c.paused == nil && len(c.unknown) == 0 {
			c.active++
			c.next++
			id := c.next
			c.mu.Unlock()
			return id, true
		}
		changed := c.changedLocked()
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, false
		case <-changed:
		}
	}
}

// fault closes admission before user callbacks can block. The affected cycle
// stays active until its callbacks return. Each cycle can fault only once.
func (c *CycleController) fault(r CycleResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unknown = append(c.unknown, r)
	c.notifyLocked()
}

func (c *CycleController) finish(r CycleResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	c.last = &r
	c.notifyLocked()
}
