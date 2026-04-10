package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	queue "github.com/hollis-labs/go-queue"
)

type entry struct {
	id          int64
	jobType     string
	queueName   string
	payload     []byte
	maxTries    int
	attempts    int
	createdAt   time.Time
	availableAt time.Time
	reservedAt  *time.Time
}

type failedEntry struct {
	job    queue.QueuedJob
	errMsg string
}

// Driver is a mutex-protected, slice-backed in-memory Queue implementation.
type Driver struct {
	mu     sync.Mutex
	jobs   []*entry
	failed []*failedEntry
	nextID int64
}

// New returns a new in-memory Driver.
func New() *Driver {
	return &Driver{}
}

// Push enqueues a new job.
func (d *Driver) Push(ctx context.Context, jobType string, payload []byte, opts ...queue.PushOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cfg := queue.ResolvePushConfig(opts)

	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextID++
	now := time.Now()
	e := &entry{
		id:          d.nextID,
		jobType:     jobType,
		queueName:   cfg.Queue,
		payload:     payload,
		maxTries:    cfg.MaxTries,
		createdAt:   now,
		availableAt: now.Add(cfg.Delay),
	}
	d.jobs = append(d.jobs, e)
	return nil
}

// Pop retrieves and reserves the next available job from the named queue.
// Returns nil, nil when no job is available.
func (d *Driver) Pop(ctx context.Context, queueName string) (*queue.QueuedJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	for _, e := range d.jobs {
		if e.queueName != queueName {
			continue
		}
		if e.reservedAt != nil {
			continue
		}
		if e.availableAt.After(now) {
			continue
		}
		// Reserve it.
		t := now
		e.reservedAt = &t
		e.attempts++
		return &queue.QueuedJob{
			ID:          fmt.Sprintf("%d", e.id),
			Type:        e.jobType,
			Queue:       e.queueName,
			Payload:     e.payload,
			Attempts:    e.attempts,
			MaxTries:    e.maxTries,
			CreatedAt:   e.createdAt,
			AvailableAt: e.availableAt,
			ReservedAt:  e.reservedAt,
		}, nil
	}
	return nil, nil
}

// Delete removes a completed job from the queue (idempotent).
func (d *Driver) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.removeByID(id)
	return nil
}

// removeByID removes the entry with the given string ID. Must be called with mu held.
func (d *Driver) removeByID(id string) {
	for i, e := range d.jobs {
		if fmt.Sprintf("%d", e.id) == id {
			d.jobs = append(d.jobs[:i], d.jobs[i+1:]...)
			return
		}
	}
}

// findByID returns the entry and its index. Must be called with mu held.
func (d *Driver) findByID(id string) (*entry, int) {
	for i, e := range d.jobs {
		if fmt.Sprintf("%d", e.id) == id {
			return e, i
		}
	}
	return nil, -1
}

// Release puts a job back on the queue with an updated availableAt and a new ID
// (preserving FIFO ordering). Attempts count is preserved.
func (d *Driver) Release(ctx context.Context, id string, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	e, idx := d.findByID(id)
	if e == nil {
		return nil
	}

	// Capture state before removal.
	jobType := e.jobType
	queueName := e.queueName
	payload := e.payload
	maxTries := e.maxTries
	attempts := e.attempts
	createdAt := e.createdAt

	// Remove old entry.
	d.jobs = append(d.jobs[:idx], d.jobs[idx+1:]...)

	// Re-insert with new ID at end of slice (FIFO).
	d.nextID++
	now := time.Now()
	reinserted := &entry{
		id:          d.nextID,
		jobType:     jobType,
		queueName:   queueName,
		payload:     payload,
		maxTries:    maxTries,
		attempts:    attempts,
		createdAt:   createdAt,
		availableAt: now.Add(delay),
		reservedAt:  nil,
	}
	d.jobs = append(d.jobs, reinserted)
	return nil
}

// Size returns the total number of jobs on the named queue (includes reserved and delayed).
func (d *Driver) Size(ctx context.Context, queueName string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	count := 0
	for _, e := range d.jobs {
		if e.queueName == queueName {
			count++
		}
	}
	return count, nil
}

// Failed moves a job to the failed store and removes it from active jobs.
func (d *Driver) Failed(ctx context.Context, job *queue.QueuedJob, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.removeByID(job.ID)
	d.failed = append(d.failed, &failedEntry{job: *job, errMsg: errMsg})
	return nil
}

// FailedJobs returns a copy of the failed jobs slice for test inspection.
func (d *Driver) FailedJobs() []queue.QueuedJob {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make([]queue.QueuedJob, len(d.failed))
	for i, f := range d.failed {
		out[i] = f.job
	}
	return out
}
