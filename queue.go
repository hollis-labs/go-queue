package queue

import (
	"context"
	"time"
)

// Queue is the driver interface. Each backend (SQLite, memory, noop)
// implements this contract. Consumers depend on this interface, not
// concrete drivers.
type Queue interface {
	// Push enqueues a new job. The jobType identifies which handler
	// processes it; payload is an opaque JSON blob.
	Push(ctx context.Context, jobType string, payload []byte, opts ...PushOption) error

	// Pop retrieves and reserves the next available job from the named
	// queue. Returns nil, nil when the queue is empty.
	Pop(ctx context.Context, queueName string) (*QueuedJob, error)

	// Delete removes a completed job from the queue.
	Delete(ctx context.Context, id string) error

	// Release puts a job back on the queue with a delay. The job gets
	// a new position (FIFO ordering preserved) but keeps its attempt
	// count.
	Release(ctx context.Context, id string, delay time.Duration) error

	// Size returns the total number of jobs on the named queue.
	Size(ctx context.Context, queueName string) (int, error)

	// Failed moves a job to the failed jobs store with the given error
	// message. The original job is removed from the active queue.
	Failed(ctx context.Context, job *QueuedJob, errMsg string) error
}

// QueuedJob is a job that has been popped from the queue. It carries
// the metadata the worker needs for dispatch, retry, and failure.
type QueuedJob struct {
	ID          string
	Type        string
	Queue       string
	Payload     []byte
	Attempts    int
	MaxTries    int
	CreatedAt   time.Time
	AvailableAt time.Time
	ReservedAt  *time.Time
}

// Handler processes a job payload. Registered with the worker by job
// type string.
type Handler func(ctx context.Context, job *QueuedJob) error

// PushConfig holds resolved push options. Exported so drivers can
// access the resolved values.
type PushConfig struct {
	Queue    string
	Delay    time.Duration
	MaxTries int
}

func defaultPushConfig() PushConfig {
	return PushConfig{
		Queue:    "default",
		Delay:    0,
		MaxTries: 0, // 0 = inherit from WorkerOpts.MaxTries
	}
}

// ResolvePushConfig applies options and returns the resolved config.
func ResolvePushConfig(opts []PushOption) PushConfig {
	cfg := defaultPushConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// PushOption configures optional parameters for Push.
type PushOption func(*PushConfig)

// OnQueue sets the queue name for the job. Default: "default".
func OnQueue(name string) PushOption {
	return func(c *PushConfig) { c.Queue = name }
}

// WithDelay sets a delay before the job becomes available. Default: 0.
func WithDelay(d time.Duration) PushOption {
	return func(c *PushConfig) { c.Delay = d }
}

// WithMaxTries sets the maximum number of attempts for this job.
// 0 means inherit from WorkerOpts.MaxTries.
func WithMaxTries(n int) PushOption {
	return func(c *PushConfig) { c.MaxTries = n }
}
