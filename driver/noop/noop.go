package noop

import (
	"context"
	"time"

	queue "github.com/hollis-labs/go-queue"
)

// Driver is a no-op Queue implementation. Push accepts and drops every
// job. Pop always returns nil. Used when queue functionality is disabled.
type Driver struct{}

// New creates a new noop driver.
func New() *Driver { return &Driver{} }

func (*Driver) Push(_ context.Context, _ string, _ []byte, _ ...queue.PushOption) error {
	return nil
}

func (*Driver) Pop(_ context.Context, _ string) (*queue.QueuedJob, error) {
	return nil, nil
}

func (*Driver) Delete(_ context.Context, _ string) error        { return nil }
func (*Driver) Release(_ context.Context, _ string, _ time.Duration) error { return nil }
func (*Driver) Size(_ context.Context, _ string) (int, error)   { return 0, nil }
func (*Driver) Failed(_ context.Context, _ *queue.QueuedJob, _ string) error { return nil }
