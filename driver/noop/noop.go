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

func (*Driver) Push(ctx context.Context, _ string, _ []byte, _ ...queue.PushOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (*Driver) Pop(ctx context.Context, _ string) (*queue.QueuedJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (*Driver) Delete(ctx context.Context, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (*Driver) Release(ctx context.Context, _ string, _ time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (*Driver) Size(ctx context.Context, _ string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return 0, nil
}

func (*Driver) Failed(ctx context.Context, _ *queue.QueuedJob, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
