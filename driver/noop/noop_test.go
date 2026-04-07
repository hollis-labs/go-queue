package noop_test

import (
	"context"
	"testing"

	queue "github.com/hollis-labs/go-queue"
	"github.com/hollis-labs/go-queue/driver/noop"
)

var _ queue.Queue = (*noop.Driver)(nil)

func TestNoopPushAndPop(t *testing.T) {
	ctx := context.Background()
	q := noop.New()

	if err := q.Push(ctx, "job", []byte("data")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	job, err := q.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if job != nil {
		t.Fatalf("Pop = %+v, want nil", job)
	}

	size, err := q.Size(ctx, "default")
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != 0 {
		t.Fatalf("Size = %d, want 0", size)
	}
}

func TestNoopDeleteAndRelease(t *testing.T) {
	ctx := context.Background()
	q := noop.New()

	if err := q.Delete(ctx, "nonexistent"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := q.Release(ctx, "nonexistent", 0); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func TestNoopFailed(t *testing.T) {
	ctx := context.Background()
	q := noop.New()

	job := &queue.QueuedJob{ID: "1", Type: "test"}
	if err := q.Failed(ctx, job, "err"); err != nil {
		t.Fatalf("Failed: %v", err)
	}
}
