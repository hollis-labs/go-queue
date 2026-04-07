package memory_test

import (
	"context"
	"testing"
	"time"

	queue "github.com/hollis-labs/go-queue"
	"github.com/hollis-labs/go-queue/driver/memory"
)

// Compile-time assertion: Driver implements queue.Queue.
var _ queue.Queue = (*memory.Driver)(nil)

func TestPushPopDelete(t *testing.T) {
	ctx := context.Background()
	d := memory.New()

	if err := d.Push(ctx, "send-email", []byte(`{"to":"a@b.com"}`)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	size, err := d.Size(ctx, "default")
	if err != nil || size != 1 {
		t.Fatalf("Size after push: got %d, err %v", size, err)
	}

	job, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if job == nil {
		t.Fatal("Pop returned nil job")
	}
	if job.Type != "send-email" {
		t.Errorf("Type: got %q, want %q", job.Type, "send-email")
	}
	if string(job.Payload) != `{"to":"a@b.com"}` {
		t.Errorf("Payload: got %q", job.Payload)
	}
	if job.Queue != "default" {
		t.Errorf("Queue: got %q, want %q", job.Queue, "default")
	}
	if job.Attempts != 1 {
		t.Errorf("Attempts: got %d, want 1", job.Attempts)
	}

	if err := d.Delete(ctx, job.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	size, err = d.Size(ctx, "default")
	if err != nil || size != 0 {
		t.Fatalf("Size after delete: got %d, err %v", size, err)
	}
}

func TestPopEmptyQueue(t *testing.T) {
	ctx := context.Background()
	d := memory.New()

	job, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if job != nil {
		t.Fatalf("Expected nil job from empty queue, got %+v", job)
	}
}

func TestPopFIFOOrdering(t *testing.T) {
	ctx := context.Background()
	d := memory.New()

	for _, typ := range []string{"first", "second", "third"} {
		if err := d.Push(ctx, typ, nil); err != nil {
			t.Fatalf("Push %s: %v", typ, err)
		}
	}

	for _, want := range []string{"first", "second", "third"} {
		job, err := d.Pop(ctx, "default")
		if err != nil {
			t.Fatalf("Pop error: %v", err)
		}
		if job == nil {
			t.Fatalf("Expected job %q, got nil", want)
		}
		if job.Type != want {
			t.Errorf("FIFO order: got %q, want %q", job.Type, want)
		}
	}
}

func TestNamedQueues(t *testing.T) {
	ctx := context.Background()
	d := memory.New()

	if err := d.Push(ctx, "high-job", nil, queue.OnQueue("high")); err != nil {
		t.Fatalf("Push high: %v", err)
	}
	if err := d.Push(ctx, "low-job", nil, queue.OnQueue("low")); err != nil {
		t.Fatalf("Push low: %v", err)
	}

	high, err := d.Pop(ctx, "high")
	if err != nil || high == nil || high.Type != "high-job" {
		t.Fatalf("Pop high: got %v, err %v", high, err)
	}

	low, err := d.Pop(ctx, "low")
	if err != nil || low == nil || low.Type != "low-job" {
		t.Fatalf("Pop low: got %v, err %v", low, err)
	}

	// Each queue should now be empty.
	nilJob, err := d.Pop(ctx, "high")
	if err != nil || nilJob != nil {
		t.Errorf("Expected empty high queue, got %v, err %v", nilJob, err)
	}
}

func TestDelayedJob(t *testing.T) {
	ctx := context.Background()
	d := memory.New()

	if err := d.Push(ctx, "delayed-job", nil, queue.WithDelay(1*time.Hour)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Pop should return nil — not yet available.
	job, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if job != nil {
		t.Fatalf("Expected nil for delayed job, got %+v", job)
	}

	// Size should still count it.
	size, err := d.Size(ctx, "default")
	if err != nil || size != 1 {
		t.Fatalf("Size for delayed job: got %d, err %v", size, err)
	}
}

func TestMaxTriesStoredOnJob(t *testing.T) {
	ctx := context.Background()
	d := memory.New()

	if err := d.Push(ctx, "retryable", nil, queue.WithMaxTries(5)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	job, err := d.Pop(ctx, "default")
	if err != nil || job == nil {
		t.Fatalf("Pop: %v, err %v", job, err)
	}
	if job.MaxTries != 5 {
		t.Errorf("MaxTries: got %d, want 5", job.MaxTries)
	}
}

func TestRelease(t *testing.T) {
	ctx := context.Background()
	d := memory.New()

	if err := d.Push(ctx, "releasable", []byte("data"), queue.WithMaxTries(3)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	job1, err := d.Pop(ctx, "default")
	if err != nil || job1 == nil {
		t.Fatalf("First Pop: %v, err %v", job1, err)
	}
	firstID := job1.ID

	// Release with no delay.
	if err := d.Release(ctx, firstID, 0); err != nil {
		t.Fatalf("Release: %v", err)
	}

	job2, err := d.Pop(ctx, "default")
	if err != nil || job2 == nil {
		t.Fatalf("Second Pop after release: %v, err %v", job2, err)
	}

	// New ID because it was re-inserted.
	if job2.ID == firstID {
		t.Errorf("Expected new ID after release, got same ID %s", firstID)
	}
	if job2.Type != "releasable" {
		t.Errorf("Type: got %q, want %q", job2.Type, "releasable")
	}
	// Attempts should be 2: 1 from first pop, incremented again on second pop.
	if job2.Attempts != 2 {
		t.Errorf("Attempts: got %d, want 2", job2.Attempts)
	}
}

func TestReleaseFIFOOrdering(t *testing.T) {
	ctx := context.Background()
	d := memory.New()

	if err := d.Push(ctx, "first", nil); err != nil {
		t.Fatalf("Push first: %v", err)
	}
	if err := d.Push(ctx, "second", nil); err != nil {
		t.Fatalf("Push second: %v", err)
	}

	// Pop "first".
	job1, err := d.Pop(ctx, "default")
	if err != nil || job1 == nil || job1.Type != "first" {
		t.Fatalf("Pop first: %v, err %v", job1, err)
	}

	// Release "first" with no delay — it should go to the back.
	if err := d.Release(ctx, job1.ID, 0); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// Next pop should be "second".
	job2, err := d.Pop(ctx, "default")
	if err != nil || job2 == nil {
		t.Fatalf("Pop second: %v, err %v", job2, err)
	}
	if job2.Type != "second" {
		t.Errorf("Expected 'second', got %q (released job should be at back)", job2.Type)
	}
}

func TestFailedJobStorage(t *testing.T) {
	ctx := context.Background()
	d := memory.New()

	if err := d.Push(ctx, "will-fail", []byte("oops")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	job, err := d.Pop(ctx, "default")
	if err != nil || job == nil {
		t.Fatalf("Pop: %v, err %v", job, err)
	}

	if err := d.Failed(ctx, job, "something went wrong"); err != nil {
		t.Fatalf("Failed: %v", err)
	}

	size, err := d.Size(ctx, "default")
	if err != nil || size != 0 {
		t.Fatalf("Size after Failed: got %d, err %v", size, err)
	}

	failed := d.FailedJobs()
	if len(failed) != 1 {
		t.Fatalf("FailedJobs: got %d entries, want 1", len(failed))
	}
	if failed[0].Type != "will-fail" {
		t.Errorf("Failed job Type: got %q, want %q", failed[0].Type, "will-fail")
	}
}
