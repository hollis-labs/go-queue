// Package main is a runnable end-to-end example of go-queue using the
// in-memory driver. It pushes a handful of jobs onto two named queues,
// registers a handler, and runs a Worker until the queues are drained.
//
// Run from the repo root:
//
//	go run ./examples/inmemory
//
// Expected output (job IDs and ordering may vary by run):
//
//	processed [high] greet#1: hello-1
//	processed [high] greet#2: hello-2
//	processed [low]  greet#3: hello-3
//	all jobs processed
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	queue "github.com/hollis-labs/go-queue"
	"github.com/hollis-labs/go-queue/driver/memory"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q := memory.New()

	// Two jobs on "high", one on "low". Worker.Queues priority will drain
	// "high" first, then "low".
	for i, payload := range []string{"hello-1", "hello-2"} {
		if err := q.Push(ctx, "greet", []byte(payload), queue.OnQueue("high")); err != nil {
			log.Fatalf("push high #%d: %v", i+1, err)
		}
	}
	if err := q.Push(ctx, "greet", []byte("hello-3"), queue.OnQueue("low")); err != nil {
		log.Fatalf("push low: %v", err)
	}

	w := queue.NewWorker(q, queue.WorkerOpts{
		Queues:        []string{"high", "low"},
		Concurrency:   1,
		PollInterval:  10 * time.Millisecond,
		MaxTries:      3,
		StopWhenEmpty: true, // exit Start once the queues drain
	})

	w.Register("greet", func(_ context.Context, job *queue.QueuedJob) error {
		fmt.Printf("processed [%s] %s#%s: %s\n", job.Queue, job.Type, job.ID, string(job.Payload))
		return nil
	})

	if err := w.Start(ctx); err != nil {
		log.Fatalf("worker: %v", err)
	}
	fmt.Println("all jobs processed")
}
