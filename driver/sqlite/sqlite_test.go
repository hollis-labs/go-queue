package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	queue "github.com/hollis-labs/go-queue"
	qsqlite "github.com/hollis-labs/go-queue/driver/sqlite"
	_ "modernc.org/sqlite"
)

// compile-time interface assertion
var _ queue.Queue = (*qsqlite.Driver)(nil)

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("WAL: %v", err)
	}
	return db
}

func newDriver(t *testing.T, opts qsqlite.Opts) *qsqlite.Driver {
	t.Helper()
	db := newTestDB(t)
	d, err := qsqlite.New(db, opts)
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}
	return d
}

func TestSQLitePushPopDelete(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t, qsqlite.Opts{})

	if err := d.Push(ctx, "email", []byte(`{"to":"a@b.com"}`)); err != nil {
		t.Fatalf("push: %v", err)
	}

	size, err := d.Size(ctx, "default")
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	if size != 1 {
		t.Fatalf("expected size 1, got %d", size)
	}

	job, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if job == nil {
		t.Fatal("expected job, got nil")
	}
	if job.Type != "email" {
		t.Errorf("type: want email, got %s", job.Type)
	}
	if string(job.Payload) != `{"to":"a@b.com"}` {
		t.Errorf("payload mismatch: %s", job.Payload)
	}
	if job.Queue != "default" {
		t.Errorf("queue: want default, got %s", job.Queue)
	}
	if job.Attempts != 1 {
		t.Errorf("attempts: want 1, got %d", job.Attempts)
	}

	if err := d.Delete(ctx, job.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	size, err = d.Size(ctx, "default")
	if err != nil {
		t.Fatalf("size after delete: %v", err)
	}
	if size != 0 {
		t.Fatalf("expected size 0 after delete, got %d", size)
	}
}

func TestSQLitePopEmpty(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t, qsqlite.Opts{})

	job, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop empty: %v", err)
	}
	if job != nil {
		t.Fatalf("expected nil job from empty queue, got %+v", job)
	}
}

func TestSQLiteFIFO(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t, qsqlite.Opts{})

	types := []string{"first", "second", "third"}
	for _, typ := range types {
		if err := d.Push(ctx, typ, nil); err != nil {
			t.Fatalf("push %s: %v", typ, err)
		}
	}

	for _, want := range types {
		job, err := d.Pop(ctx, "default")
		if err != nil {
			t.Fatalf("pop: %v", err)
		}
		if job == nil {
			t.Fatalf("expected job %s, got nil", want)
		}
		if job.Type != want {
			t.Errorf("FIFO order: want %s, got %s", want, job.Type)
		}
		if err := d.Delete(ctx, job.ID); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}
}

func TestSQLiteNamedQueues(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t, qsqlite.Opts{})

	if err := d.Push(ctx, "high-job", nil, queue.OnQueue("high")); err != nil {
		t.Fatalf("push high: %v", err)
	}
	if err := d.Push(ctx, "low-job", nil, queue.OnQueue("low")); err != nil {
		t.Fatalf("push low: %v", err)
	}

	highJob, err := d.Pop(ctx, "high")
	if err != nil {
		t.Fatalf("pop high: %v", err)
	}
	if highJob == nil || highJob.Type != "high-job" {
		t.Fatalf("expected high-job, got %v", highJob)
	}

	lowJob, err := d.Pop(ctx, "low")
	if err != nil {
		t.Fatalf("pop low: %v", err)
	}
	if lowJob == nil || lowJob.Type != "low-job" {
		t.Fatalf("expected low-job, got %v", lowJob)
	}

	// other queue should be empty
	none, err := d.Pop(ctx, "high")
	if err != nil {
		t.Fatalf("pop high again: %v", err)
	}
	if none != nil {
		t.Fatalf("expected nil from empty high queue, got %+v", none)
	}
}

func TestSQLiteDelayedJob(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t, qsqlite.Opts{})

	if err := d.Push(ctx, "delayed", nil, queue.WithDelay(1*time.Hour)); err != nil {
		t.Fatalf("push delayed: %v", err)
	}

	job, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if job != nil {
		t.Fatalf("expected nil for delayed job, got %+v", job)
	}
}

func TestSQLiteMaxTries(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t, qsqlite.Opts{})

	if err := d.Push(ctx, "risky", nil, queue.WithMaxTries(5)); err != nil {
		t.Fatalf("push: %v", err)
	}

	job, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if job == nil {
		t.Fatal("expected job, got nil")
	}
	if job.MaxTries != 5 {
		t.Errorf("max_tries: want 5, got %d", job.MaxTries)
	}
}

func TestSQLiteRelease(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t, qsqlite.Opts{})

	if err := d.Push(ctx, "task", []byte("data")); err != nil {
		t.Fatalf("push: %v", err)
	}

	job1, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if job1 == nil {
		t.Fatal("expected job, got nil")
	}

	if err := d.Release(ctx, job1.ID, 0); err != nil {
		t.Fatalf("release: %v", err)
	}

	job2, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop after release: %v", err)
	}
	if job2 == nil {
		t.Fatal("expected job after release, got nil")
	}
	if job2.ID == job1.ID {
		t.Errorf("expected new ID after release, got same: %s", job1.ID)
	}
	if job2.Attempts != 2 {
		t.Errorf("attempts after release: want 2, got %d", job2.Attempts)
	}
}

func TestSQLiteReleaseFIFO(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t, qsqlite.Opts{})

	if err := d.Push(ctx, "first", nil); err != nil {
		t.Fatalf("push first: %v", err)
	}
	if err := d.Push(ctx, "second", nil); err != nil {
		t.Fatalf("push second: %v", err)
	}

	first, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop first: %v", err)
	}
	if first == nil || first.Type != "first" {
		t.Fatalf("expected first job, got %v", first)
	}

	// Release first back with no delay — it should re-enqueue after second
	if err := d.Release(ctx, first.ID, 0); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Next pop should return "second" (lower ID, was inserted first and untouched)
	next, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop after release: %v", err)
	}
	if next == nil {
		t.Fatal("expected job, got nil")
	}
	if next.Type != "second" {
		t.Errorf("FIFO after release: want second, got %s", next.Type)
	}
}

func TestSQLiteFailed(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	d, err := qsqlite.New(db, qsqlite.Opts{})
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}

	if err := d.Push(ctx, "broken", []byte("oops")); err != nil {
		t.Fatalf("push: %v", err)
	}

	job, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if job == nil {
		t.Fatal("expected job, got nil")
	}

	if err := d.Failed(ctx, job, "something went wrong"); err != nil {
		t.Fatalf("failed: %v", err)
	}

	size, err := d.Size(ctx, "default")
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	if size != 0 {
		t.Errorf("expected size 0 after failed, got %d", size)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM failed_jobs`).Scan(&count); err != nil {
		t.Fatalf("query failed_jobs: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row in failed_jobs, got %d", count)
	}
}

func TestSQLiteTableAutoCreate(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	_, err := qsqlite.New(db, qsqlite.Opts{})
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}

	// verify tables exist by querying them
	if _, err := db.ExecContext(ctx, `SELECT 1 FROM jobs LIMIT 1`); err != nil {
		t.Errorf("jobs table not created: %v", err)
	}
	if _, err := db.ExecContext(ctx, `SELECT 1 FROM failed_jobs LIMIT 1`); err != nil {
		t.Errorf("failed_jobs table not created: %v", err)
	}
}

func TestSQLiteStuckJobReclaim(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	d, err := qsqlite.New(db, qsqlite.Opts{RetryAfter: 1 * time.Second})
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}

	if err := d.Push(ctx, "stuck", nil); err != nil {
		t.Fatalf("push: %v", err)
	}

	// Pop to reserve the job
	job, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if job == nil {
		t.Fatal("expected job, got nil")
	}
	if job.Attempts != 1 {
		t.Fatalf("attempts after first pop: want 1, got %d", job.Attempts)
	}

	// Backdate reserved_at by 10 seconds so it looks stuck
	staleReservedAt := time.Now().UTC().Unix() - 10
	if _, err := db.Exec(`UPDATE jobs SET reserved_at = ? WHERE id = ?`, staleReservedAt, job.ID); err != nil {
		t.Fatalf("backdate reserved_at: %v", err)
	}

	// Pop again — should reclaim the stuck job
	job2, err := d.Pop(ctx, "default")
	if err != nil {
		t.Fatalf("pop reclaim: %v", err)
	}
	if job2 == nil {
		t.Fatal("expected reclaimed job, got nil")
	}
	if job2.Attempts != 2 {
		t.Errorf("attempts after reclaim: want 2, got %d", job2.Attempts)
	}
}

func TestSQLiteConcurrentPop(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	// Limit to one connection so all goroutines share the same in-memory DB.
	db.SetMaxOpenConns(1)
	q, err := qsqlite.New(db, qsqlite.Opts{})
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}

	// Push 100 jobs.
	for i := 0; i < 100; i++ {
		_ = q.Push(ctx, "concurrent", []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}

	// Pop from 10 goroutines concurrently.
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[string]bool)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				job, err := q.Pop(ctx, "default")
				if err != nil {
					t.Errorf("Pop: %v", err)
					return
				}
				if job == nil {
					return
				}
				mu.Lock()
				if seen[job.ID] {
					t.Errorf("duplicate job ID: %s", job.ID)
				}
				seen[job.ID] = true
				mu.Unlock()
				_ = q.Delete(ctx, job.ID)
			}
		}()
	}
	wg.Wait()

	if len(seen) != 100 {
		t.Errorf("processed %d jobs, want 100", len(seen))
	}
}
