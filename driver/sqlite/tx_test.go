package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	queue "github.com/hollis-labs/go-queue"
)

func openFile(t *testing.T, busyMS int, conns int) *sql.DB {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "q.db") +
		"?_pragma=busy_timeout(" + strconv.Itoa(busyMS) + ")&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(conns)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func count(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Each transactional method must fail at BEGIN, not later at its first write,
// when another connection holds the writer lock. A deferred BEGIN would sail
// through BEGIN and the first read and fail at a write ("... delete: database
// is locked"), which is the difference this test pins per call site. Failed
// starts with a write, so for it this is the only observable proof.
func TestTransactionsTakeTheWriterLockAtBegin(t *testing.T) {
	ctx := context.Background()
	db := openFile(t, 150, 4)
	d, err := New(db, Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Push(ctx, "t", []byte("x")); err != nil {
		t.Fatal(err)
	}

	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	if _, err := holder.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = holder.ExecContext(ctx, "ROLLBACK") }()

	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{"Pop", func() error { _, err := d.Pop(ctx, "default"); return err }, "sqlite pop begin:"},
		{"Release", func() error { return d.Release(ctx, "1", 0) }, "sqlite release begin:"},
		{"Failed", func() error {
			return d.Failed(ctx, &queue.QueuedJob{ID: "1", Queue: "default", Type: "t"}, "boom")
		}, "sqlite failed begin:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			err := tc.run()
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "locked") {
				t.Fatalf("err = %v, want %q with a lock error", err, tc.want)
			}
			if time.Since(start) < 100*time.Millisecond {
				t.Errorf("gave up after %v; BEGIN IMMEDIATE should wait out busy_timeout", time.Since(start))
			}
		})
	}
}

func TestWithImmediate_FnErrorRollsBackAndIsReturnedUnchanged(t *testing.T) {
	ctx := context.Background()
	db := openFile(t, 1000, 2)
	if _, err := New(db, Opts{}); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	err := withImmediate(ctx, db, "op", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `INSERT INTO jobs (type, available_at, created_at) VALUES ('t', 0, 0)`); err != nil {
			return err
		}
		return boom
	})
	if err != boom {
		t.Fatalf("err = %v, want the callback's error unchanged", err)
	}
	if n := count(t, db, "jobs"); n != 0 {
		t.Fatalf("rolled-back insert persisted: %d rows", n)
	}
	// and the lock was released: another writer is not blocked
	if _, err := db.ExecContext(ctx, `INSERT INTO jobs (type, available_at, created_at) VALUES ('t', 0, 0)`); err != nil {
		t.Fatalf("writer blocked after rollback: %v", err)
	}
}

func TestWithImmediate_SuccessCommits(t *testing.T) {
	ctx := context.Background()
	db := openFile(t, 1000, 2)
	if _, err := New(db, Opts{}); err != nil {
		t.Fatal(err)
	}
	if err := withImmediate(ctx, db, "op", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO jobs (type, available_at, created_at) VALUES ('t', 0, 0)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, "jobs"); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
}

// The caller's context is often why the callback failed. ROLLBACK runs on its
// own context, and the connection goes back to the pool clean rather than being
// discarded or left holding the writer lock. (modernc.org/sqlite happens to
// execute a quick ROLLBACK even on a canceled context, so using the caller's
// context there is an equivalent mutant today; the separate context is
// protection against a driver or a slow rollback that does honor it.)
func TestWithImmediate_RollbackSurvivesCanceledContext(t *testing.T) {
	db := openFile(t, 1000, 2)
	if _, err := New(db, Opts{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := withImmediate(ctx, db, "op", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `INSERT INTO jobs (type, available_at, created_at) VALUES ('t', 0, 0)`); err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if n := count(t, db, "jobs"); n != 0 {
		t.Fatalf("rows = %d, want 0", n)
	}
	if s := db.Stats(); s.OpenConnections != 1 || s.Idle != 1 {
		t.Fatalf("pool after failure = %+v; the clean connection should have been returned, not discarded", s)
	}
}

// If ROLLBACK itself fails the connection may still be inside the transaction,
// holding the writer lock: it must be discarded, not pooled. Here the callback
// commits on its own, so the deferred ROLLBACK finds no transaction and fails.
func TestWithImmediate_FailedRollbackDiscardsTheConnection(t *testing.T) {
	ctx := context.Background()
	db := openFile(t, 1000, 2)
	if _, err := New(db, Opts{}); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	err := withImmediate(ctx, db, "op", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return err
		}
		return boom
	})
	if err != boom {
		t.Fatalf("err = %v", err)
	}
	if s := db.Stats(); s.OpenConnections != 0 {
		t.Fatalf("pool after failed rollback = %+v; the suspect connection must be discarded", s)
	}
	// the pool recovers
	if _, err := db.ExecContext(ctx, `INSERT INTO jobs (type, available_at, created_at) VALUES ('t', 0, 0)`); err != nil {
		t.Fatalf("write after discard: %v", err)
	}
}
