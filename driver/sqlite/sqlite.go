package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	queue "github.com/hollis-labs/go-queue"
	_ "modernc.org/sqlite"
)

// Opts configures the SQLite driver.
type Opts struct {
	// Table is the name of the jobs table. Default: "jobs".
	Table string
	// FailedTable is the name of the failed jobs table. Default: "failed_jobs".
	FailedTable string
	// RetryAfter is the duration after which a reserved job is considered
	// stuck and can be reclaimed by Pop. Default: 60s.
	RetryAfter time.Duration
}

// Driver is a SQLite-backed Queue implementation.
type Driver struct {
	db          *sql.DB
	table       string
	failedTable string
	retryAfter  time.Duration
}

// New creates the required tables (if not present) and returns a Driver.
func New(db *sql.DB, opts Opts) (*Driver, error) {
	if opts.Table == "" {
		opts.Table = defaultJobsTable
	}
	if opts.FailedTable == "" {
		opts.FailedTable = defaultFailedTable
	}
	if opts.RetryAfter == 0 {
		opts.RetryAfter = 60 * time.Second
	}

	if err := createTables(db, opts.Table, opts.FailedTable); err != nil {
		return nil, fmt.Errorf("sqlite driver: create tables: %w", err)
	}

	return &Driver{
		db:          db,
		table:       opts.Table,
		failedTable: opts.FailedTable,
		retryAfter:  opts.RetryAfter,
	}, nil
}

// Push enqueues a new job.
func (d *Driver) Push(ctx context.Context, jobType string, payload []byte, opts ...queue.PushOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cfg := queue.ResolvePushConfig(opts)
	now := time.Now().UTC().Unix()
	availableAt := now + int64(cfg.Delay.Seconds())

	_, err := d.db.ExecContext(
		ctx,
		`INSERT INTO `+d.table+` (queue, type, payload, max_tries, available_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		cfg.Queue, jobType, payload, cfg.MaxTries, availableAt, now,
	)
	if err != nil {
		return fmt.Errorf("sqlite push: %w", err)
	}
	return nil
}

// Pop retrieves and reserves the next available job from the named queue.
// Returns nil, nil when the queue is empty.
func (d *Driver) Pop(ctx context.Context, queueName string) (*queue.QueuedJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var job *queue.QueuedJob
	err := withImmediate(ctx, d.db, "sqlite pop", func(conn *sql.Conn) error {
		now := time.Now().UTC().Unix()
		retryThreshold := now - int64(d.retryAfter.Seconds())

		var (
			id          int64
			jobType     string
			jobPayload  []byte
			attempts    int
			maxTries    int
			createdAt   int64
			availableAt int64
			reservedAt  *int64
		)

		err := conn.QueryRowContext(
			ctx,
			`SELECT id, type, payload, attempts, max_tries, created_at, available_at, reserved_at
			 FROM `+d.table+`
			 WHERE queue = ?
			   AND ((reserved_at IS NULL AND available_at <= ?) OR (reserved_at IS NOT NULL AND reserved_at <= ?))
			 ORDER BY id ASC
			 LIMIT 1`,
			queueName, now, retryThreshold,
		).Scan(&id, &jobType, &jobPayload, &attempts, &maxTries, &createdAt, &availableAt, &reservedAt)
		if err == sql.ErrNoRows {
			return nil // empty queue: nothing reserved, the empty transaction commits
		}
		if err != nil {
			return fmt.Errorf("sqlite pop select: %w", err)
		}

		newAttempts := attempts + 1
		if _, err := conn.ExecContext(
			ctx,
			`UPDATE `+d.table+` SET reserved_at = ?, attempts = ? WHERE id = ?`,
			now, newAttempts, id,
		); err != nil {
			return fmt.Errorf("sqlite pop update: %w", err)
		}

		var reservedAtTime *time.Time
		if reservedAt != nil {
			t := time.Unix(*reservedAt, 0).UTC()
			reservedAtTime = &t
		}
		job = &queue.QueuedJob{
			ID:          fmt.Sprintf("%d", id),
			Type:        jobType,
			Queue:       queueName,
			Payload:     jobPayload,
			Attempts:    newAttempts,
			MaxTries:    maxTries,
			CreatedAt:   time.Unix(createdAt, 0).UTC(),
			AvailableAt: time.Unix(availableAt, 0).UTC(),
			ReservedAt:  reservedAtTime,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

// Delete removes a completed job from the queue.
func (d *Driver) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	_, err := d.db.ExecContext(ctx, `DELETE FROM `+d.table+` WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("sqlite delete: %w", err)
	}
	return nil
}

// Release puts a job back on the queue with a delay, preserving its attempt count.
func (d *Driver) Release(ctx context.Context, id string, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return withImmediate(ctx, d.db, "sqlite release", func(conn *sql.Conn) error {
		var (
			jobType   string
			queueName string
			payload   []byte
			attempts  int
			maxTries  int
			createdAt int64
		)

		err := conn.QueryRowContext(
			ctx,
			`SELECT type, queue, payload, attempts, max_tries, created_at FROM `+d.table+` WHERE id = ?`,
			id,
		).Scan(&jobType, &queueName, &payload, &attempts, &maxTries, &createdAt)
		if err != nil {
			return fmt.Errorf("sqlite release select: %w", err)
		}

		if _, err := conn.ExecContext(ctx, `DELETE FROM `+d.table+` WHERE id = ?`, id); err != nil {
			return fmt.Errorf("sqlite release delete: %w", err)
		}

		now := time.Now().UTC().Unix()
		availableAt := now + int64(delay.Seconds())

		if _, err := conn.ExecContext(
			ctx,
			`INSERT INTO `+d.table+` (queue, type, payload, attempts, max_tries, available_at, created_at, reserved_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, NULL)`,
			queueName, jobType, payload, attempts, maxTries, availableAt, createdAt,
		); err != nil {
			return fmt.Errorf("sqlite release insert: %w", err)
		}
		return nil
	})
}

// Size returns the total number of jobs on the named queue.
func (d *Driver) Size(ctx context.Context, queueName string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	var count int
	err := d.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM `+d.table+` WHERE queue = ?`,
		queueName,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("sqlite size: %w", err)
	}
	return count, nil
}

// Failed moves a job to the failed jobs store.
func (d *Driver) Failed(ctx context.Context, job *queue.QueuedJob, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return withImmediate(ctx, d.db, "sqlite failed", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `DELETE FROM `+d.table+` WHERE id = ?`, job.ID); err != nil {
			return fmt.Errorf("sqlite failed delete: %w", err)
		}

		now := time.Now().UTC().Unix()
		if _, err := conn.ExecContext(
			ctx,
			`INSERT INTO `+d.failedTable+` (queue, type, payload, error, attempts, failed_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			job.Queue, job.Type, job.Payload, errMsg, job.Attempts, now,
		); err != nil {
			return fmt.Errorf("sqlite failed insert: %w", err)
		}
		return nil
	})
}
