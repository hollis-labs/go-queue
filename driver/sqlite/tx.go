package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

// rollbackTimeout bounds the ROLLBACK issued when a transaction fails. It runs
// on its own context: the caller's is often the reason the transaction failed.
const rollbackTimeout = 5 * time.Second

// withImmediate runs fn inside one BEGIN IMMEDIATE transaction on a dedicated
// connection, so the writer lock is taken at BEGIN rather than raced for at the
// first write. A deferred BEGIN lets two connections both read a row and then
// collide when they upgrade; under WAL that surfaces as SQLITE_BUSY_SNAPSHOT,
// which busy_timeout does not retry. The driver issues BEGIN itself so it does
// not depend on how the caller opened the *sql.DB (no _txlock=immediate needed).
//
// fn must use the *sql.Conn it is given for every statement, never the Driver's
// *sql.DB: only that connection carries the transaction. fn's error rolls the
// transaction back and is returned unchanged; otherwise it commits. op labels
// errors raised by the transaction machinery ("<op> begin", "<op> commit").
func withImmediate(ctx context.Context, db *sql.DB, op string, fn func(conn *sql.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%s begin: %w", op, err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("%s begin: %w", op, err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
		defer cancel()
		if _, err := conn.ExecContext(rbCtx, "ROLLBACK"); err != nil {
			// The connection may still be inside the transaction. Do not hand it
			// back to the pool holding the writer lock: have database/sql discard it.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()

	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("%s commit: %w", op, err)
	}
	committed = true
	return nil
}
