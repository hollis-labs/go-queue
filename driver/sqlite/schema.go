package sqlite

import "database/sql"

const defaultJobsTable = "jobs"
const defaultFailedTable = "failed_jobs"

func createTables(db *sql.DB, table, failedTable string) error {
	ddl := `
CREATE TABLE IF NOT EXISTS ` + table + ` (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    queue        TEXT    NOT NULL DEFAULT 'default',
    type         TEXT    NOT NULL,
    payload      BLOB,
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_tries    INTEGER NOT NULL DEFAULT 0,
    reserved_at  INTEGER,
    available_at INTEGER NOT NULL,
    created_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_` + table + `_queue_available ON ` + table + ` (queue, available_at);
CREATE INDEX IF NOT EXISTS idx_` + table + `_queue_reserved  ON ` + table + ` (queue, reserved_at);

CREATE TABLE IF NOT EXISTS ` + failedTable + ` (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    queue     TEXT    NOT NULL,
    type      TEXT    NOT NULL,
    payload   BLOB,
    error     TEXT    NOT NULL,
    attempts  INTEGER NOT NULL,
    failed_at INTEGER NOT NULL
);`
	_, err := db.Exec(ddl)
	return err
}
