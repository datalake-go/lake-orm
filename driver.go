package lakeorm

import (
	"context"
)

// Driver is the connection + execution mechanism. At v0 both concrete
// Drivers are Spark Connect variants (spark.Remote, databricksconnect.Driver) —
// same protocol, different connection setup.
type Driver interface {
	Name() string

	// Execute runs a plan and returns a Finalizer for two-phase commit.
	// Single-phase plans (e.g. small-batch direct ingest) return a
	// no-op Finalizer so callers never need to branch.
	Execute(ctx context.Context, plan ExecutionPlan) (Result, Finalizer, error)

	// ExecuteStreaming runs a read plan and returns a pull-based row
	// stream. No finalizer — reads don't stage.
	ExecuteStreaming(ctx context.Context, plan ExecutionPlan) (RowStream, error)

	// DataFrame is the escape hatch — raw SQL to a DataFrame that the
	// caller can then chain Spark operations on.
	DataFrame(ctx context.Context, sql string, args ...any) (DataFrame, error)

	// Exec runs a SQL statement that returns no rows (DDL, DML that
	// doesn't need the Dialect-planned path).
	Exec(ctx context.Context, sql string, args ...any) (ExecResult, error)

	Close() error
}

// Finalizer is the commit phase of a two-phase write. Modeled on
// database/sql.Tx: call Commit on success, Abort otherwise, safely
// defer Abort for the error path. Commit is idempotent.
type Finalizer interface {
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
}

// Result is the outcome of a single ExecutionPlan that did not produce
// rows (writes, DDL). Reads return a RowStream instead.
type Result struct {
	RowsAffected int64
}

// ExecResult mirrors database/sql.Result for the raw Exec escape hatch.
type ExecResult struct {
	RowsAffected int64
}
