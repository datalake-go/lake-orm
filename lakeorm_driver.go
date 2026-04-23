package lakeorm

import (
	"context"
)

// Driver is the write-side execution contract every lake-orm driver
// implements: run an ExecutionPlan the Dialect produced, honour the
// two-phase Finalizer lifecycle, and expose a raw Exec escape hatch
// for DDL and one-off DML the typed surface doesn't cover.
//
// Read-side decoding is a separate capability the driver optionally
// implements — see drivers.Convertible in the drivers package. The
// split is deliberate: writes are planned and dispatched by the
// Dialect + Driver; reads come in as driver-native Source closures
// the Convertible impl decodes into user-declared Go types. That
// separation is what lets lake-orm avoid owning a query grammar.
//
// Concrete v0 drivers live under the drivers/ sibling:
//
//   - drivers/spark             — generic Spark Connect
//   - drivers/databricksconnect — Databricks Connect (OAuth M2M)
//   - drivers/databricks        — Databricks native (BYO *sql.DB)
//   - drivers/duckdb            — embedded DuckDB (CGO)
//
// All four satisfy this interface plus drivers.Convertible.
type Driver interface {
	Name() string

	// Execute runs a plan and returns a Finalizer for two-phase commit.
	// Single-phase plans (e.g. small-batch direct ingest) return a
	// no-op Finalizer so callers never need to branch.
	Execute(ctx context.Context, plan ExecutionPlan) (Result, Finalizer, error)

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
// rows (writes, DDL). Reads return their decoded values directly via
// the drivers.Convertible capability.
type Result struct {
	RowsAffected int64
}

// ExecResult mirrors database/sql.Result for the raw Exec escape hatch.
type ExecResult struct {
	RowsAffected int64
}
