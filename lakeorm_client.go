package lakeorm

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Client is the main entry point. All methods are safe to call
// concurrently; the internal session pool serializes per-session state
// as needed.
//
// Writes bind to persisted types via Insert (auto-routes to MERGE
// when the struct carries a mergeKey). Reads run through the
// drivers.Convertible capability — build a driver-native Source
// with the concrete driver's conversion helpers, reach the driver
// via Driver(), then feed the Source to lakeorm.Query[T] /
// QueryStream[T] / QueryFirst[T] for typed decode:
//
//	drv := db.Driver().(*spark.Driver)
//	users, _ := lakeorm.Query[User](ctx, db,
//	    drv.FromSQL("SELECT * FROM users"))
//
// Migrate bootstraps tables from struct tags; MigrateGenerate writes
// .sql files for the lake-goose migration runner; CleanupStaging
// sweeps orphan staging prefixes; Exec is the raw-SQL escape hatch
// for DDL / one-off DML the typed surface doesn't cover.
type Client interface {
	// Insert writes records (pointer, slice, or slice-of-pointer) to
	// the target table. Validation runs first; then the Dialect
	// plans KindDirectIngest (small batch), KindParquetIngest (large
	// append), or KindParquetMerge (struct carries mergeKey) and the
	// Driver executes.
	Insert(ctx context.Context, records any, opts ...InsertOption) error

	// Driver returns the underlying driver. Callers type-assert to
	// the concrete driver type to reach per-driver conversion
	// helpers (spark.Driver.FromSQL, duckdb.Driver.FromRows, etc.)
	// or the raw native handle (spark.Driver.Session,
	// duckdb.Driver.DB).
	//
	// The reason Client exposes this instead of wrapping every
	// driver-specific helper behind a Client method is that read
	// grammar is driver-specific: a Spark DataFrame and a *sql.Rows
	// have different acquisition shapes, and hiding that costs the
	// caller power without buying portability.
	Driver() Driver

	// Exec runs a raw SQL statement that returns no rows (DDL, DML
	// that the typed surface doesn't cover).
	Exec(ctx context.Context, sql string, args ...any) (ExecResult, error)

	// Migrate is the bootstrap path — idempotent CREATE TABLE IF NOT
	// EXISTS derived from struct tags. Sufficient for dev and fresh
	// tables; ALTER TABLE-shaped schema evolution goes through
	// MigrateGenerate + lake-goose.
	Migrate(ctx context.Context, models ...any) error

	// MigrateGenerate writes iceberg/delta-dialect .sql files for any
	// pending struct diffs into dir, in goose's migration-file
	// format. Destructive operations (DROP COLUMN, RENAME COLUMN,
	// type narrowings, NOT-NULL tightenings) land with a
	// `-- DESTRUCTIVE: <reason>` informational comment so the
	// reviewer notices them in the PR diff.
	//
	// Execution is not this library's job — it belongs to lake-goose
	// running against the Spark Connect database/sql driver. An
	// lakeorm.sum manifest is emitted alongside so downstream tooling
	// can detect post-generation edits.
	//
	// Returns the list of generated file paths.
	MigrateGenerate(ctx context.Context, dir string, models ...any) ([]string, error)

	// CleanupStaging walks the Backend's _staging/ namespace and
	// deletes prefixes older than olderThan. Intended as a periodic
	// janitor (e.g. goroutine on a ticker) to sweep orphaned parquet
	// from aborted Insert calls.
	CleanupStaging(ctx context.Context, olderThan time.Duration) (*CleanupReport, error)

	// MetricsRegistry returns the Prometheus registry lakeorm writes
	// runtime metrics into. Consumed by the composed runtime
	// (lakehouse) to expose /metrics. Returns nil at v0.
	MetricsRegistry() *prometheus.Registry

	// Close releases the underlying driver, session pool, and any
	// backend resources. Safe to call once at process shutdown.
	Close() error
}
