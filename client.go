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
// The surface matches what the README promises: validated writes via
// Insert (which auto-routes to MERGE when the struct carries a
// mergeKey), typed reads via the top-level Query[T] generics over
// DataFrame, bootstrap via Migrate, migration authoring via
// MigrateGenerate, staging cleanup via CleanupStaging, and escape
// hatches Exec / DataFrame for anything the typed surface doesn't
// cover.
type Client interface {
	// Insert writes records (pointer, slice, or slice-of-pointer) to
	// the target table. Validation runs first; then the Dialect
	// plans KindDirectIngest (small batch), KindParquetIngest (large
	// append), or KindParquetMerge (struct carries mergeKey) and the
	// Driver executes.
	Insert(ctx context.Context, records any, opts ...InsertOption) error

	// Query is the dynamic (non-generic) entry point. Prefer the
	// top-level lakeorm.Query[T] / QueryStream[T] / QueryFirst[T]
	// helpers when T is known at compile time.
	Query(ctx context.Context) QueryBuilder

	// Exec runs a raw SQL statement that returns no rows (DDL, DML
	// that the typed surface doesn't cover).
	Exec(ctx context.Context, sql string, args ...any) (ExecResult, error)

	// DataFrame is the escape hatch for CQRS reads — hand a SQL
	// string in, get a driver-agnostic DataFrame back, then
	// materialise with lakeorm.CollectAs[T] / StreamAs[T] / FirstAs[T].
	DataFrame(ctx context.Context, sql string, args ...any) (DataFrame, error)

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
	MigrateGenerate(ctx context.Context, dir string, structs ...any) ([]string, error)

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
