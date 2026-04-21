package lakeorm

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

// Client is the main entry point. All methods are safe to call
// concurrently; the internal session pool serializes per-session state
// as needed.
type Client interface {
	// Core CRUD. records is *T, []*T, or []T where T has `spark` tags.
	// Validation runs before any I/O.
	Insert(ctx context.Context, records any, opts ...InsertOption) error
	InsertRaw(ctx context.Context, records any, opts ...InsertOption) RawInsertion
	Update(ctx context.Context, records any, opts ...UpdateOption) error
	Upsert(ctx context.Context, records any, opts ...UpsertOption) error
	Delete(ctx context.Context, records any, opts ...DeleteOption) error

	// Query is the dynamic (non-generic) entry point. Prefer the
	// top-level lakeorm.Query[T] / QueryStream[T] / QueryFirst[T]
	// helpers when T is known at compile time.
	Query(ctx context.Context) QueryBuilder

	// Escape hatches to raw SQL / DataFrame / Spark.
	Exec(ctx context.Context, sql string, args ...any) (ExecResult, error)
	DataFrame(ctx context.Context, sql string, args ...any) (DataFrame, error)
	Session(ctx context.Context) (*PooledSession, error)

	// Migrate is the bootstrap path — idempotent CREATE TABLE IF NOT
	// EXISTS derived from struct tags. Sufficient for dev + fresh
	// tables; schema evolution on existing tables goes through
	// lake-goose (authoring via MigrateGenerate, execution via the
	// goose-spark-ish CLI or goose's library API against the
	// database/sql driver in datalake-go/spark-connect-go).
	Migrate(ctx context.Context, models ...any) error
	Maintain() Maintenance
	Ping(ctx context.Context) error
	Close() error

	// MetricsRegistry returns the Prometheus registry lakeorm writes
	// runtime metrics into. Returns nil at v0 as a placeholder for
	// v1+ integration.
	MetricsRegistry() *prometheus.Registry

	// MigrateGenerate writes iceberg/delta-dialect .sql files for any
	// pending struct diffs into dir, in goose's migration-file format.
	// Destructive operations (DROP COLUMN, RENAME COLUMN, type
	// narrowings, NOT-NULL tightenings) land with a `-- DESTRUCTIVE:
	// <reason>` informational comment so the reviewer notices them in
	// the PR diff.
	//
	// Execution is not this library's job — it belongs to lake-goose
	// running against the Spark Connect database/sql driver. An
	// atlas.sum manifest is emitted alongside so downstream tooling
	// can detect post-generation edits.
	//
	// Returns the list of generated file paths.
	MigrateGenerate(ctx context.Context, dir string, structs ...any) ([]string, error)

	// AssertSchema verifies the catalog's current schema matches
	// compiled code's expectation (SchemaFingerprint) for each
	// struct. Recommended at app startup after lakeorm.Verify.
	// v0 stub — the DESCRIBE TABLE catalog read lands in v1.
	AssertSchema(ctx context.Context, structs ...any) error
}

// RawInsertion is the opt-in raw-then-merge escape hatch for
// genuinely untrusted external inputs (research datasets, untyped
// third-party feeds, CSVs of unknown provenance). It is NOT the
// default — db.Insert writes straight to the target table. Bronze-
// style landing zones exist here as a conscious opt-in, never as
// an accidental default.
type RawInsertion interface {
	ThenMerge(ctx context.Context, opts MergeOpts) error
	AsyncThenMerge(opts MergeOpts) MergeFuture
	Commit(ctx context.Context) error
}
