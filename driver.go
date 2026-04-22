package lakeorm

import (
	"context"
	"io"
	"iter"

	"github.com/datalake-go/lake-orm/types"
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

// Dialect describes the data-dialect opinion: DDL shape, DML shape,
// capabilities, and semantics for Iceberg vs Delta vs any future
// lakehouse dialect. "Data dialect" rather than "format" because
// it's not just on-disk layout — it covers CREATE TABLE clauses,
// MERGE semantics, table properties, partition grammar, and schema-
// evolution rules. Exactly one Dialect per Client at v0.
type Dialect interface {
	Name() string

	CreateTableDDL(schema *LakeSchema, loc types.Location) (string, error)
	AlterTableDDL(schema *LakeSchema, existing *TableInfo) ([]string, error)

	PlanInsert(req WriteRequest) (ExecutionPlan, error)
	PlanUpsert(req UpsertRequest) (ExecutionPlan, error)
	PlanDelete(req DeleteRequest) (ExecutionPlan, error)
	PlanQuery(req QueryRequest) (ExecutionPlan, error)

	IndexStrategy(intent IndexIntent) IndexStrategy
	LayoutStrategy(intent LayoutIntent) LayoutStrategy

	Maintenance() Maintenance
}

// Maintenance is the Dialect-level physical-optimization surface.
// At v0 all implementations return ErrNotImplemented — the interface
// exists so the Client API doesn't shift when v1 fleshes each
// Dialect out.
type Maintenance interface {
	Optimize(ctx context.Context, table string, opts MaintenanceOptions) error
	Vacuum(ctx context.Context, table string, opts VacuumOptions) error
	Stats(ctx context.Context, table string) (TableStats, error)
}

// MaintenanceOptions / VacuumOptions are the v0 stubs. Specific
// Dialects may carry richer option structs behind Dialect.Maintenance()
// as v1 lands.
type MaintenanceOptions struct {
	Filter string
	ZOrder []string
}

// NewMaintenanceOptions builds a MaintenanceOptions with the supplied
// WHERE filter and Z-order column list.
func NewMaintenanceOptions(filter string, zorder ...string) MaintenanceOptions {
	return MaintenanceOptions{Filter: filter, ZOrder: zorder}
}

type VacuumOptions struct {
	RetentionHours int
}

// NewVacuumOptions builds a VacuumOptions with the supplied retention
// window in hours.
func NewVacuumOptions(retentionHours int) VacuumOptions {
	return VacuumOptions{RetentionHours: retentionHours}
}

type TableStats struct {
	NumRows       int64
	NumDataFiles  int64
	TotalBytes    int64
	SnapshotCount int
}

type TableInfo struct {
	Name       string
	Location   types.Location
	Columns    []ColumnInfo
	Partitions []string
	SnapshotID string
}

type ColumnInfo struct {
	Name     string
	DataType string
	Nullable bool
}

// Backend is where the bytes live. Each concrete backend owns its
// SDK directly (aws-sdk-go-v2 for S3, cloud.google.com/go/storage for
// GCS, stdlib for File / Memory) — no generic abstraction layer.
type Backend interface {
	Name() string

	// RootURI returns the URI that the Driver will interpolate into
	// its SQL. Client and Driver must resolve this string to the same
	// physical storage; endpoint/credential differences are handled
	// per-actor, not in Backend.
	RootURI() string

	TableLocation(tableName string) types.Location
	StagingPrefix(ingestID string) string
	// StagingLocation returns the absolute URI that Spark should read
	// from (e.g. "s3a://bucket/lake/staging/<id>"). Distinct from
	// StagingPrefix — Spark's Hadoop-AWS integration requires s3a://
	// even though the Backend's own SDK calls use s3://. The scheme
	// translation happens here, not in the Dialect.
	StagingLocation(ingestID string) types.Location

	Writer(ctx context.Context, key string) (io.WriteCloser, error)
	Reader(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)

	// CleanupStaging removes every object under prefix. Called by
	// Finalizer.Abort and by the staging-TTL janitor.
	CleanupStaging(ctx context.Context, prefix string) error
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

// DataFrame is the Driver-agnostic handle to a remote DataFrame. At v0
// it wraps the Spark Connect DataFrame; v1+ drivers may implement their
// own. The interface is deliberately minimal — anything fancier is
// available by unwrapping with DriverType().
type DataFrame interface {
	Schema(ctx context.Context) ([]ColumnInfo, error)
	Collect(ctx context.Context) ([][]any, error)
	Count(ctx context.Context) (int64, error)
	Stream(ctx context.Context) iter.Seq2[Row, error]

	// DriverType returns the Driver's native DataFrame handle for
	// callers that need to drop to the underlying API. Kept as any
	// to avoid leaking driver-specific types into the public
	// signature; callers type-assert to the concrete type they
	// expect (e.g. sparksql.DataFrame). Prefer the lakeorm.CollectAs
	// / lakeorm.StreamAs helpers when materialising into a typed
	// result struct.
	DriverType() any
}

// Row is a Driver-agnostic row handle. Scanner consumes these to
// populate typed structs.
type Row interface {
	Values() []any
	Columns() []string
}

// RowStream is the Driver's streaming primitive — one Row per
// iteration, constant memory, natural backpressure. It is an alias
// for iter.Seq2[Row, error] so it rangeable directly.
type RowStream = iter.Seq2[Row, error]

// QueryBuilder is the dynamic (non-generic) query entry point for cases
// where the result type isn't known at compile time. Prefer the
// top-level Query[T] generic when possible.
type QueryBuilder interface {
	From(table string) QueryBuilder
	Where(sql string, args ...any) QueryBuilder
	OrderBy(col string, desc bool) QueryBuilder
	Limit(n int) QueryBuilder
	Offset(n int) QueryBuilder
	Select(cols ...string) QueryBuilder

	Collect(ctx context.Context) ([][]any, error)
	DataFrame(ctx context.Context) (DataFrame, error)
}
