// Package drivers is the contract shared by every lake-orm driver.
// A Driver is what ships writes to a lakehouse engine and (optionally)
// decodes reads back into user-declared Go types; concrete
// implementations for Spark Connect, DuckDB, Databricks SQL, and
// Databricks Connect sit in sibling sub-packages.
//
// Three orthogonal responsibilities live here:
//
//   - Driver + Finalizer + Result + ExecResult — the write-side
//     execution contract. The client hands a Driver an ExecutionPlan
//     the Dialect built, the Driver runs it, the Finalizer closes the
//     two-phase commit.
//
//   - ExecutionPlan + PlanKind + StagingRef + WriteRequest + WritePath
//     — the plan wire shape. The client populates a WriteRequest on
//     Insert, the Dialect converts it to an ExecutionPlan, the Driver
//     dispatches on Kind.
//
//   - Source + Convertible — the read-side capability. A caller hands
//     the driver a Source closure that produces the driver's native
//     row source (a Spark DataFrame, a *sql.Rows, whatever); the
//     driver decodes each row into the user-supplied Go type. This
//     is why lake-orm never has to own a query grammar.
//
// Per-driver conversion helpers — FromSQL / FromDataFrame / FromRows /
// FromTable / FromRow, each a method on the concrete driver type —
// build the Source for common cases so callers write one line instead
// of six:
//
//	drv := db.Driver().(*spark.Driver)
//	users, _ := lakeorm.Query[User](ctx, db, drv.FromSQL("SELECT * FROM users"))
//
// Anything the helpers don't cover can be expressed as a bare
// closure: five lines of glue at the call site, no framework
// primitives.
package drivers

import (
	"context"
	"iter"

	"github.com/datalake-go/lake-orm/backends"
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/types"
)

// Driver is the write-side execution contract every lake-orm driver
// implements: run an ExecutionPlan the Dialect produced, honour the
// two-phase Finalizer lifecycle, and expose a raw Exec escape hatch
// for DDL and one-off DML the typed surface doesn't cover.
//
// Read-side decoding is a separate capability the driver optionally
// implements — see Convertible below. The split is deliberate: writes
// are planned and dispatched by the Dialect + Driver; reads come in
// as driver-native Source closures the Convertible impl decodes into
// user-declared Go types.
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
// the Convertible capability.
type Result struct {
	RowsAffected int64
}

// ExecResult mirrors database/sql.Result for the raw Exec escape hatch.
type ExecResult struct {
	RowsAffected int64
}

// ExecutionPlan is the opaque artifact a Dialect hands to a Driver. The
// Driver executes the plan without knowing the Dialect's name. Internally
// it is a tagged union of variants (DirectIngest, ParquetIngest,
// ParquetMerge, SQL, DDL); Dialect constructs them, Driver reads the
// Kind and dispatches.
//
// The type is intentionally a struct (not an interface) so the wire
// shape can evolve without breaking v1+ drivers that consume plans
// they didn't construct.
type ExecutionPlan struct {
	Kind PlanKind
	SQL  string // For KindSQL / KindDDL
	Args []any  // SQL-parameter bindings
	// IngestID is the per-operation UUIDv7. Every write plan carries
	// it. The driver stamps it onto every row (direct-ingest) or
	// onto the parquet staging output (parquet-ingest) so the
	// system-managed _ingest_id column is populated on every row.
	// Also the filter predicate on KindParquetMerge's MERGE source.
	IngestID string
	Target   string              // table name for writes
	Staging  StagingRef          // populated for KindParquetIngest
	Rows     any                 // typed slice for KindDirectIngest
	Schema   *structs.LakeSchema // referenced by the Driver for type-aware ingest
	Options  map[string]any      // Dialect-specific hints opaque to the Driver
}

// PlanKind identifies the plan variant. Values are stable — drivers
// branch on them.
type PlanKind int

const (
	KindSQL PlanKind = iota
	KindStream
	KindDDL
	KindDirectIngest
	KindParquetIngest
	// KindParquetMerge is the upsert variant of KindParquetIngest.
	// Emitted when the schema carries at least one mergeKey field.
	// Drivers execute MERGE INTO <target> USING (SELECT * FROM
	// <staging> WHERE _ingest_id = '<plan.IngestID>') AS source ON
	// target.<mergeKey> = source.<mergeKey> WHEN MATCHED THEN
	// UPDATE SET * WHEN NOT MATCHED THEN INSERT *. The _ingest_id
	// filter on source bounds the merge to this batch so the
	// operation is O(batch_size) and retry-on-OCC-conflict is
	// idempotent.
	KindParquetMerge
)

// StagingRef names the URI prefix and parts produced by a fast-path
// partition writer. The Driver reads this to emit the
// `INSERT ... SELECT FROM parquet.<prefix>/*.parquet` statement.
type StagingRef struct {
	Backend  backends.Backend
	Prefix   string
	PartKeys []string
	Location types.Location
}

// WriteRequest is what the Client hands the Dialect on Insert.
type WriteRequest struct {
	Ctx    context.Context
	Schema *structs.LakeSchema
	// IngestID is a UUIDv7 correlation ID generated by Client.Insert
	// for every operation. Threads through staging prefix, logs, and
	// the Finalizer's cleanup target. Required for the fast path;
	// Dialect implementations use this (not Idempotency) to compute
	// staging locations.
	IngestID       string
	Records        any
	RecordCount    int
	ApproxRowBytes int
	// Idempotency is an optional caller-supplied deduplication token.
	// Distinct from IngestID: IngestID is internal operational
	// correlation; Idempotency is the contract with the caller for
	// retry safety. Empty when the caller didn't supply one.
	Idempotency   string
	Backend       backends.Backend
	FastPathBytes int            // advisory crossover threshold
	ForcePath     WritePath      // None / ViaGRPC / ViaObjectStorage
	Options       map[string]any // dialect-specific overrides
}

// WritePath is the caller's optional override of the Dialect's routing.
type WritePath int

const (
	WritePathAuto WritePath = iota
	WritePathGRPC
	WritePathObjectStorage
)

// Source is the closure a caller hands to a Convertible read. It
// returns the driver's native row source when invoked — e.g. a
// sparksql.DataFrame for the Spark driver, a *sql.Rows for
// DuckDB / Databricks SQL. The concrete type is opaque to the
// caller; the Convertible implementation type-asserts to its own
// known native type and fails fast if something else came back.
//
// The cleanup function is the source's release hook: any resources
// acquired to produce the native (a borrowed session, an open
// *sql.Rows, a file handle) get released when the Convertible
// implementation is done iterating. May be nil when the source
// holds nothing that needs releasing. Convertible implementations
// call it via defer immediately after the source returns a
// non-nil native value, so the lifecycle always closes cleanly
// even if iteration returns early.
type Source func(ctx context.Context) (native any, cleanup func(), err error)

// Convertible is the optional driver capability: given a Source
// that produces the driver's own native row type, decode each row
// into the user-supplied Go target.
//
// The three methods match the three typed-read shapes the top-
// level lakeorm helpers expose:
//
//   - Collect walks the source end-to-end and writes every
//     decoded row into out, which must be a *[]T.
//   - First walks the source until the first row, writes the
//     decoded value into out (a *T), returns errors.ErrNoRows
//     if the source yielded zero rows.
//   - Stream yields one decoded row at a time as the driver
//     walks the source; constant memory regardless of result
//     size. sample is a *T so the driver reflects to discover T
//     once at the top of iteration.
//
// Reflection-via-out is the lingua franca between the driver and
// the typed helpers. Drivers that hold their decode path as a
// Go-generics call internally can still satisfy this interface by
// reflecting on out and dispatching.
type Convertible interface {
	Collect(ctx context.Context, source Source, out any) error
	First(ctx context.Context, source Source, out any) error
	Stream(ctx context.Context, source Source, sample any) iter.Seq2[any, error]
}
