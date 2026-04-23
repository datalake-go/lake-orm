// Package dialects defines the Dialect interface — the data-dialect
// opinion lake-orm delegates DDL, DML, and write planning to — plus
// the trio of concrete dialects lake-orm ships. Each concrete
// dialect lives in its own sub-package (iceberg, delta, duckdb) and
// only implements the bits its engine semantics actually need.
//
// Why this lives here and not at the root: Dialect is a contract a
// user composes into their Client at Open; the concrete impls live
// next to it. Putting the contract next to its implementations
// means a reader opening `dialects/` sees the whole concept — the
// interface, every v0 impl, and the rendering idioms — in one place.
// The root package stays lean.
//
// "Data dialect" rather than "format": Dialect covers CREATE TABLE
// clauses, MERGE semantics, table properties, and partition
// grammar, not just on-disk layout. Exactly one Dialect per Client
// at v0.
package dialects

import (
	"github.com/datalake-go/lake-orm/drivers"
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/types"
)

// Dialect describes the data-dialect opinion: DDL shape, DML shape,
// and write planning for Iceberg vs Delta vs any future lakehouse
// dialect.
//
// The interface is deliberately narrow — only the methods the
// Client actually calls. Write planning goes through PlanInsert,
// which routes between drivers.KindDirectIngest,
// drivers.KindParquetIngest, and drivers.KindParquetMerge based on
// batch size and mergeKey presence. Reads don't touch the Dialect
// — they go through drivers.Convertible with a driver-native Source
// closure (see the top-level Query[T] / QueryStream[T] /
// QueryFirst[T] helpers).
//
// structs.IndexStrategy / structs.LayoutStrategy let the Dialect
// expose its concrete strategy vocabulary for a given tag intent.
type Dialect interface {
	Name() string

	// CreateTableDDL emits one idempotent CREATE TABLE IF NOT EXISTS
	// statement for the supplied schema at the supplied location.
	CreateTableDDL(schema *structs.LakeSchema, loc types.Location) (string, error)

	// PlanInsert returns the ExecutionPlan the Driver should execute
	// for a given Insert request. The Dialect chooses between
	// KindDirectIngest, KindParquetIngest, and KindParquetMerge based
	// on batch size, mergeKey presence, and any caller-supplied
	// WritePath override.
	PlanInsert(req drivers.WriteRequest) (drivers.ExecutionPlan, error)

	// IndexStrategy resolves an `indexed` / `mergeKey` tag intent
	// into the dialect's concrete strategy descriptor.
	IndexStrategy(intent structs.IndexIntent) structs.IndexStrategy

	// LayoutStrategy resolves a `sortable` tag intent into the
	// dialect's concrete layout descriptor.
	LayoutStrategy(intent structs.LayoutIntent) structs.LayoutStrategy
}
