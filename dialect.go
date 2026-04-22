package lakeorm

import (
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/types"
)

// Dialect describes the data-dialect opinion: DDL shape, DML shape,
// and query planning for Iceberg vs Delta vs any future lakehouse
// dialect. "Data dialect" rather than "format" because it's not just
// on-disk layout — it covers CREATE TABLE clauses, MERGE semantics,
// table properties, partition grammar, and projection planning.
// Exactly one Dialect per Client at v0.
//
// The interface is deliberately narrow — only the methods the Client
// actually calls. Read planning goes through PlanQuery; write
// planning through PlanInsert (which routes between KindDirectIngest,
// KindParquetIngest, and KindParquetMerge based on batch size and
// mergeKey presence). structs.IndexStrategy / structs.LayoutStrategy let the
// Dialect expose its concrete strategy vocabulary for a given tag
// intent.
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
	PlanInsert(req WriteRequest) (ExecutionPlan, error)

	// PlanQuery returns the ExecutionPlan for a dynamic
	// (non-generic) Query/QueryBuilder call.
	PlanQuery(req QueryRequest) (ExecutionPlan, error)

	// IndexStrategy resolves an `indexed` / `mergeKey` tag intent
	// into the dialect's concrete strategy descriptor.
	IndexStrategy(intent structs.IndexIntent) structs.IndexStrategy

	// LayoutStrategy resolves a `sortable` tag intent into the
	// dialect's concrete layout descriptor.
	LayoutStrategy(intent structs.LayoutIntent) structs.LayoutStrategy
}
