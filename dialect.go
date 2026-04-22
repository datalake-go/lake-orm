package lakeorm

import (
	"context"

	"github.com/datalake-go/lake-orm/types"
)

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

// TableStats is Dialect-level physical metadata for a table.
type TableStats struct {
	NumRows       int64
	NumDataFiles  int64
	TotalBytes    int64
	SnapshotCount int
}

// TableInfo is the shape of the catalog's current view of a table,
// handed to Dialect.AlterTableDDL for migration planning.
type TableInfo struct {
	Name       string
	Location   types.Location
	Columns    []ColumnInfo
	Partitions []string
	SnapshotID string
}

// ColumnInfo is one column in a TableInfo.
type ColumnInfo struct {
	Name     string
	DataType string
	Nullable bool
}
