// Package delta is the Delta Lake Dialect. v0: stub with the SQL-shape
// pieces that mirror iceberg.Dialect (PlanQuery, PlanInsert basic
// routing) so the same typed queries work against a Delta lake. v1
// fleshes out deletion vectors, COPY INTO, OPTIMIZE/ZORDER.
package delta

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/internal/sqlbuild"
	"github.com/datalake-go/lake-orm/types"
)

type DialectOption func(*config)

type config struct {
	deletionVectors bool
	indexStrategy   map[string]structs.IndexStrategy
}

// ZOrder marks a column as contributing to Delta ZORDER on OPTIMIZE.
var ZOrder structs.IndexStrategy = "zorder"

func WithDeletionVectors(b bool) DialectOption {
	return func(c *config) { c.deletionVectors = b }
}

func WithIndexStrategy(col string, s structs.IndexStrategy) DialectOption {
	return func(c *config) {
		if c.indexStrategy == nil {
			c.indexStrategy = map[string]structs.IndexStrategy{}
		}
		c.indexStrategy[col] = s
	}
}

func Dialect(opts ...DialectOption) lakeorm.Dialect {
	cfg := &config{indexStrategy: map[string]structs.IndexStrategy{}}
	for _, o := range opts {
		o(cfg)
	}
	return &dialect{cfg: cfg}
}

type dialect struct{ cfg *config }

func (d *dialect) Name() string { return "delta" }

func (d *dialect) IndexStrategy(intent structs.IndexIntent) structs.IndexStrategy {
	switch intent {
	case structs.IntentIndexed, structs.IntentMergeKey:
		return ZOrder
	default:
		return ""
	}
}

func (d *dialect) LayoutStrategy(structs.LayoutIntent) structs.LayoutStrategy { return "" }

func (d *dialect) CreateTableDDL(schema *structs.LakeSchema, loc types.Location) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("delta: nil schema")
	}
	cols := make([]string, 0, len(schema.Fields)+1)
	for i := range schema.Fields {
		fl := schema.Fields[i]
		if fl.Ignored {
			continue
		}
		ty, err := goTypeToDelta(fl.Type)
		if err != nil {
			return "", fmt.Errorf("delta: column %s: %w", fl.Column, err)
		}
		col := fl.Column + " " + ty
		if !fl.IsNullable {
			col += " NOT NULL"
		}
		cols = append(cols, col)
	}
	// System-managed ingest_id column: every table lake-orm creates
	// carries this, stamped per-Insert at the driver layer. Nullable
	// so pre-existing tables can gain the column via ALTER TABLE
	// without a backfill. See types.SystemIngestIDColumn.
	cols = append(cols, types.SystemIngestIDColumn+" STRING")
	locationClause := ""
	if loc.URI() != "" {
		locationClause = fmt.Sprintf(" LOCATION '%s'", loc.URI())
	}
	tblProps := ""
	if d.cfg.deletionVectors {
		tblProps = " TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')"
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s) USING delta%s%s",
		schema.TableName, strings.Join(cols, ", "), locationClause, tblProps,
	), nil
}

// goTypeToDelta translates a Go reflect.Type to the Delta/Spark SQL
// equivalent. Spark SQL types are the same across Iceberg and Delta
// at the primitive level, but keeping a separate mapping here avoids
// importing across Dialect packages just for a helper; the v1 branch
// where the dialect-specific edges surface (Delta's GENERATED ALWAYS,
// Iceberg's TIMESTAMPTZ, etc.) can diverge without a cross-package
// change.
func goTypeToDelta(t reflect.Type) (string, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return "STRING", nil
	case reflect.Bool:
		return "BOOLEAN", nil
	case reflect.Int, reflect.Int64:
		return "BIGINT", nil
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return "INT", nil
	case reflect.Float32:
		return "FLOAT", nil
	case reflect.Float64:
		return "DOUBLE", nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "BINARY", nil
		}
		// Arrays come back through Spark as JSON strings in v0; real
		// ARRAY<T> emission lands when the lake tag grammar gains
		// element-type hints.
		return "STRING", nil
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP", nil
		}
		// Unknown structs serialize as JSON → STRING, matching the
		// reflection scanner's assignStringValue path.
		return "STRING", nil
	default:
		return "STRING", nil
	}
}

func (d *dialect) PlanInsert(req lakeorm.WriteRequest) (lakeorm.ExecutionPlan, error) {
	if req.ForcePath == lakeorm.WritePathGRPC ||
		(req.ForcePath == lakeorm.WritePathAuto && req.ApproxRowBytes < req.FastPathBytes) {
		return lakeorm.ExecutionPlan{
			Kind:     lakeorm.KindDirectIngest,
			IngestID: req.IngestID,
			Target:   req.Schema.TableName,
			Rows:     req.Records,
			Schema:   req.Schema,
		}, nil
	}
	if req.Backend == nil {
		return lakeorm.ExecutionPlan{}, fmt.Errorf("delta: fast-path requires a Backend")
	}
	kind := lakeorm.KindParquetIngest
	if len(req.Schema.MergeKeys) > 0 {
		kind = lakeorm.KindParquetMerge
	}
	prefix := req.Backend.StagingPrefix(req.IngestID)
	return lakeorm.ExecutionPlan{
		Kind:     kind,
		IngestID: req.IngestID,
		Target:   req.Schema.TableName,
		Schema:   req.Schema,
		Staging: lakeorm.StagingRef{
			Backend:  req.Backend,
			Prefix:   prefix,
			Location: req.Backend.StagingLocation(req.IngestID),
		},
	}, nil
}

func (d *dialect) PlanQuery(req lakeorm.QueryRequest) (lakeorm.ExecutionPlan, error) {
	cols := req.Columns
	if len(cols) == 0 {
		cols = req.Schema.ColumnNames()
	}
	table := req.Table
	if table == "" && req.Schema != nil {
		table = req.Schema.TableName
	}
	order := make([]sqlbuild.OrderSpec, 0, len(req.OrderBy))
	for _, o := range req.OrderBy {
		order = append(order, sqlbuild.OrderSpec{Column: o.Column, Desc: o.Desc})
	}
	sql, args := sqlbuild.Select{
		Columns: cols,
		Table:   table,
		Where:   req.Where,
		Args:    req.WhereArg,
		OrderBy: order,
		Limit:   req.Limit,
	}.Build()
	return lakeorm.ExecutionPlan{
		Kind:   lakeorm.KindStream,
		SQL:    sql,
		Args:   args,
		Schema: req.Schema,
	}, nil
}

