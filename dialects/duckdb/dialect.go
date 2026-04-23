// Package duckdb is the lake-orm Dialect for embedded DuckDB. It
// emits plain DuckDB SQL — no Iceberg / Delta "USING <format>"
// clauses, no LOCATION override, no dialect-specific merge. Tables
// are native DuckDB tables stored in the engine's own catalog (in-
// memory when the connection string is empty, on-disk when it
// points at a .ddb file).
//
// Pair this with driver/duckdb for a zero-network, zero-JVM path
// suitable for dev, tests, and single-process analytics workloads.
// For cross-engine portability (Spark, Trino reads) against the
// same tables, stick with iceberg.Dialect or delta.Dialect and the
// Spark-family drivers.
//
// Scope of v0:
//
//   - CREATE TABLE IF NOT EXISTS with primitive column types.
//   - INSERT via the Spark-family fast-path is a non-fit: DuckDB
//     has no COPY-INTO-from-staging analog the same shape, so the
//     Dialect routes WritePathAuto + WritePathGRPC to the direct
//     ingest plan. The driver emits one bulk-prepared INSERT.
//   - PlanQuery builds standard SELECT ... WHERE ... LIMIT.
//   - PlanDelete builds DELETE.
//   - Upsert, Optimize, Vacuum, Stats stay stubbed behind
//     ErrNotImplemented for v0.
package duckdb

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	lakeorm "github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/types"
	lkerrors "github.com/datalake-go/lake-orm/errors"
)

// DialectOption tunes the DuckDB Dialect. Reserved for future knobs
// (schema qualifier, extension preloads).
type DialectOption func(*config)

type config struct {
	// schema qualifies table references as "<schema>.<table>" when
	// non-empty. Defaults to "" so references stay unqualified and
	// DuckDB's current schema (usually `main`) applies.
	schema string
}

// WithSchema sets the schema name tables are qualified under. Maps
// to DuckDB's SCHEMA concept; default is to leave table names
// unqualified.
func WithSchema(name string) DialectOption {
	return func(c *config) { c.schema = name }
}

// Dialect returns a lakeorm.Dialect configured for DuckDB.
func Dialect(opts ...DialectOption) lakeorm.Dialect {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}
	return &dialect{cfg: cfg}
}

type dialect struct{ cfg *config }

func (d *dialect) Name() string { return "duckdb" }

// IndexStrategy returns an empty strategy — DuckDB has ART indexes
// on primary keys but lake-orm's Intent hooks don't map to
// anything we need to emit here in v0.
func (d *dialect) IndexStrategy(structs.IndexIntent) structs.IndexStrategy {
	return ""
}

func (d *dialect) LayoutStrategy(structs.LayoutIntent) structs.LayoutStrategy { return "" }

func (d *dialect) qualified(table string) string {
	if d.cfg.schema != "" {
		return d.cfg.schema + "." + table
	}
	return table
}

// CreateTableDDL builds a plain CREATE TABLE statement. The
// `loc types.Location` parameter exists in the Dialect interface
// to carry a warehouse-root URI for lake-file-format engines; it's
// unused here because DuckDB keeps tables in its own catalog.
func (d *dialect) CreateTableDDL(schema *structs.LakeSchema, _ types.Location) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("duckdb: nil schema")
	}
	cols := make([]string, 0, len(schema.Fields)+1)
	for i := range schema.Fields {
		f := schema.Fields[i]
		if f.Ignored {
			continue
		}
		ty, err := goTypeToDuckDB(f.Type)
		if err != nil {
			return "", fmt.Errorf("duckdb: column %s: %w", f.Column, err)
		}
		col := f.Column + " " + ty
		if !f.IsNullable {
			col += " NOT NULL"
		}
		cols = append(cols, col)
	}
	// System-managed ingest_id column: every table lake-orm creates
	// carries this, stamped per-Insert at the driver layer. Nullable
	// so pre-existing tables can gain the column via ALTER TABLE
	// without a backfill. See types.SystemIngestIDColumn.
	cols = append(cols, types.SystemIngestIDColumn+" VARCHAR")
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s)",
		d.qualified(schema.TableName), strings.Join(cols, ", "),
	), nil
}

// goTypeToDuckDB maps Go reflect.Type to DuckDB's SQL type names.
// DuckDB's type system is standard-SQL-leaning with a few richer
// types (HUGEINT, DECIMAL(18,3), LIST) we don't surface here yet.
func goTypeToDuckDB(t reflect.Type) (string, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return "VARCHAR", nil
	case reflect.Bool:
		return "BOOLEAN", nil
	case reflect.Int, reflect.Int64:
		return "BIGINT", nil
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return "INTEGER", nil
	case reflect.Float32:
		return "REAL", nil
	case reflect.Float64:
		return "DOUBLE", nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "BLOB", nil
		}
		// Slice-of-struct / slice-of-primitive: land as JSON strings
		// for now; matches the reflection scanner's JSON-decode
		// path. Real LIST<T> emission lands with schema-elem hints.
		return "VARCHAR", nil
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP", nil
		}
		return "VARCHAR", nil
	default:
		return "VARCHAR", nil
	}
}

// PlanInsert routes to KindDirectIngest regardless of batch size.
// DuckDB has no multi-file COPY-INTO-from-staging idiom the
// object-storage fast path expects; bulk inserts go straight
// through the driver's prepared-statement loop.
func (d *dialect) PlanInsert(req lakeorm.WriteRequest) (lakeorm.ExecutionPlan, error) {
	// DuckDB's v0 dialect handles pure-append semantics only.
	// Structs that declare a mergeKey want upsert (MERGE INTO
	// target ON mergeKey = mergeKey ...), which the fast-path
	// family (iceberg / delta) handles via KindParquetMerge but
	// the direct-ingest embedded path doesn't yet. Erroring here
	// is better than silently appending — the user thinks they're
	// upserting and gets duplicates instead.
	if len(req.Schema.MergeKeys) > 0 {
		return lakeorm.ExecutionPlan{}, fmt.Errorf(
			"duckdb: upsert (via mergeKey) is not yet implemented for the embedded dialect; "+
				"use iceberg / delta + driver/spark for upsert, or drop the mergeKey tag: %w",
			lkerrors.ErrNotImplemented)
	}
	return lakeorm.ExecutionPlan{
		Kind:     lakeorm.KindDirectIngest,
		IngestID: req.IngestID,
		Target:   d.qualified(req.Schema.TableName),
		Rows:     req.Records,
		Schema:   req.Schema,
	}, nil
}

