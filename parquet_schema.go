package lakeorm

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	pq "github.com/parquet-go/parquet-go"

	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/types"
)

// timeType is reflect.TypeOf(time.Time{}) cached so the schema
// synthesis doesn't call reflect.TypeOf per field.
var timeType = reflect.TypeOf(time.Time{})

// ParquetSchema is the bridge between a structs.LakeSchema (the user's
// lake-tagged model) and a parquet schema (what the fast-path writer
// serializes against). The whole point: users tag their structs with
// `spark:"..."` once; they do NOT need to also write `parquet:"..."`
// tags. The translation lives here.
//
// Internally it synthesizes an anonymous struct type at runtime whose
// parquet tags are derived from lake tags, then hands that type to
// parquet.SchemaOf. At write time, user rows are projected field-by-
// field into the synthesized struct.
//
// The synthesized struct always carries a trailing `_ingest_id`
// string field so the parquet output matches the target table DDL,
// which also includes that system-managed column. Convert stamps
// the per-operation ingest_id onto every row.
type ParquetSchema struct {
	lake           *structs.LakeSchema
	schema         *pq.Schema
	synthesized    reflect.Type
	projection     []parquetFieldProjection
	ingestIDTarget int // index of the _ingest_id field in the synthesized struct
}

type parquetFieldProjection struct {
	sourceIndex []int // path in the user's struct (handles embedded fields)
	targetIndex int   // flat field index in the synthesized struct
}

// BuildParquetSchema translates a structs.LakeSchema into a parquet schema +
// row projector. The returned ParquetSchema is cheap to call; the
// reflection work happens once and is reused per Write call.
//
// The fast-path writer (internal/parquet) wires this up automatically
// when Insert routes through object storage — users don't construct
// ParquetSchema directly.
func BuildParquetSchema(lake *structs.LakeSchema) (*ParquetSchema, error) {
	if lake == nil {
		return nil, fmt.Errorf("lakeorm: BuildParquetSchema requires a non-nil structs.LakeSchema")
	}

	fields := make([]reflect.StructField, 0, len(lake.Fields)+1)
	proj := make([]parquetFieldProjection, 0, len(lake.Fields))

	for _, f := range lake.Fields {
		if f.Ignored {
			continue
		}
		tag := buildParquetTag(f)
		fields = append(fields, reflect.StructField{
			Name: f.Name,
			Type: f.Type,
			Tag:  reflect.StructTag(fmt.Sprintf(`parquet:%q`, tag)),
		})
		proj = append(proj, parquetFieldProjection{
			sourceIndex: f.Index,
			targetIndex: len(proj),
		})
	}

	// System-managed _ingest_id column (see tag.go's
	// types.SystemIngestIDColumn). Appended last so the parquet output's
	// column order mirrors the target table DDL, which also emits
	// it last. Marked optional so NULL rows in existing tables
	// stay valid, but Convert always stamps the operation's UUIDv7
	// so new rows carry the filter predicate MERGE needs (#63
	// phase 2 follow-up).
	ingestIDIdx := len(fields)
	fields = append(fields, reflect.StructField{
		Name: "XIngestID", // synthetic — unexported-looking but exported for reflect
		Type: reflect.TypeOf(""),
		Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"%s,optional"`, types.SystemIngestIDColumn)),
	})

	synth := reflect.StructOf(fields)
	instance := reflect.New(synth).Interface()
	schema := pq.SchemaOf(instance)

	return &ParquetSchema{
		lake:           lake,
		schema:         schema,
		synthesized:    synth,
		projection:     proj,
		ingestIDTarget: ingestIDIdx,
	}, nil
}

// Schema returns the underlying parquet schema. Pass to
// parquet.NewWriter (or to the internal partition writer).
func (p *ParquetSchema) Schema() *pq.Schema { return p.schema }

// ConverterFor returns a RowConverter closure bound to ingestID.
// Every row the returned converter produces carries the same
// ingestID on its trailing _ingest_id field, so downstream SQL
// steps (INSERT ... SELECT, MERGE INTO ... WHERE _ingest_id = ?)
// can identify this batch in staging.
//
// Intended as the RowConverter handed to the internal partition
// writer — the writer's conversion happens once per row, so the
// closure captures ingestID without per-row allocation.
func (p *ParquetSchema) ConverterFor(ingestID string) func(any) any {
	return func(row any) any {
		source := reflect.ValueOf(row)
		for source.Kind() == reflect.Ptr {
			source = source.Elem()
		}
		target := reflect.New(p.synthesized).Elem()
		for _, pr := range p.projection {
			src := parquetFieldAt(source, pr.sourceIndex)
			target.Field(pr.targetIndex).Set(src)
		}
		target.Field(p.ingestIDTarget).SetString(ingestID)
		return target.Interface()
	}
}

// buildParquetTag renders the parquet-go struct-tag string for a
// structs.LakeField. Translation rules (extend here when v1 adds a new
// lake tag modifier or column kind):
//
//   - Column name comes from the lake tag (or snake_case field name).
//   - time.Time → `timestamp(microsecond)`. parquet-go defaults to
//     TIMESTAMP(NANOS); Spark, Iceberg, Delta, DuckDB, and Polars all
//     want MICROS. Fixed here once so user structs never set it.
//   - Pointer or `nullable` → `optional`. parquet-go's default is
//     `required` for non-pointer fields; explicit `optional` lets
//     rows carry NULLs without allocating pointer wrappers.
func buildParquetTag(f structs.LakeField) string {
	parts := []string{f.Column}

	t := f.Type
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t == timeType {
		parts = append(parts, "timestamp(microsecond)")
	}

	if f.IsPointer || f.IsNullable {
		parts = append(parts, "optional")
	}

	return strings.Join(parts, ",")
}

// parquetFieldAt navigates an index path, dereferencing pointers on
// the way down. structs.LakeField.Index handles embedded fields; this helper
// walks that path without allocating.
func parquetFieldAt(v reflect.Value, index []int) reflect.Value {
	cur := v
	for _, i := range index {
		for cur.Kind() == reflect.Ptr {
			cur = cur.Elem()
		}
		cur = cur.Field(i)
	}
	return cur
}
