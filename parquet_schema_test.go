package lakeorm

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/datalake-go/lake-orm/types"
)

// pqTestUser covers every translation rule the synthesizer is
// expected to apply: primary key, merge key, nullable pointer
// field, time.Time → TIMESTAMP(MICROS), ignored field.
type pqTestUser struct {
	ID        types.SortableID `spark:"id,pk"`
	Email     string           `spark:"email,mergeKey"`
	Country   string           `spark:"country"`
	CreatedAt time.Time        `spark:"created_at,auto=createTime"`
	DeletedAt *time.Time       `spark:"deleted_at,nullable"`
	Internal  string           `spark:"-"`
}

// TestBuildParquetSchema_SurfaceShape pins the visible columns the
// schema synthesizer produces. If this test ever fails, the
// dorm→parquet translation is dropping or adding a column that user
// code doesn't expect.
func TestBuildParquetSchema_SurfaceShape(t *testing.T) {
	lake, err := ParseSchema(reflect.TypeOf(pqTestUser{}))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	ps, err := BuildParquetSchema(lake)
	if err != nil {
		t.Fatalf("BuildParquetSchema: %v", err)
	}

	// User columns first, then the system-managed _ingest_id column
	// appended after user fields. Order matters — the SELECT * at
	// Spark's INSERT step assumes the staging column order matches
	// the target-table DDL order.
	want := []string{"id", "email", "country", "created_at", "deleted_at", SystemIngestIDColumn}
	got := columnNamesOf(ps)
	if len(got) != len(want) {
		t.Fatalf("got %d columns (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for i, c := range want {
		if got[i] != c {
			t.Errorf("column %d: got %q, want %q", i, got[i], c)
		}
	}
	// Explicitly: the ignored field must not surface.
	for _, c := range got {
		if c == "internal" {
			t.Errorf("ignored field leaked into parquet schema: %v", got)
		}
	}
}

// TestBuildParquetSchema_TimestampTag pins the TIMESTAMP(MICROS)
// translation. This is the rule that stopped users having to write
// `parquet:"...,timestamp(microsecond)"` by hand — if the tag ever
// stops being emitted, Spark starts rejecting reads again.
func TestBuildParquetSchema_TimestampTag(t *testing.T) {
	lake, _ := ParseSchema(reflect.TypeOf(pqTestUser{}))

	for _, f := range lake.Fields {
		if f.Ignored {
			continue
		}
		tag := buildParquetTag(f)
		isTimestamp := f.Type == timeType ||
			(f.Type.Kind() == reflect.Ptr && f.Type.Elem() == timeType)
		hasMicros := strings.Contains(tag, "timestamp(microsecond)")
		if isTimestamp && !hasMicros {
			t.Errorf("field %s has time.Time but tag %q is missing timestamp(microsecond)", f.Name, tag)
		}
		if !isTimestamp && hasMicros {
			t.Errorf("field %s is not time.Time but tag %q includes timestamp(microsecond)", f.Name, tag)
		}
	}
}

// TestBuildParquetSchema_OptionalForNullable pins the `optional`
// translation. parquet-go's default is `required`, which blows up
// NULL writes.
func TestBuildParquetSchema_OptionalForNullable(t *testing.T) {
	lake, _ := ParseSchema(reflect.TypeOf(pqTestUser{}))
	byName := map[string]LakeField{}
	for _, f := range lake.Fields {
		byName[f.Name] = f
	}

	tag := buildParquetTag(byName["DeletedAt"])
	if !strings.Contains(tag, "optional") {
		t.Errorf("pointer field DeletedAt tag = %q, missing optional", tag)
	}

	tag = buildParquetTag(byName["Email"])
	if strings.Contains(tag, "optional") {
		t.Errorf("required field Email tag = %q, should not be optional", tag)
	}
}

// TestBuildParquetSchema_ConverterFor_Projects pins that the
// converter closure copies user-struct values into the synthesized
// struct in the same order the schema expects — visible via the
// synthesized type's field names — and stamps the per-operation
// ingest_id onto the trailing XIngestID field.
func TestBuildParquetSchema_ConverterFor_Projects(t *testing.T) {
	lake, _ := ParseSchema(reflect.TypeOf(pqTestUser{}))
	ps, _ := BuildParquetSchema(lake)

	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	in := &pqTestUser{
		ID:        "3CSLfoj0JdmsnLNW3LCMi0596TW",
		Email:     "alice@example.com",
		Country:   "UK",
		CreatedAt: when,
	}
	const ingestID = "0193abc-test"
	out := ps.ConverterFor(ingestID)(in)
	v := reflect.ValueOf(out)
	if v.Kind() != reflect.Struct {
		t.Fatalf("ConverterFor returned %v, want a struct", v.Kind())
	}
	// Synthesized struct preserves the Go field names, so we can read
	// values by name.
	if got := v.FieldByName("Email").String(); got != "alice@example.com" {
		t.Errorf("Email projected = %q, want alice@example.com", got)
	}
	if got := v.FieldByName("Country").String(); got != "UK" {
		t.Errorf("Country projected = %q, want UK", got)
	}
	if got := v.FieldByName("CreatedAt").Interface().(time.Time); !got.Equal(when) {
		t.Errorf("CreatedAt projected = %v, want %v", got, when)
	}
	// System-managed XIngestID should carry the ingest_id.
	if got := v.FieldByName("XIngestID").String(); got != ingestID {
		t.Errorf("XIngestID = %q, want %q", got, ingestID)
	}
	// Ignored field should not exist on the synthesized struct.
	if f := v.FieldByName("Internal"); f.IsValid() {
		t.Errorf("Internal field leaked into synthesized struct")
	}
}

// TestBuildParquetSchema_NilLakeErrors pins defensive behavior:
// passing nil in is a programming error, surfaced as a real error
// rather than a panic.
func TestBuildParquetSchema_NilLakeErrors(t *testing.T) {
	_, err := BuildParquetSchema(nil)
	if err == nil {
		t.Fatal("BuildParquetSchema(nil) should error")
	}
}

// columnNamesOf returns the ordered column names of a parquet schema,
// read directly off the compiled schema so the test validates what
// the writer will emit.
func columnNamesOf(ps *ParquetSchema) []string {
	fields := ps.Schema().Fields()
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		out = append(out, f.Name())
	}
	return out
}
