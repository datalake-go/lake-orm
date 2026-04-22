package duckdb_test

import (
	"errors"
	"reflect"
	"testing"

	lakeorm "github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/dialects/duckdb"
	"github.com/datalake-go/lake-orm/types"
)

// bookUpsert declares title as a mergeKey — the duckdb dialect
// should reject this because upsert isn't wired on the embedded
// direct-ingest path.
type bookUpsert struct {
	ID    types.SortableID `lake:"id,pk"`
	Title string           `lake:"title,mergeKey"`
}

// TestPlanInsert_RejectsMergeKey pins that duckdb's dialect errors
// with ErrNotImplemented when the schema declares a mergeKey, so
// users don't get silent append when they expected upsert.
func TestPlanInsert_RejectsMergeKey(t *testing.T) {
	d := duckdb.Dialect()
	schema, err := structs.ParseSchema(reflect.TypeOf(bookUpsert{}))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	_, err = d.PlanInsert(lakeorm.WriteRequest{Schema: schema, IngestID: "abc"})
	if err == nil {
		t.Fatal("PlanInsert with mergeKey should error; duckdb has no upsert in v0")
	}
	if !errors.Is(err, lakeorm.ErrNotImplemented) {
		t.Errorf("err = %v, want wraps ErrNotImplemented", err)
	}
}
