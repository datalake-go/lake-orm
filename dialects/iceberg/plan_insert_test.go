package iceberg_test

import (
	"reflect"
	"testing"

	lakeorm "github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/dialects/iceberg"
	"github.com/datalake-go/lake-orm/testutils"
	"github.com/datalake-go/lake-orm/types"
)

// bookAppend has no mergeKey — declares pure append intent.
type bookAppend struct {
	ID    types.SortableID `lake:"id,pk"`
	Title string           `lake:"title"`
}

// bookUpsert declares title as a mergeKey — upsert intent.
type bookUpsert struct {
	ID    types.SortableID `lake:"id,pk"`
	Title string           `lake:"title,mergeKey"`
}

// fakeBackend is a minimal Backend for PlanInsert routing tests.
// Only the methods PlanInsert's fast-path branch touches are
// implemented; everything else would panic on reach, which is the
// correct signal that routing went wrong.
type fakeBackend struct{ lakeorm.Backend }

func (fakeBackend) StagingPrefix(ingestID string) string  { return "s3://b/staging/" + ingestID }
func (fakeBackend) StagingLocation(string) types.Location { return types.Location{} }

// TestPlanInsert_NoMergeKeyRoutesToKindParquetIngest pins the
// append path — pure-insert structs stay on the append SQL.
func TestPlanInsert_NoMergeKeyRoutesToKindParquetIngest(t *testing.T) {
	d := iceberg.Dialect()
	schema, err := lakeorm.ParseSchema(reflect.TypeOf(bookAppend{}))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}

	f := testutils.NewFactory(t)
	plan, err := d.PlanInsert(lakeorm.WriteRequest{
		Schema:         schema,
		IngestID:       f.IngestID.String(),
		RecordCount:    3,
		ApproxRowBytes: 1 << 20, // above fast-path threshold
		FastPathBytes:  1 << 10,
		Backend:        fakeBackend{},
	})
	if err != nil {
		t.Fatalf("PlanInsert: %v", err)
	}
	if plan.Kind != lakeorm.KindParquetIngest {
		t.Errorf("plan.Kind = %v, want KindParquetIngest", plan.Kind)
	}
}

// TestPlanInsert_WithMergeKeyRoutesToKindParquetMerge pins the
// upsert path — structs declaring a mergeKey flow into MERGE INTO.
func TestPlanInsert_WithMergeKeyRoutesToKindParquetMerge(t *testing.T) {
	d := iceberg.Dialect()
	schema, err := lakeorm.ParseSchema(reflect.TypeOf(bookUpsert{}))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}

	f := testutils.NewFactory(t)
	plan, err := d.PlanInsert(lakeorm.WriteRequest{
		Schema:         schema,
		IngestID:       f.IngestID.String(),
		RecordCount:    3,
		ApproxRowBytes: 1 << 20,
		FastPathBytes:  1 << 10,
		Backend:        fakeBackend{},
	})
	if err != nil {
		t.Fatalf("PlanInsert: %v", err)
	}
	if plan.Kind != lakeorm.KindParquetMerge {
		t.Errorf("plan.Kind = %v, want KindParquetMerge", plan.Kind)
	}
	// IngestID must be threaded through so the driver's MERGE SQL
	// can bind it into the source filter.
	if plan.IngestID != f.IngestID.String() {
		t.Errorf("plan.IngestID = %q, want %q", plan.IngestID, f.IngestID.String())
	}
}

// TestPlanInsert_MergeKeyOnDirectIngestPathStaysDirect pins that
// the mergeKey routing only applies to the fast path. When the
// caller forces the direct-ingest route (small batch), upsert
// semantics aren't wired through yet and the plan stays as
// KindDirectIngest. Phase 3+ may revisit; for now, explicit
// non-surprise.
func TestPlanInsert_MergeKeyOnDirectIngestPathStaysDirect(t *testing.T) {
	d := iceberg.Dialect()
	schema, _ := lakeorm.ParseSchema(reflect.TypeOf(bookUpsert{}))
	f := testutils.NewFactory(t)
	plan, err := d.PlanInsert(lakeorm.WriteRequest{
		Schema:         schema,
		IngestID:       f.IngestID.String(),
		RecordCount:    1,
		ApproxRowBytes: 100,
		FastPathBytes:  1 << 20, // forces direct
		Backend:        fakeBackend{},
	})
	if err != nil {
		t.Fatalf("PlanInsert: %v", err)
	}
	if plan.Kind != lakeorm.KindDirectIngest {
		t.Errorf("plan.Kind = %v, want KindDirectIngest", plan.Kind)
	}
}
