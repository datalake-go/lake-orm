package duckdb_test

import (
	"context"
	"testing"
	"time"

	lakeorm "github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backends"
	ddialect "github.com/datalake-go/lake-orm/dialects/duckdb"
	ddriver "github.com/datalake-go/lake-orm/drivers/duckdb"
	"github.com/datalake-go/lake-orm/testutils"
	"github.com/datalake-go/lake-orm/types"
)

type user struct {
	ID        types.SortableID `lake:"id,pk"`
	Email     string           `lake:"email"`
	Country   string           `lake:"country"`
	CreatedAt time.Time        `lake:"created_at"`
}

// openEmbedded stands up the full lakeorm stack against embedded
// DuckDB + an in-memory object-storage backend. Zero external
// dependencies; <1s per test.
func openEmbedded(t *testing.T) lakeorm.Client {
	t.Helper()
	db := testutils.DuckDB(t)
	store := backends.Memory("duckdb-driver-test")
	client, err := lakeorm.Open(ddriver.Driver(db), ddialect.Dialect(), store)
	if err != nil {
		t.Fatalf("lakeorm.Open: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestDriver_MigrateInsertQueryRoundTrip(t *testing.T) {
	client := openEmbedded(t)
	ctx := context.Background()

	if err := client.Migrate(ctx, &user{}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	f := testutils.NewFactory(t)
	want := []*user{
		{ID: types.NewSortableID(), Email: "alice@example.com", Country: "UK", CreatedAt: f.Now()},
		{ID: types.NewSortableID(), Email: "bob@example.com", Country: "UK", CreatedAt: f.Now()},
		{ID: types.NewSortableID(), Email: "carol@example.com", Country: "US", CreatedAt: f.Now()},
	}
	if err := client.Insert(ctx, want); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := lakeorm.Query[user](ctx, client,
		`SELECT id, email, country, created_at FROM users WHERE country = ? ORDER BY email`, "UK")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Query returned %d rows, want 2", len(got))
	}
	if got[0].Email != "alice@example.com" || got[1].Email != "bob@example.com" {
		t.Errorf("emails = [%s, %s], want [alice@example.com, bob@example.com]",
			got[0].Email, got[1].Email)
	}
}

func TestDriver_DataFrameCount(t *testing.T) {
	client := openEmbedded(t)
	ctx := context.Background()

	if err := client.Migrate(ctx, &user{}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	f := testutils.NewFactory(t)
	if err := client.Insert(ctx, []*user{
		{ID: types.NewSortableID(), Email: "a@x", Country: "UK", CreatedAt: f.Now()},
		{ID: types.NewSortableID(), Email: "b@x", Country: "US", CreatedAt: f.Now()},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	df, err := client.DataFrame(ctx, `SELECT country, COUNT(*) AS n FROM users GROUP BY country`)
	if err != nil {
		t.Fatalf("DataFrame: %v", err)
	}
	n, err := df.Count(ctx)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 2 {
		t.Errorf("Count = %d, want 2", n)
	}
}

// TestDriver_StampsIngestIDOnEveryRow pins the Phase-2 invariant
// that every row lake-orm writes carries a non-empty UUIDv7 in the
// system-managed _ingest_id column. Without this, MERGE (Phase 2b)
// can't scope its source to a single batch.
func TestDriver_StampsIngestIDOnEveryRow(t *testing.T) {
	client := openEmbedded(t)
	ctx := context.Background()

	if err := client.Migrate(ctx, &user{}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := client.Insert(ctx, []*user{
		{ID: types.NewSortableID(), Email: "a@x", Country: "UK", CreatedAt: time.Now().Truncate(time.Microsecond)},
		{ID: types.NewSortableID(), Email: "b@x", Country: "US", CreatedAt: time.Now().Truncate(time.Microsecond)},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	type ingestRow struct {
		IngestID string `lake:"_ingest_id"`
	}
	rows, err := lakeorm.Query[ingestRow](ctx, client, `SELECT _ingest_id FROM users`)
	if err != nil {
		t.Fatalf("Query _ingest_id: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0].IngestID == "" {
		t.Error("row 0 _ingest_id is empty; expected a UUIDv7")
	}
	// Both rows from the same Insert share one ingest_id.
	if rows[0].IngestID != rows[1].IngestID {
		t.Errorf("rows from the same Insert should share ingest_id, got %q vs %q",
			rows[0].IngestID, rows[1].IngestID)
	}
}

func TestDriver_EmptyInsertIsNoop(t *testing.T) {
	client := openEmbedded(t)
	ctx := context.Background()

	if err := client.Migrate(ctx, &user{}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := client.Insert(ctx, []*user{}); err != nil {
		t.Errorf("empty Insert should be a no-op, got: %v", err)
	}
}
