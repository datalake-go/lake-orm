package lakeorm

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datalake-go/lake-orm/types"
)

type genUser struct {
	ID    string  `spark:"id,pk"`
	Email string  `spark:"email,mergeKey"`
	Tier  *string `spark:"tier,nullable"`
}

// TestMigrateGenerate_WritesGooseFile covers the main flow:
// LakeSchema → migrate.Schema → Diff (treated as bootstrap in v0
// because the current-state read is still stubbed) → goose-format
// file on disk. We verify the file exists, has the expected goose
// annotations, and passes through the `-- dam:ack` rule for the
// nullable add.
//
// v0 note: because planLocalDiffs assumes a nil current state, a
// fresh struct produces a single OpCreateTable change — which
// Generate explicitly skips. To exercise the Add path, we construct
// the diff directly and call the migrate package; this test
// therefore verifies the file-writing plumbing, not the full
// DESCRIBE-TABLE pipeline that lands in v1.
func TestMigrateGenerate_BootstrapTableCreatesEmptyFile(t *testing.T) {
	dir := t.TempDir()
	c := &client{
		dialect: stubDialect{name: "iceberg"},
	}
	written, err := c.MigrateGenerate(context.Background(), dir, &genUser{})
	if err != nil {
		t.Fatalf("MigrateGenerate: %v", err)
	}
	// OpCreateTable gets skipped by Generate, so a fresh struct
	// produces a file with a header + empty +goose Up block. That
	// is expected in v0 until DESCRIBE TABLE is wired.
	if len(written) != 1 {
		t.Fatalf("expected 1 file, got %d", len(written))
	}
	body, err := os.ReadFile(written[0])
	if err != nil {
		t.Fatalf("read %s: %v", written[0], err)
	}
	bs := string(body)
	for _, want := range []string{
		"-- lakeorm: generated",
		"-- Struct fingerprint: sha256:",
		"-- Dialect: iceberg",
		"-- +goose Up",
		"-- +goose Down",
	} {
		if !strings.Contains(bs, want) {
			t.Errorf("missing %q in file:\n%s", want, bs)
		}
	}
	// Sanity: filename format = YYYYMMDDHHMMSS_<slug>.sql under dir.
	base := filepath.Base(written[0])
	if !strings.HasSuffix(base, ".sql") {
		t.Errorf("filename should end in .sql, got %s", base)
	}
	if !strings.Contains(base, "_gen_user") && !strings.Contains(base, "_genuser") {
		t.Errorf("filename should contain slug derived from table name; got %s", base)
	}
}

// stubDialect implements the subset of Dialect that MigrateGenerate
// exercises. Lives in this test file so the test doesn't need a real
// Iceberg/Delta instance.
type stubDialect struct{ name string }

func (s stubDialect) Name() string                               { return s.name }
func (s stubDialect) IndexStrategy(IndexIntent) IndexStrategy    { return "" }
func (s stubDialect) LayoutStrategy(LayoutIntent) LayoutStrategy { return "" }
func (s stubDialect) Maintenance() Maintenance                   { return nil }
func (s stubDialect) CreateTableDDL(*LakeSchema, types.Location) (string, error) {
	return "", nil
}
func (s stubDialect) AlterTableDDL(*LakeSchema, *TableInfo) ([]string, error) { return nil, nil }
func (s stubDialect) PlanQuery(QueryRequest) (ExecutionPlan, error)           { return ExecutionPlan{}, nil }
func (s stubDialect) PlanInsert(WriteRequest) (ExecutionPlan, error)          { return ExecutionPlan{}, nil }
func (s stubDialect) PlanUpsert(UpsertRequest) (ExecutionPlan, error)         { return ExecutionPlan{}, nil }
func (s stubDialect) PlanDelete(DeleteRequest) (ExecutionPlan, error)         { return ExecutionPlan{}, nil }

// TestMigrateGenerate_LegacyStateTriggersIngestIDAdd pins the
// Phase-3 migration path from issue #63: a table whose most recent
// migration file's State-JSON header predates the system-managed
// _ingest_id column (pre-Phase-1 of the system-column work) gets
// an `ALTER TABLE ... ADD COLUMN _ingest_id STRING` on the next
// MigrateGenerate run.
func TestMigrateGenerate_LegacyStateTriggersIngestIDAdd(t *testing.T) {
	dir := t.TempDir()
	// Write a legacy state file as if lake-orm produced it before the
	// system-column change landed. Hand-rolled JSON so the test
	// doesn't accidentally use the current EncodeState path (which
	// now always writes _ingest_id).
	legacyFile := filepath.Join(dir, "20260101000000_gen_users.sql")
	legacy := "-- lakeorm: generated 2026-01-01T00:00:00Z\n" +
		"-- Dialect: iceberg\n" +
		`-- State-JSON: {"table_name":"gen_users","fields":[` +
		`{"column":"id","sql_type":"STRING","nullable":false,"pk":true},` +
		`{"column":"email","sql_type":"STRING","nullable":false,"merge_key":true},` +
		`{"column":"tier","sql_type":"STRING","nullable":true}` +
		`]}` + "\n\n-- +goose Up\n-- +goose Down\n"
	if err := os.WriteFile(legacyFile, []byte(legacy), 0o644); err != nil {
		t.Fatalf("write legacy file: %v", err)
	}

	c := &client{dialect: stubDialect{name: "iceberg"}}
	written, err := c.MigrateGenerate(context.Background(), dir, &genUser{})
	if err != nil {
		t.Fatalf("MigrateGenerate: %v", err)
	}
	// MigrateGenerate writes one file per struct with changes. Pick the
	// one that isn't the legacy seed.
	var newFile string
	for _, f := range written {
		if filepath.Base(f) != filepath.Base(legacyFile) {
			newFile = f
			break
		}
	}
	if newFile == "" {
		t.Fatal("expected a new migration file distinct from the legacy seed")
	}
	body, err := os.ReadFile(newFile)
	if err != nil {
		t.Fatalf("read %s: %v", newFile, err)
	}
	bs := string(body)
	if !strings.Contains(bs, "ALTER TABLE gen_users ADD COLUMN _ingest_id") {
		t.Errorf("new migration should carry ADD COLUMN _ingest_id; got:\n%s", bs)
	}
	// And the regenerated State-JSON should now include _ingest_id so
	// subsequent runs stay quiet.
	if !strings.Contains(bs, `"column":"_ingest_id"`) {
		t.Errorf("new migration's State-JSON should include _ingest_id; got:\n%s", bs)
	}
}

func TestSlugifyTable(t *testing.T) {
	cases := map[string]string{
		"users":             "users",
		"my.db.users":       "my_db_users",
		"Weird Table Name!": "weird_table_name",
		"":                  "",
		"___":               "",
	}
	for in, want := range cases {
		if got := slugifyTable(in); got != want {
			t.Errorf("slugifyTable(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAssertSchema_StubPath(t *testing.T) {
	c := &client{dialect: stubDialect{name: "iceberg"}}
	if err := c.AssertSchema(context.Background()); err != nil {
		t.Errorf("empty input should be no-op, got %v", err)
	}
	if err := c.AssertSchema(context.Background(), &genUser{}); err == nil {
		t.Errorf("expected ErrNotImplemented for populated call in v0")
	}
}
