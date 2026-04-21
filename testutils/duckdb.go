package testutils

import (
	"database/sql"
	"testing"

	// Register the "duckdb" sql driver for sql.Open.
	_ "github.com/marcboeker/go-duckdb/v2"
)

// DuckDB opens an in-memory DuckDB *sql.DB and registers a
// t.Cleanup that closes it at test end. No file paths, no network,
// no external services — the cheapest real engine available.
//
// Use this anywhere a test needs a running SQL database to exercise
// the full driver/dialect/backend stack without standing up
// docker-compose.
//
// CGO note: go-duckdb links against the DuckDB C++ library, so
// tests that depend on this helper require a cgo-enabled toolchain.
// Pre-built binaries ship for linux-amd64/arm64, darwin-amd64/arm64,
// and windows-amd64; CI on those platforms works out of the box.
func DuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("testutils.DuckDB: open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
