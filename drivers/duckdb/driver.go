// Package duckdb is lake-orm's embedded DuckDB driver. It takes a
// *sql.DB constructed against go-duckdb and implements the
// lakeorm.Driver interface by translating between lake-tagged Go
// structs and DuckDB SQL rows.
//
// This is the "no network, no JVM, no container" path — useful for:
//
//   - Unit + integration tests that want a real engine without
//     Docker or k8s.
//   - Local development iteration faster than docker-compose up.
//   - Single-process analytics workloads that fit on one box.
//
// What this driver is NOT:
//
//   - A distributed engine. DuckDB is embedded single-process.
//   - Feature-matched with Spark. Features like Iceberg merge-on-
//     read, Delta deletion vectors, Hive-style writes depend on
//     DuckDB's first-party extensions; this driver only relies on
//     DuckDB's core SQL + parquet support by default.
//
// CGO requirement. go-duckdb links against DuckDB's C++ library
// via CGO. Builds with `CGO_ENABLED=0` will fail. Pre-compiled
// DuckDB binaries cover linux-amd64 / linux-arm64 / darwin-amd64
// / darwin-arm64 / windows-amd64 out of the box; exotic targets
// (distroless without CGO, some musl variants) may need custom
// builds.
//
// Driver family (four now):
//
//   - drivers/spark             — generic Spark Connect
//   - drivers/databricksconnect — Databricks Connect (OAuth M2M)
//   - drivers/databricks        — Databricks native (BYO *sql.DB)
//   - drivers/duckdb            — embedded DuckDB (this package)
//
// All four implement lakeorm.Driver + drivers.Convertible. ORM code
// (Insert, Query[T], Migrate) stays portable across them.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	lakeorm "github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/types"
	"github.com/jmoiron/sqlx/reflectx"
)

// New returns a *Driver backed by the supplied *sql.DB.
// The *sql.DB is owned by the caller: Driver.Close does NOT close
// it. This matches the database/sql lifecycle most consumers
// already implement.
//
// Typical construction (embedded in-memory DuckDB):
//
//	import _ "github.com/marcboeker/go-duckdb/v2"
//	sqlDB, _ := sql.Open("duckdb", "")
//	drv := duckdb.New(sqlDB)
//	client, _ := lakeorm.Open(drv, dialect, store)
//
// For a persistent file-backed database pass a path:
//
//	sql.Open("duckdb", "analytics.ddb")
func New(db *sql.DB, opts ...Option) *Driver {
	cfg := &config{name: "duckdb"}
	for _, opt := range opts {
		opt(cfg)
	}
	return &Driver{
		name: cfg.name,
		db:   db,
	}
}

// Option tunes the driver at construction. Functional so the
// surface stays forward-compatible.
type Option func(*config)

// WithName overrides the Driver.Name() string. Defaults to "duckdb";
// set for discriminating multiple DuckDB driver instances in logs
// / metrics.
func WithName(name string) Option {
	return func(c *config) {
		if name != "" {
			c.name = name
		}
	}
}

type config struct {
	name string
}

// Driver is lake-orm's DuckDB driver. Exported so callers can reach
// the per-driver conversion helpers (FromSQL, FromRows, FromTable,
// FromRow) and the raw *sql.DB (DB) via a Client.Driver()
// type-assertion.
type Driver struct {
	name string
	db   *sql.DB
}

// DB returns the underlying *sql.DB. Escape hatch for callers who
// need to drop to raw database/sql operations the lakeorm.Driver
// interface doesn't expose (PrepareContext, transactions,
// `INSTALL iceberg;`-style extension setup, etc.).
func (d *Driver) DB() *sql.DB { return d.db }

// Name implements lakeorm.Driver.
func (d *Driver) Name() string { return d.name }

// Close implements lakeorm.Driver. The *sql.DB is owned by the
// caller and is NOT closed here — they constructed it, they close
// it on their own shutdown path.
func (d *Driver) Close() error { return nil }

// Execute implements lakeorm.Driver.
//
// KindDDL / KindSQL route through (*sql.DB).ExecContext.
// KindDirectIngest walks plan.Rows via reflection and fires one
// prepared INSERT per row — fine for an embedded engine where
// there's no network round-trip per statement. KindParquetIngest
// is not reachable from the duckdb Dialect (which never routes to
// it); if another dialect emits one at this driver it errors.
func (d *Driver) Execute(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
	switch plan.Kind {
	case lakeorm.KindDDL, lakeorm.KindSQL:
		if _, err := d.db.ExecContext(ctx, plan.SQL, plan.Args...); err != nil {
			return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("duckdb: exec: %w", err)
		}
		return lakeorm.Result{}, nopFinalizer{}, nil
	case lakeorm.KindDirectIngest:
		return d.executeDirectIngest(ctx, plan)
	default:
		return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("duckdb: plan kind %d not supported", plan.Kind)
	}
}

// executeDirectIngest walks plan.Rows (a typed slice, typically
// []*T) and inserts each record. Values are pulled via reflection
// against plan.Schema's field order; SortableID and other
// sql.Valuer types pass through database/sql's normal conversion.
func (d *Driver) executeDirectIngest(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
	if plan.Schema == nil {
		return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("duckdb: direct ingest requires a schema")
	}
	if plan.Target == "" {
		return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("duckdb: direct ingest requires a target")
	}
	rv := reflect.ValueOf(plan.Rows)
	if rv.Kind() != reflect.Slice {
		return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("duckdb: direct ingest expects a slice, got %v", rv.Kind())
	}
	if rv.Len() == 0 {
		return lakeorm.Result{}, nopFinalizer{}, nil
	}

	cols := make([]string, 0, len(plan.Schema.Fields)+1)
	indices := make([][]int, 0, len(plan.Schema.Fields))
	for i := range plan.Schema.Fields {
		f := plan.Schema.Fields[i]
		if f.Ignored {
			continue
		}
		cols = append(cols, f.Column)
		indices = append(indices, f.Index)
	}
	// System-managed _ingest_id column: appended after user columns
	// so the INSERT column list mirrors the CREATE TABLE DDL. Value
	// comes from plan.IngestID (generated in Client.Insert).
	cols = append(cols, types.SystemIngestIDColumn)
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", ")
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		plan.Target, strings.Join(cols, ", "), placeholders)

	stmt, err := d.db.PrepareContext(ctx, insertSQL)
	if err != nil {
		return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("duckdb: prepare insert: %w", err)
	}
	defer stmt.Close()

	args := make([]any, len(indices)+1)
	args[len(indices)] = plan.IngestID
	for i := 0; i < rv.Len(); i++ {
		row := reflect.Indirect(rv.Index(i))
		for j, idx := range indices {
			args[j] = reflectx.FieldByIndexes(row, idx).Interface()
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("duckdb: insert row %d: %w", i, err)
		}
	}
	return lakeorm.Result{}, nopFinalizer{}, nil
}

// Exec implements lakeorm.Driver. Plain fire-and-forget exec.
func (d *Driver) Exec(ctx context.Context, sqlStr string, args ...any) (lakeorm.ExecResult, error) {
	res, err := d.db.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		return lakeorm.ExecResult{}, fmt.Errorf("duckdb: exec: %w", err)
	}
	var affected int64 = -1
	if n, nerr := res.RowsAffected(); nerr == nil {
		affected = n
	}
	return lakeorm.ExecResult{RowsAffected: affected}, nil
}

// nopFinalizer satisfies lakeorm.Finalizer for single-phase plans.
type nopFinalizer struct{}

func (nopFinalizer) Commit(context.Context) error { return nil }
func (nopFinalizer) Abort(context.Context) error  { return nil }
