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
//   - driver/spark             — generic Spark Connect
//   - driver/databricksconnect — Databricks Connect (OAuth M2M)
//   - driver/databricks        — Databricks native (BYO *sql.DB)
//   - driver/duckdb            — embedded DuckDB (this package)
//
// All four implement the same lakeorm.Driver interface. ORM code
// (Query[T], Insert, Migrate, DataFrame.Collect) stays portable.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"reflect"
	"strings"

	lakeorm "github.com/datalake-go/lake-orm"
	"github.com/jmoiron/sqlx/reflectx"
)

// Driver returns a lakeorm.Driver backed by the supplied *sql.DB.
// The *sql.DB is owned by the caller: Driver.Close does NOT close
// it. This matches the database/sql lifecycle most consumers
// already implement.
//
// Typical construction (embedded in-memory DuckDB):
//
//	import _ "github.com/marcboeker/go-duckdb/v2"
//	db, _ := sql.Open("duckdb", "")
//	drv := duckdb.Driver(db)
//	client, _ := lakeorm.Open(drv, dialect, store)
//
// For a persistent file-backed database pass a path:
//
//	sql.Open("duckdb", "analytics.ddb")
func Driver(db *sql.DB, opts ...Option) lakeorm.Driver {
	cfg := &config{name: "duckdb"}
	for _, opt := range opts {
		opt(cfg)
	}
	return &driver{
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

type driver struct {
	name string
	db   *sql.DB
}

// DB returns the underlying *sql.DB. Escape hatch for callers who
// need to drop to raw database/sql operations the lakeorm.Driver
// interface doesn't expose (PrepareContext, transactions,
// `INSTALL iceberg;`-style extension setup, etc.).
func (d *driver) DB() *sql.DB { return d.db }

// Name implements lakeorm.Driver.
func (d *driver) Name() string { return d.name }

// Close implements lakeorm.Driver. The *sql.DB is owned by the
// caller and is NOT closed here — they constructed it, they close
// it on their own shutdown path.
func (d *driver) Close() error { return nil }

// Execute implements lakeorm.Driver.
//
// KindDDL / KindSQL route through (*sql.DB).ExecContext.
// KindDirectIngest walks plan.Rows via reflection and fires one
// prepared INSERT per row — fine for an embedded engine where
// there's no network round-trip per statement. KindParquetIngest
// is not reachable from the duckdb Dialect (which never routes to
// it); if another dialect emits one at this driver it errors.
func (d *driver) Execute(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
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
func (d *driver) executeDirectIngest(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
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
	cols = append(cols, lakeorm.SystemIngestIDColumn)
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

// ExecuteStreaming implements lakeorm.Driver. Runs the plan's SQL
// via QueryContext and returns an iter.Seq2 of raw Rows. Struct-
// level decoding happens upstream in lake-orm's root Scanner when
// the caller invokes Query[T] / CollectAs[T] / StreamAs[T].
func (d *driver) ExecuteStreaming(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.RowStream, error) {
	rows, err := d.db.QueryContext(ctx, plan.SQL, plan.Args...)
	if err != nil {
		return nil, fmt.Errorf("duckdb: query: %w", err)
	}
	return rowsToStream(rows), nil
}

// DataFrame implements lakeorm.Driver. DuckDB doesn't ship a native
// DataFrame abstraction, so we expose a thin wrapper that presents
// the result through the Schema / Collect / Count / Stream surface.
// Use a Spark-family driver when you need the full DataFrame
// transformation API.
func (d *driver) DataFrame(ctx context.Context, sqlStr string, args ...any) (lakeorm.DataFrame, error) {
	return &queryDataFrame{db: d.db, ctx: ctx, sql: sqlStr, args: args}, nil
}

// Exec implements lakeorm.Driver. Plain fire-and-forget exec.
func (d *driver) Exec(ctx context.Context, sqlStr string, args ...any) (lakeorm.ExecResult, error) {
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

// rowsToStream adapts *sql.Rows into lakeorm.RowStream.
func rowsToStream(rows *sql.Rows) lakeorm.RowStream {
	return func(yield func(lakeorm.Row, error) bool) {
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			yield(nil, err)
			return
		}
		for rows.Next() {
			vals := make([]any, len(cols))
			holders := make([]any, len(cols))
			for i := range vals {
				holders[i] = &vals[i]
			}
			if err := rows.Scan(holders...); err != nil {
				yield(nil, err)
				return
			}
			if !yield(&rowValue{cols: cols, vals: vals}, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(nil, err)
		}
	}
}

// rowValue implements lakeorm.Row for rows coming out of *sql.Rows.
type rowValue struct {
	cols []string
	vals []any
}

func (r *rowValue) Values() []any     { return r.vals }
func (r *rowValue) Columns() []string { return r.cols }

// queryDataFrame is the lakeorm.DataFrame adapter over a pending
// *sql.DB query. The query executes lazily on the first terminal
// call (Collect / Count / Stream / Schema).
type queryDataFrame struct {
	db   *sql.DB
	ctx  context.Context
	sql  string
	args []any
}

func (q *queryDataFrame) Schema(ctx context.Context) ([]lakeorm.ColumnInfo, error) {
	// LIMIT 0 is the cheapest way to introspect column metadata
	// without scanning rows. DuckDB accepts it.
	rows, err := q.db.QueryContext(ctx, q.sql+" LIMIT 0", q.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	out := make([]lakeorm.ColumnInfo, len(cts))
	for i, ct := range cts {
		nullable, _ := ct.Nullable()
		out[i] = lakeorm.ColumnInfo{
			Name:     ct.Name(),
			DataType: ct.DatabaseTypeName(),
			Nullable: nullable,
		}
	}
	return out, nil
}

func (q *queryDataFrame) Collect(ctx context.Context) ([][]any, error) {
	rows, err := q.db.QueryContext(ctx, q.sql, q.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		holders := make([]any, len(cols))
		for i := range vals {
			holders[i] = &vals[i]
		}
		if err := rows.Scan(holders...); err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	return out, rows.Err()
}

func (q *queryDataFrame) Count(ctx context.Context) (int64, error) {
	var n int64
	if err := q.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ("+q.sql+")", q.args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (q *queryDataFrame) Stream(ctx context.Context) iter.Seq2[lakeorm.Row, error] {
	return func(yield func(lakeorm.Row, error) bool) {
		rows, err := q.db.QueryContext(ctx, q.sql, q.args...)
		if err != nil {
			yield(nil, err)
			return
		}
		for row, rerr := range rowsToStream(rows) {
			if !yield(row, rerr) {
				return
			}
		}
	}
}

// DriverType returns the underlying *sql.DB so callers with access
// to this DataFrame can drop down to raw database/sql. Mirrors the
// escape-hatch idiom the other *sql.DB-backed drivers expose.
func (q *queryDataFrame) DriverType() any { return q.db }
