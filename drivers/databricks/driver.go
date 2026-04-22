// Package databricks is lake-orm's native Databricks driver. Takes
// a user-constructed *sql.DB (typically from databricks-sql-go) and
// implements the lakeorm.Driver interface by translating between
// Spark-tagged Go structs and SQL rows.
//
// Bring-your-own-connection. Configuring Databricks (OAuth M2M,
// warehouse selection, catalog/schema routing, connection-pool
// lifecycle, CloudFetch, session params) is notoriously finicky and
// every team handles it differently — this driver doesn't try to
// unify that. The caller constructs the *sql.DB exactly as their
// environment requires, then hands it here.
//
// Three drivers, three paths:
//
//   - driver/spark            — generic Spark Connect (self-hosted,
//     EMR, Glue, lake-k8s)
//   - driver/databricksconnect — Databricks Connect (Spark Connect
//     over Databricks, OAuth-wrapped URL)
//   - driver/databricks       — Databricks native (this package;
//     BYO *sql.DB via databricks-sql-go)
//
// Pick based on what's on your cluster: Spark Connect endpoints for
// the first two, SQL warehouses + COPY INTO for this one. All three
// present the same lake-orm Client API upstream — ORM code stays
// portable across them.
//
// v0 surface: Exec, ExecContext-backed Query, streaming read via
// *sql.Rows adapter, typed scan via `spark:"..."` tags. The fast-
// path write (KindParquetIngest) via S3 staging + COPY INTO is a
// v1 target — callers who need it today wire it themselves against
// the returned *sql.DB.
package databricks

import (
	"context"
	"database/sql"
	"fmt"
	"iter"

	"github.com/datalake-go/lake-orm"
)

// Driver returns a lakeorm.Driver backed by the supplied *sql.DB.
// The database/sql handle is owned by the caller: Driver.Close
// does NOT close it. This matches the database/sql lifecycle most
// consumers already implement.
//
// Opts currently hold no tuning — reserved so the public signature
// stays stable if session-level knobs land later.
func Driver(db *sql.DB, opts ...Option) lakeorm.Driver {
	cfg := &config{name: "databricks"}
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

// WithName overrides the Driver.Name() string. Defaults to
// "databricks"; set for discriminating multiple Databricks driver
// instances in logs / metrics.
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
// interface doesn't expose (PrepareContext, transactions, etc.).
func (d *driver) DB() *sql.DB { return d.db }

// Name implements lakeorm.Driver.
func (d *driver) Name() string { return d.name }

// Close implements lakeorm.Driver. The *sql.DB is owned by the
// caller and is NOT closed here — they constructed it, they close
// it on their own shutdown path.
func (d *driver) Close() error { return nil }

// Execute implements lakeorm.Driver for plans the v0 surface
// supports.
//
// KindDDL / KindSQL route through (*sql.DB).ExecContext as a
// fire-and-forget statement. Direct-ingest / parquet-ingest paths
// are v1 targets — they require Backend + COPY INTO wiring the
// reference implementation in svc-data-platform-api documents.
func (d *driver) Execute(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
	switch plan.Kind {
	case lakeorm.KindDDL, lakeorm.KindSQL:
		if _, err := d.db.ExecContext(ctx, plan.SQL, plan.Args...); err != nil {
			return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("databricks: exec: %w", err)
		}
		return lakeorm.Result{}, nopFinalizer{}, nil
	default:
		return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("databricks: plan kind %d not supported in v0 (use the spark driver for KindParquetIngest / KindDirectIngest)", plan.Kind)
	}
}

// ExecuteStreaming implements lakeorm.Driver. Runs the plan's SQL
// via QueryContext and returns an iter.Seq2 of raw Rows. Struct-
// level decoding happens upstream in lake-orm's root Scanner when
// the caller invokes Query[T] / CollectAs[T] / StreamAs[T].
func (d *driver) ExecuteStreaming(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.RowStream, error) {
	rows, err := d.db.QueryContext(ctx, plan.SQL, plan.Args...)
	if err != nil {
		return nil, fmt.Errorf("databricks: query: %w", err)
	}
	return rowsToStream(rows), nil
}

// DataFrame implements lakeorm.Driver. No native DataFrame
// abstraction exists on *sql.DB — we expose a thin wrapper that
// presents the result through the Schema / Collect / Count / Stream
// surface. Use the spark or databricksconnect drivers if you need
// the full DataFrame transformation API.
func (d *driver) DataFrame(ctx context.Context, sqlStr string, args ...any) (lakeorm.DataFrame, error) {
	return &queryDataFrame{db: d.db, ctx: ctx, sql: sqlStr, args: args}, nil
}

// Exec implements lakeorm.Driver. Plain fire-and-forget exec.
func (d *driver) Exec(ctx context.Context, sqlStr string, args ...any) (lakeorm.ExecResult, error) {
	res, err := d.db.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		return lakeorm.ExecResult{}, fmt.Errorf("databricks: exec: %w", err)
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

// rowsToStream adapts *sql.Rows into lakeorm.RowStream. Each
// iteration yields a rowValue carrying the raw column values + the
// column names — same shape the Spark driver produces so lake-orm's
// root Scanner doesn't need to distinguish.
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
// call (Collect / Count / Stream / Schema) — matches how callers
// expect the DataFrame interface to behave.
type queryDataFrame struct {
	db   *sql.DB
	ctx  context.Context
	sql  string
	args []any
}

func (q *queryDataFrame) Schema(ctx context.Context) ([]lakeorm.ColumnInfo, error) {
	// LIMIT 0 is the cheapest way to get column metadata from a
	// warehouse without scanning rows. Databricks accepts it.
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
// to this DataFrame can drop down to raw database/sql operations.
// Deliberately mirrors how the spark driver exposes its native
// SparkSession — same escape-hatch idiom.
func (q *queryDataFrame) DriverType() any { return q.db }
