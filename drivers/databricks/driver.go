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
//   - drivers/spark            — generic Spark Connect (self-hosted,
//     EMR, Glue, lake-k8s)
//   - drivers/databricksconnect — Databricks Connect (Spark Connect
//     over Databricks, OAuth-wrapped URL)
//   - drivers/databricks       — Databricks native (this package;
//     BYO *sql.DB via databricks-sql-go)
//
// Pick based on what's on your cluster: Spark Connect endpoints for
// the first two, SQL warehouses + COPY INTO for this one. All three
// present the same lake-orm Client API upstream — ORM code stays
// portable across them.
//
// v0 surface: Exec, drivers.Convertible reads via *sql.Rows, typed
// scan via `spark:"..."` tags. The fast-path write
// (KindParquetIngest) via S3 staging + COPY INTO is a v1 target —
// callers who need it today wire it themselves against the returned
// *sql.DB.
package databricks

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datalake-go/lake-orm/drivers"
)

// New returns a *Driver backed by the supplied *sql.DB.
// The database/sql handle is owned by the caller: Driver.Close
// does NOT close it. This matches the database/sql lifecycle most
// consumers already implement.
//
// Opts currently hold no tuning — reserved so the public signature
// stays stable if session-level knobs land later.
func New(db *sql.DB, opts ...Option) *Driver {
	cfg := &config{name: "databricks"}
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

// Driver is lake-orm's native Databricks driver. Exported so
// callers can reach the per-driver conversion helpers (FromSQL,
// FromRows, FromTable, FromRow) and the raw *sql.DB (DB) via a
// Client.Driver() type-assertion.
type Driver struct {
	name string
	db   *sql.DB
}

// DB returns the underlying *sql.DB. Escape hatch for callers who
// need to drop to raw database/sql operations the lakeorm.Driver
// interface doesn't expose (PrepareContext, transactions, etc.).
func (d *Driver) DB() *sql.DB { return d.db }

// Name implements lakeorm.Driver.
func (d *Driver) Name() string { return d.name }

// Close implements lakeorm.Driver. The *sql.DB is owned by the
// caller and is NOT closed here — they constructed it, they close
// it on their own shutdown path.
func (d *Driver) Close() error { return nil }

// Execute implements lakeorm.Driver for plans the v0 surface
// supports.
//
// KindDDL / KindSQL route through (*sql.DB).ExecContext as a
// fire-and-forget statement. Direct-ingest / parquet-ingest paths
// are v1 targets — they require Backend + COPY INTO wiring the
// reference implementation in svc-data-platform-api documents.
func (d *Driver) Execute(ctx context.Context, plan drivers.ExecutionPlan) (drivers.Result, drivers.Finalizer, error) {
	switch plan.Kind {
	case drivers.KindDDL, drivers.KindSQL:
		if _, err := d.db.ExecContext(ctx, plan.SQL, plan.Args...); err != nil {
			return drivers.Result{}, nopFinalizer{}, fmt.Errorf("databricks: exec: %w", err)
		}
		return drivers.Result{}, nopFinalizer{}, nil
	default:
		return drivers.Result{}, nopFinalizer{}, fmt.Errorf("databricks: plan kind %d not supported in v0 (use the spark driver for KindParquetIngest / KindDirectIngest)", plan.Kind)
	}
}

// Exec implements lakeorm.Driver. Plain fire-and-forget exec.
func (d *Driver) Exec(ctx context.Context, sqlStr string, args ...any) (drivers.ExecResult, error) {
	res, err := d.db.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		return drivers.ExecResult{}, fmt.Errorf("databricks: exec: %w", err)
	}
	var affected int64 = -1
	if n, nerr := res.RowsAffected(); nerr == nil {
		affected = n
	}
	return drivers.ExecResult{RowsAffected: affected}, nil
}

// nopFinalizer satisfies drivers.Finalizer for single-phase plans.
type nopFinalizer struct{}

func (nopFinalizer) Commit(context.Context) error { return nil }
func (nopFinalizer) Abort(context.Context) error  { return nil }
