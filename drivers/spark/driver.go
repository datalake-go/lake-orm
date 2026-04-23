// Package spark provides lakeorm's generic Spark Connect driver.
// Use Remote for plain Spark Connect endpoints (self-hosted, EMR,
// Glue, lake-k8s). For Databricks clusters see the sibling
// driver/databricksconnect package.
package spark

import (
	"context"
	"fmt"
	"strings"

	scsql "github.com/datalake-go/spark-connect-go/spark/sql"
	"github.com/rs/zerolog"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backends"
	lkerrors "github.com/datalake-go/lake-orm/errors"
	"github.com/datalake-go/lake-orm/types"
)

// Driver is the shared Spark Connect driver backing both Remote and
// databricksconnect. Exported so callers can reach the per-driver
// conversion helpers (FromSQL, FromDataFrame, FromTable, FromRow)
// and the raw session (Session) via a Client.Driver() type assertion.
//
// Session-level confs (WithSessionConfs) are applied inside the pool
// factory, not held on driver, so they survive pool refreshes and
// apply to every newly-created session identically.
type Driver struct {
	name   string
	logger zerolog.Logger
	pool   *SessionPool
}

// Name implements lakeorm.Driver.
func (d *Driver) Name() string { return d.name }

// Close implements lakeorm.Driver. Stops the session pool.
func (d *Driver) Close() error { return d.pool.Close() }

// Execute implements lakeorm.Driver. Dispatches by plan kind.
func (d *Driver) Execute(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
	switch plan.Kind {
	case lakeorm.KindSQL, lakeorm.KindDDL:
		return d.executeSQL(ctx, plan)
	case lakeorm.KindDirectIngest:
		return d.executeDirectIngest(ctx, plan)
	case lakeorm.KindParquetIngest:
		return d.executeParquetIngest(ctx, plan)
	case lakeorm.KindParquetMerge:
		return d.executeParquetMerge(ctx, plan)
	default:
		return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("spark: unsupported plan kind %d", plan.Kind)
	}
}

func (d *Driver) executeSQL(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
	s, err := d.pool.Borrow(ctx)
	if err != nil {
		return lakeorm.Result{}, nopFinalizer{}, err
	}
	defer d.pool.Return(s)

	df, err := s.Sql(ctx, plan.SQL)
	if err != nil {
		return lakeorm.Result{}, nopFinalizer{}, translateClusterError(err)
	}
	if _, err := df.Collect(ctx); err != nil {
		return lakeorm.Result{}, nopFinalizer{}, translateClusterError(err)
	}
	return lakeorm.Result{}, nopFinalizer{}, nil
}

// executeDirectIngest is the small-batch path — v0 stub. Real
// implementation constructs an Arrow record batch from plan.Rows and
// calls spark.CreateDataFrameFromArrow + df.Write().SaveAsTable. Wired
// up in the write-path milestone.
func (d *Driver) executeDirectIngest(_ context.Context, _ lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
	return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("spark: direct ingest not yet implemented (v0 scaffold)")
}

// executeParquetIngest is the fast-path commit — once the partition
// writer has uploaded every part, this runs SQL to register them with
// the target table via a temp view.
//
// We use CREATE TEMP VIEW ... USING parquet (+ INSERT ... SELECT)
// instead of the shorter SELECT * FROM parquet.`<path>` because the
// Iceberg SparkSessionExtensions intercept the `parquet.<path>`
// shorthand and reject it as "not a valid Iceberg table". The temp
// view path goes through the standard DataSourceV2 read, which
// Iceberg's extensions leave alone. See UNSUPPORTED_DATASOURCE_FOR_
// DIRECT_QUERY.
func (d *Driver) executeParquetIngest(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
	s, err := d.pool.Borrow(ctx)
	if err != nil {
		return lakeorm.Result{}, nopFinalizer{}, err
	}

	// View name derived from the staging prefix — guaranteed unique
	// per ingest by the SortableID the Client generates.
	viewName := "lakeorm_staging_" + sanitizeIdent(plan.Staging.Prefix)
	uri := plan.Staging.Location.URI()

	// `INSERT INTO target SELECT * FROM staging` works because the
	// parquet staging now emits the system-managed _ingest_id
	// column alongside user columns (see BuildParquetSchema +
	// ConverterFor). Target and staging column sets match.
	f := &parquetIngestFinalizer{
		driver:   d,
		session:  s,
		viewName: viewName,
		viewSQL:  fmt.Sprintf("CREATE OR REPLACE TEMP VIEW %s USING parquet OPTIONS (path '%s/*.parquet')", viewName, uri),
		insertSQL: fmt.Sprintf("INSERT INTO %s SELECT * FROM %s",
			plan.Target, viewName),
		dropSQL: "DROP VIEW IF EXISTS " + viewName,
		backend: plan.Staging.Backend,
		prefix:  plan.Staging.Prefix,
		logger:  d.logger,
	}
	return lakeorm.Result{}, f, nil
}

// executeParquetMerge is the upsert variant of executeParquetIngest.
// Used when the schema declares at least one mergeKey field (see
// lakeorm.KindParquetMerge).
//
// The emitted SQL is:
//
//	MERGE INTO <target> AS target
//	USING (SELECT * FROM <staging> WHERE _ingest_id = '<ingest_id>') AS source
//	ON target.<mk1> = source.<mk1> [AND ...]
//	WHEN MATCHED THEN UPDATE SET *
//	WHEN NOT MATCHED THEN INSERT *
//
// The _ingest_id filter on source is what makes this O(batch_size)
// instead of O(whole_staging) and what makes retry-on-OCC-conflict
// idempotent — same filter, same source set. Without it, two
// concurrent MERGEs would see each other's parquet parts in the
// staging directory (if the caller ever shared staging roots) and
// merge them indiscriminately.
func (d *Driver) executeParquetMerge(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
	if len(plan.Schema.MergeKeys) == 0 {
		return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("spark: parquet-merge requires schema.MergeKeys")
	}
	if plan.IngestID == "" {
		return lakeorm.Result{}, nopFinalizer{}, fmt.Errorf("spark: parquet-merge requires plan.IngestID")
	}
	s, err := d.pool.Borrow(ctx)
	if err != nil {
		return lakeorm.Result{}, nopFinalizer{}, err
	}

	viewName := "lakeorm_staging_" + sanitizeIdent(plan.Staging.Prefix)
	uri := plan.Staging.Location.URI()

	// ON clause: AND-joined equality on every mergeKey field.
	onParts := make([]string, 0, len(plan.Schema.MergeKeys))
	for _, idx := range plan.Schema.MergeKeys {
		col := plan.Schema.Fields[idx].Column
		onParts = append(onParts, fmt.Sprintf("target.%s = source.%s", col, col))
	}
	onClause := strings.Join(onParts, " AND ")

	mergeSQL := fmt.Sprintf(
		"MERGE INTO %s AS target "+
			"USING (SELECT * FROM %s WHERE %s = '%s') AS source "+
			"ON %s "+
			"WHEN MATCHED THEN UPDATE SET * "+
			"WHEN NOT MATCHED THEN INSERT *",
		plan.Target, viewName, types.SystemIngestIDColumn, plan.IngestID, onClause,
	)

	f := &parquetIngestFinalizer{
		driver:    d,
		session:   s,
		viewName:  viewName,
		viewSQL:   fmt.Sprintf("CREATE OR REPLACE TEMP VIEW %s USING parquet OPTIONS (path '%s/*.parquet')", viewName, uri),
		insertSQL: mergeSQL,
		dropSQL:   "DROP VIEW IF EXISTS " + viewName,
		backend:   plan.Staging.Backend,
		prefix:    plan.Staging.Prefix,
		logger:    d.logger,
	}
	return lakeorm.Result{}, f, nil
}

// sanitizeIdent strips non-identifier characters so a staging prefix
// (which has slashes and KSUID alphanumerics) is safe to use as a
// temp view name. SQL identifiers can't contain '/' or '-'.
func sanitizeIdent(s string) string {
	out := make([]byte, 0, len(s))
	for _, c := range []byte(s) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			out = append(out, c)
		case c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// Exec implements lakeorm.Driver.Exec — fire-and-forget SQL for DDL or
// one-off DML. Does not go through Dialect.
func (d *Driver) Exec(ctx context.Context, sql string, args ...any) (lakeorm.ExecResult, error) {
	s, err := d.pool.Borrow(ctx)
	if err != nil {
		return lakeorm.ExecResult{}, err
	}
	defer d.pool.Return(s)

	rendered, err := renderSQL(sql, args)
	if err != nil {
		return lakeorm.ExecResult{}, err
	}

	df, err := s.Sql(ctx, rendered)
	if err != nil {
		return lakeorm.ExecResult{}, translateClusterError(err)
	}
	if _, err := df.Collect(ctx); err != nil {
		return lakeorm.ExecResult{}, translateClusterError(err)
	}
	return lakeorm.ExecResult{}, nil
}

// nopFinalizer is the no-op Finalizer returned by single-phase plans.
type nopFinalizer struct{}

func (nopFinalizer) Commit(context.Context) error { return nil }
func (nopFinalizer) Abort(context.Context) error  { return nil }

// parquetIngestFinalizer commits the fast-path write by registering a
// temp view over the staged parquet parts and INSERTing from it into
// the target table. Abort drops the view (if created) and cleans up
// staging instead.
type parquetIngestFinalizer struct {
	driver    *Driver
	session   scsql.SparkSession
	viewName  string
	viewSQL   string
	insertSQL string
	dropSQL   string
	backend   backends.Backend
	prefix    string
	logger    zerolog.Logger
	committed bool
}

// runSQL executes a statement and drains its result — Spark SQL
// statements return a DataFrame that must be collected to drive
// execution, even when it has no rows.
func (f *parquetIngestFinalizer) runSQL(ctx context.Context, sql string) error {
	df, err := f.session.Sql(ctx, sql)
	if err != nil {
		return translateClusterError(err)
	}
	if _, err := df.Collect(ctx); err != nil {
		return translateClusterError(err)
	}
	return nil
}

func (f *parquetIngestFinalizer) Commit(ctx context.Context) error {
	if f.committed {
		return lkerrors.ErrAlreadyCommitted
	}
	defer f.driver.pool.Return(f.session)

	if err := f.runSQL(ctx, f.viewSQL); err != nil {
		return fmt.Errorf("create staging view: %w", err)
	}
	// Drop the view after successful INSERT regardless of error so
	// long-running sessions don't accumulate them.
	defer func() { _ = f.runSQL(ctx, f.dropSQL) }()

	if err := f.runSQL(ctx, f.insertSQL); err != nil {
		return fmt.Errorf("insert from staging: %w", err)
	}
	f.committed = true
	return nil
}

func (f *parquetIngestFinalizer) Abort(ctx context.Context) error {
	if f.committed {
		return nil
	}
	defer f.driver.pool.Return(f.session)
	if f.backend == nil || f.prefix == "" {
		return nil
	}
	return f.backend.CleanupStaging(ctx, f.prefix)
}
