// Package spark provides lakeorm's generic Spark Connect driver.
// Use Remote for plain Spark Connect endpoints (self-hosted, EMR,
// Glue, lake-k8s). For Databricks clusters see the sibling
// driver/databricksconnect package.
package spark

import (
	"context"
	"fmt"
	"iter"
	"strings"

	scsql "github.com/datalake-go/spark-connect-go/spark/sql"
	"github.com/rs/zerolog"

	"github.com/datalake-go/lake-orm"
)

// driver is the shared Spark Connect driver backing both Remote and
// Databricks. It lives in an unexported type; users touch it via the
// lakeorm.Driver interface.
//
// Session-level confs (WithSessionConfs) are applied inside the pool
// factory, not held on driver, so they survive pool refreshes and
// apply to every newly-created session identically.
type driver struct {
	name   string
	logger zerolog.Logger
	pool   *SessionPool
}

// Name implements lakeorm.Driver.
func (d *driver) Name() string { return d.name }

// Close implements lakeorm.Driver. Stops the session pool.
func (d *driver) Close() error { return d.pool.Close() }

// Execute implements lakeorm.Driver. Dispatches by plan kind.
func (d *driver) Execute(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
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

func (d *driver) executeSQL(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
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
func (d *driver) executeDirectIngest(_ context.Context, _ lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
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
func (d *driver) executeParquetIngest(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
	s, err := d.pool.Borrow(ctx)
	if err != nil {
		return lakeorm.Result{}, nopFinalizer{}, err
	}

	// View name derived from the staging prefix — guaranteed unique
	// per ingest by the SortableID the Client generates.
	viewName := "dorm_staging_" + sanitizeIdent(plan.Staging.Prefix)
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
func (d *driver) executeParquetMerge(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.Result, lakeorm.Finalizer, error) {
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

	viewName := "dorm_staging_" + sanitizeIdent(plan.Staging.Prefix)
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
		plan.Target, viewName, lakeorm.SystemIngestIDColumn, plan.IngestID, onClause,
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

// ExecuteStreaming implements lakeorm.Driver. Produces a pull-based row
// stream backed by the Spark Connect client's StreamRows primitive
// (SPARK-52780) — constant memory, natural backpressure.
func (d *driver) ExecuteStreaming(ctx context.Context, plan lakeorm.ExecutionPlan) (lakeorm.RowStream, error) {
	s, err := d.pool.Borrow(ctx)
	if err != nil {
		return nil, err
	}

	// Render ? placeholders. Spark Connect's parameterized-SQL path
	// exists but doesn't round-trip reliably through every Spark
	// version we care about; inline quoting via renderSQL is the
	// safer path and matches what Exec / DataFrame do. When the fork
	// lands a stable posparameter binding, switch here first since
	// stream reads are the high-throughput path.
	rendered, err := renderSQL(plan.SQL, plan.Args)
	if err != nil {
		d.pool.Return(s)
		return nil, err
	}

	df, err := s.Sql(ctx, rendered)
	if err != nil {
		d.pool.Return(s)
		return nil, translateClusterError(err)
	}

	stream := &sparkRowStream{pool: d.pool, session: s, df: df, ctx: ctx}
	return stream.iter(), nil
}

// DataFrame implements lakeorm.Driver. The escape hatch — returns a
// DataFrame the caller can chain Spark operations on.
func (d *driver) DataFrame(ctx context.Context, sql string, args ...any) (lakeorm.DataFrame, error) {
	s, err := d.pool.Borrow(ctx)
	if err != nil {
		return nil, err
	}

	rendered, err := renderSQL(sql, args)
	if err != nil {
		d.pool.Return(s)
		return nil, err
	}

	df, err := s.Sql(ctx, rendered)
	if err != nil {
		d.pool.Return(s)
		return nil, translateClusterError(err)
	}
	return &sparkDataFrame{pool: d.pool, session: s, df: df}, nil
}

// Exec implements lakeorm.Driver.Exec — fire-and-forget SQL for DDL or
// one-off DML. Does not go through Dialect.
func (d *driver) Exec(ctx context.Context, sql string, args ...any) (lakeorm.ExecResult, error) {
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
	driver    *driver
	session   scsql.SparkSession
	viewName  string
	viewSQL   string
	insertSQL string
	dropSQL   string
	backend   lakeorm.Backend
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
		return lakeorm.ErrAlreadyCommitted
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

// sparkRowStream wraps a Spark DataFrame's streaming iterator as a
// lakeorm.RowStream. Each step pulls one logical row; the session is
// returned to the pool when iteration finishes.
type sparkRowStream struct {
	pool    *SessionPool
	session scsql.SparkSession
	df      scsql.DataFrame
	ctx     context.Context
}

func (s *sparkRowStream) iter() iter.Seq2[lakeorm.Row, error] {
	return func(yield func(lakeorm.Row, error) bool) {
		defer s.pool.Return(s.session)
		// TODO(v0): when the fork lands with StreamRows exported, swap
		// this Collect loop for the streaming primitive.
		rows, err := s.df.Collect(s.ctx)
		if err != nil {
			yield(nil, translateClusterError(err))
			return
		}
		cols, _ := s.df.Columns(s.ctx)
		for _, r := range rows {
			row := &sparkRow{values: r.Values(), columns: cols}
			if !yield(row, nil) {
				return
			}
		}
	}
}

// sparkRow is the lakeorm.Row implementation over a Spark Connect Row.
type sparkRow struct {
	values  []any
	columns []string
}

func (r *sparkRow) Values() []any     { return r.values }
func (r *sparkRow) Columns() []string { return r.columns }

// sparkDataFrame is the lakeorm.DataFrame wrapper.
type sparkDataFrame struct {
	pool    *SessionPool
	session scsql.SparkSession
	df      scsql.DataFrame
	closed  bool
}

func (d *sparkDataFrame) release() {
	if d.closed {
		return
	}
	d.pool.Return(d.session)
	d.closed = true
}

func (d *sparkDataFrame) Schema(ctx context.Context) ([]lakeorm.ColumnInfo, error) {
	cols, err := d.df.Columns(ctx)
	if err != nil {
		return nil, translateClusterError(err)
	}
	out := make([]lakeorm.ColumnInfo, len(cols))
	for i, c := range cols {
		out[i] = lakeorm.ColumnInfo{Name: c}
	}
	return out, nil
}

func (d *sparkDataFrame) Collect(ctx context.Context) ([][]any, error) {
	defer d.release()
	rows, err := d.df.Collect(ctx)
	if err != nil {
		return nil, translateClusterError(err)
	}
	out := make([][]any, len(rows))
	for i, r := range rows {
		out[i] = r.Values()
	}
	return out, nil
}

func (d *sparkDataFrame) Count(ctx context.Context) (int64, error) {
	defer d.release()
	n, err := d.df.Count(ctx)
	if err != nil {
		return 0, translateClusterError(err)
	}
	return n, nil
}

func (d *sparkDataFrame) Stream(ctx context.Context) iter.Seq2[lakeorm.Row, error] {
	return (&sparkRowStream{pool: d.pool, session: d.session, df: d.df, ctx: ctx}).iter()
}

func (d *sparkDataFrame) DriverType() any { return d.df }
