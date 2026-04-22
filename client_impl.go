package lakeorm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// client is the default Client implementation. Holds the three
// injected dependencies plus the scanner and backpressure semaphore.
type client struct {
	driver  Driver
	dialect Dialect
	backend Backend
	cfg     *clientConfig
	scanner *Scanner
	sem     *semaphore.Weighted
}

func (c *client) Insert(ctx context.Context, records any, opts ...InsertOption) error {
	ic := &insertConfig{}
	for _, o := range opts {
		o(ic)
	}

	schema, n, approx, err := schemaFromRecords(records)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	// Generate the per-operation ingest_id up-front. Used as:
	//   - staging prefix key (<warehouse>/<ingest_id>/part-*.parquet)
	//   - janitor filter (UUIDv7 embeds a ms timestamp)
	//   - (Phase 2) MERGE filter on the parquet-staging source to
	//     bound the upsert scope and make retry-on-OCC-conflict
	//     idempotent
	//
	// UUIDv7 so the staging prefix is time-sortable and
	// CleanupStaging can identify orphans by parsing the embedded
	// ms-precision timestamp — see INGEST_ID.md.
	ingestUUID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("lakeorm.Insert: generate ingest_id: %w", err)
	}
	ingestID := ingestUUID.String()

	if err := Validate(records); err != nil {
		return err
	}

	// idempotency is the caller-visible dedup token and stays
	// separate from the ingest_id. Empty when the caller doesn't
	// supply one; Dialect implementations may synthesize if they need
	// a stable token, but the staging prefix uses ingestID.
	idem := ic.idempotencyKey

	plan, err := c.dialect.PlanInsert(WriteRequest{
		Ctx:            ctx,
		Schema:         schema,
		IngestID:       ingestID,
		Records:        records,
		RecordCount:    n,
		ApproxRowBytes: approx,
		Idempotency:    idem,
		Backend:        c.backend,
		FastPathBytes:  c.cfg.fastPathThreshold,
		ForcePath:      ic.path,
	})
	if err != nil {
		return fmt.Errorf("lakeorm.Insert: plan: %w", err)
	}

	switch plan.Kind {
	case KindParquetIngest, KindParquetMerge:
		// Both variants ride the same staging-writer + commit path;
		// the difference is the SQL the driver emits on Execute
		// (INSERT INTO ... vs MERGE INTO ...). Staging behaviour,
		// row conversion, and finalizer lifecycle are identical.
		return c.runFastPath(ctx, plan, schema, records)
	default:
		res, fin, err := c.driver.Execute(ctx, plan)
		_ = res
		if err != nil {
			if fin != nil {
				_ = fin.Abort(ctx)
			}
			return err
		}
		if fin != nil {
			return fin.Commit(ctx)
		}
		return nil
	}
}

func (c *client) InsertRaw(_ context.Context, _ any, _ ...InsertOption) RawInsertion {
	return &stubRawInsertion{err: ErrNotImplemented}
}

func (c *client) Update(context.Context, any, ...UpdateOption) error { return ErrNotImplemented }
func (c *client) Upsert(context.Context, any, ...UpsertOption) error { return ErrNotImplemented }
func (c *client) Delete(context.Context, any, ...DeleteOption) error { return ErrNotImplemented }

func (c *client) Query(ctx context.Context) QueryBuilder {
	return &dynamicQuery{client: c, ctx: ctx}
}

func (c *client) Exec(ctx context.Context, sql string, args ...any) (ExecResult, error) {
	return c.driver.Exec(ctx, sql, args...)
}

func (c *client) DataFrame(ctx context.Context, sql string, args ...any) (DataFrame, error) {
	return c.driver.DataFrame(ctx, sql, args...)
}

// Session returns a typed pooled session. v0 stub — the PooledSession
// type wraps Driver.DataFrame / Exec for raw SQL work without the
// Dialect layer.
func (c *client) Session(context.Context) (*PooledSession, error) {
	return nil, ErrNotImplemented
}

func (c *client) Migrate(ctx context.Context, models ...any) error {
	// Ensure the target database exists before creating tables.
	// Iceberg REST catalogs (Nessie / Polaris / Tabular) require the
	// namespace to be explicitly registered before CREATE TABLE; Hive
	// catalogs tolerate an implicit default. CREATE NAMESPACE IF NOT
	// EXISTS is the portable shape that works for both. The "lakeorm"
	// prefix matches the catalog name docker-compose / lake-k8s
	// configures; v1 promotes this to Dialect.EnsureNamespace so the
	// hardcoded prefix goes away.
	if db := c.cfg.defaultDatabase; db != "" && c.dialect.Name() == "iceberg" {
		ns := "lakeorm." + db
		if _, err := c.driver.Exec(ctx, "CREATE NAMESPACE IF NOT EXISTS "+ns); err != nil {
			return fmt.Errorf("lakeorm.Migrate: ensure namespace %s: %w", ns, err)
		}
	}

	for _, m := range models {
		schema, err := ParseSchema(reflectTypeOf(m))
		if err != nil {
			return err
		}
		loc := c.backend.TableLocation(schema.TableName)
		ddl, err := c.dialect.CreateTableDDL(schema, loc)
		if err != nil {
			return err
		}
		if _, err := c.driver.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("lakeorm.Migrate %s: %w", schema.TableName, err)
		}
	}
	return nil
}

func (c *client) Maintain() Maintenance { return c.dialect.Maintenance() }

func (c *client) Ping(ctx context.Context) error {
	_, err := c.driver.Exec(ctx, "SELECT 1")
	return err
}

func (c *client) Close() error { return c.driver.Close() }

// MetricsRegistry is a v0 placeholder. v1+ returns a populated
// *prometheus.Registry carrying the lakeorm_* counter / histogram /
// gauge set documented in MONETIZATION.md.
func (c *client) MetricsRegistry() *prometheus.Registry { return nil }

// PooledSession is the typed-session handle returned by Client.Session.
// v0: wraps driver.DataFrame/Exec; in v1 it carries the raw
// scsql.SparkSession through for users who need it directly.
type PooledSession struct {
	driver Driver
	ctx    context.Context
}

func (s *PooledSession) Sql(ctx context.Context, query string) (DataFrame, error) {
	return s.driver.DataFrame(ctx, query)
}

func (s *PooledSession) Exec(ctx context.Context, query string) (ExecResult, error) {
	return s.driver.Exec(ctx, query)
}

func (s *PooledSession) Table(name string) (DataFrame, error) {
	return s.driver.DataFrame(s.ctx, "SELECT * FROM "+name)
}

type stubRawInsertion struct{ err error }

func (r *stubRawInsertion) ThenMerge(context.Context, MergeOpts) error { return r.err }
func (r *stubRawInsertion) AsyncThenMerge(MergeOpts) MergeFuture       { return nil }
func (r *stubRawInsertion) Commit(context.Context) error               { return r.err }
