package lakeorm

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"

	"github.com/datalake-go/lake-orm/backends"
	"github.com/datalake-go/lake-orm/dialects"
	"github.com/datalake-go/lake-orm/drivers"
)

// client is the default Client implementation. Holds the three
// injected dependencies plus the backpressure semaphore. Behaviour
// is split across sibling files by responsibility:
//
//   - lakeorm_client_impl.go (this file) — struct + trivial passthroughs
//   - lakeorm_insert.go      — Insert + fast-path + schemaFromRecords
//   - lakeorm_migrate.go     — Migrate + MigrateGenerate + diff planning
//   - lakeorm_ingest_id.go   — CleanupStaging + StagingPrefix listing
type client struct {
	driver  drivers.Driver
	dialect dialects.Dialect
	backend backends.Backend
	cfg     *clientConfig
	sem     *semaphore.Weighted
}

func (c *client) Driver() drivers.Driver { return c.driver }

func (c *client) Exec(ctx context.Context, sql string, args ...any) (drivers.ExecResult, error) {
	return c.driver.Exec(ctx, sql, args...)
}

func (c *client) Close() error { return c.driver.Close() }

// MetricsRegistry is a v0 placeholder. v1+ returns a populated
// *prometheus.Registry carrying the lakeorm_* counter / histogram /
// gauge set.
func (c *client) MetricsRegistry() *prometheus.Registry { return nil }
