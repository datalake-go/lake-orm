package spark

import (
	"context"
	"fmt"

	scsql "github.com/datalake-go/spark-connect-go/spark/sql"

	"github.com/datalake-go/lake-orm"
)

// FromFactory builds a lakeorm.Driver around an arbitrary Spark
// Connect session factory. Exported so sibling driver packages —
// notably driver/databricksconnect — can reuse the full driver
// machinery (session pool, conf application, Execute/Query/DataFrame
// dispatch, cluster-not-ready translation) without duplicating it.
//
// `name` is returned verbatim from Driver.Name; "spark-remote" for
// the plain Remote constructor, "databricks-connect" for the
// Databricks Connect wrapper, etc. Shows up in logs + metrics as
// the driver discriminator.
//
// `factory` produces a fresh scsql.SparkSession per pool borrow.
// FromFactory wraps the supplied factory so session-level confs from
// the opts are applied after the caller's factory returns, before
// the session is handed to lakeorm. Conf failures surface as
// factory errors — the pool will retry on the next borrow.
//
// Regular users should not need this entry point. Use Remote for
// self-hosted Spark Connect; use the databricksconnect package for
// Databricks clusters.
func FromFactory(
	name string,
	factory func(context.Context) (scsql.SparkSession, error),
	opts ...RemoteOption,
) lakeorm.Driver {
	cfg := newRemoteConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	wrapped := func(ctx context.Context) (scsql.SparkSession, error) {
		s, err := factory(ctx)
		if err != nil {
			return nil, err
		}
		if err := applyConfs(ctx, s, cfg.sessionConfs, cfg.logger); err != nil {
			return nil, fmt.Errorf("spark.FromFactory(%s): apply session confs: %w", name, err)
		}
		return s, nil
	}
	return &Driver{
		name:   name,
		logger: cfg.logger,
		pool:   newSessionPool(cfg.poolSize, wrapped),
	}
}
