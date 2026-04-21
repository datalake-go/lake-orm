package spark

import (
	"context"
	"fmt"

	scsql "github.com/datalake-go/spark-connect-go/spark/sql"
	"github.com/rs/zerolog"

	"github.com/datalake-go/lake-orm"
)

// RemoteOption tunes a Remote driver. Functional so the public surface
// stays extensible.
type RemoteOption func(*remoteConfig)

type remoteConfig struct {
	logger       zerolog.Logger
	poolSize     int
	sessionConfs map[string]string
}

func newRemoteConfig() *remoteConfig {
	return &remoteConfig{
		logger:       zerolog.Nop(),
		poolSize:     8,
		sessionConfs: map[string]string{},
	}
}

// WithLogger sets the driver logger.
func WithLogger(l zerolog.Logger) RemoteOption {
	return func(c *remoteConfig) { c.logger = l }
}

// WithPoolSize overrides the session pool size for this driver
// (defaults to 8 — see SessionPool). Normally tuned at the Client
// level via lakeorm.WithSessionPoolSize.
func WithPoolSize(n int) RemoteOption {
	return func(c *remoteConfig) {
		if n > 0 {
			c.poolSize = n
		}
	}
}

// WithSessionConfs sets Spark SQL session-level configuration that
// every borrowed session runs `SET key=value` on first use. Useful
// for per-client Hadoop S3A credentials, Arrow batch sizes, and any
// other spark.* knobs the Spark Connect server can't hard-wire at
// startup. Prefer server-level configuration when the cluster allows
// it; fall back to this for dev stacks, on-prem, or tenant-specific
// knobs that shouldn't leak into the cluster defaults.
func WithSessionConfs(confs map[string]string) RemoteOption {
	return func(c *remoteConfig) {
		for k, v := range confs {
			c.sessionConfs[k] = v
		}
	}
}

// Remote returns a Driver that connects to a plain Spark Connect
// endpoint (no OAuth, no cluster-ID header). Use for self-hosted
// Spark, EMR, Glue, and the lake-k8s local stack.
//
//	spark.Remote("sc://spark.internal:15002")
func Remote(uri string, opts ...RemoteOption) lakeorm.Driver {
	cfg := newRemoteConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	factory := func(ctx context.Context) (scsql.SparkSession, error) {
		s, err := (&scsql.SparkSessionBuilder{}).Remote(uri).Build(ctx)
		if err != nil {
			return nil, fmt.Errorf("spark.Remote: build session: %w", translateClusterError(err))
		}
		// Apply session-level Spark confs (Hadoop S3A creds, Arrow
		// batch sizes, etc.) before the caller sees the session.
		// Failures are logged but non-fatal: a misspelled conf key
		// shouldn't take down the whole driver — the misconfigured
		// query will fail more visibly later.
		if err := applyConfs(ctx, s, cfg.sessionConfs, cfg.logger); err != nil {
			return nil, fmt.Errorf("spark.Remote: apply session confs: %w", err)
		}
		return s, nil
	}

	d := &driver{
		name:   "spark-remote",
		logger: cfg.logger,
		pool:   newSessionPool(cfg.poolSize, factory),
	}
	return d
}
