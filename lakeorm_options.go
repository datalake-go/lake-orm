package lakeorm

import (
	"time"

	"github.com/rs/zerolog"
)

// Compression selects the codec used by the fast-path partition writer.
type Compression int

const (
	CompressionZSTD Compression = iota
	CompressionSnappy
	CompressionGzip
	CompressionUncompressed
)

// ClientOption configures the Client at construction time. Functional
// options so the struct stays extensible without API breakage.
type ClientOption func(*clientConfig)

type clientConfig struct {
	logger             zerolog.Logger
	sessionPoolSize    int
	maxInflightIngests int
	fastPathThreshold  int
	compression        Compression
	idempotencyTTL     time.Duration
	defaultCatalog     string
	defaultDatabase    string
}

func newDefaultClientConfig() *clientConfig {
	return &clientConfig{
		logger: zerolog.Nop(),
		// 8 sessions is a sensible default: large enough to keep a
		// moderate-concurrency Go service from blocking on Borrow,
		// small enough that cluster-side session overhead (auth,
		// catalog context, temp-view namespace) stays bounded.
		sessionPoolSize: 8,
		// 4 concurrent fast-path ingests is the ceiling where S3
		// upload throughput typically saturates before Go-side
		// backpressure kicks in. Going higher tends to OOM the
		// process on bursty producers without improving throughput,
		// because parquet row groups buffer in memory between rotations.
		maxInflightIngests: 4,
		// 128 MiB is the Hive/Spark-partition-friendly part size. Below
		// this, Spark executor parallelism dominates; above, metadata
		// overhead grows. Matches internal/fastpath.DefaultTargetBytes.
		fastPathThreshold: 128 << 20,
		// ZSTD is 2-3x smaller than Snappy on the row shapes that
		// matter (KSUIDs, repeated S3 path strings) at similar CPU
		// cost. All major readers (PyArrow, Spark, DuckDB) support it
		// natively. See internal/fastpath/partition_writer.go.
		compression: CompressionZSTD,
		// 24h covers end-of-day retry loops without keeping
		// idempotency-token rows around forever. Tunable because
		// "nightly retry window" differs by team.
		idempotencyTTL: 24 * time.Hour,
		// Defaults match lake-k8s local stack: no named catalog, the
		// conventional "default" database. Production callers override
		// via WithDefaultCatalog / WithDefaultDatabase.
		defaultCatalog:  "",
		defaultDatabase: "default",
	}
}

// WithLogger sets the zerolog logger for the Client. Passed into every
// driver/format/backend component via the plumbing in lakeorm.Open.
func WithLogger(l zerolog.Logger) ClientOption {
	return func(c *clientConfig) { c.logger = l }
}

// WithSessionPoolSize controls how many Spark Connect sessions the
// Client borrows in flight. Default 8 — large enough for moderate
// concurrency, small enough to bound cluster-side overhead. Increase
// for high-fanout services; decrease on tight clusters.
func WithSessionPoolSize(n int) ClientOption {
	return func(c *clientConfig) {
		if n > 0 {
			c.sessionPoolSize = n
		}
	}
}

// WithMaxInflightIngests caps the number of fast-path ingests that can
// be running concurrently. The bounded semaphore propagates backpressure
// into Write() — without it a bursty Go producer OOMs the process even
// when the partition writer "flushes correctly." Default 4.
func WithMaxInflightIngests(n int) ClientOption {
	return func(c *clientConfig) {
		if n > 0 {
			c.maxInflightIngests = n
		}
	}
}

// WithFastPathThreshold is the advisory byte crossover the Dialect uses
// to choose between gRPC and object-storage ingest. Default 128 MiB.
func WithFastPathThreshold(bytes int) ClientOption {
	return func(c *clientConfig) {
		if bytes > 0 {
			c.fastPathThreshold = bytes
		}
	}
}

// WithCompression selects the compression codec for fast-path parquet
// parts. Default ZSTD — 2-3x smaller than Snappy on the shapes we care
// about (KSUIDs, repeated S3 paths), natively readable by all engines.
func WithCompression(c Compression) ClientOption {
	return func(cc *clientConfig) { cc.compression = c }
}

// WithDefaultIdempotencyTTL sets how long idempotency tokens remain
// valid in the dedup table. Default 24h.
func WithDefaultIdempotencyTTL(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.idempotencyTTL = d }
}

// WithDefaultCatalog / WithDefaultDatabase set fallbacks for unqualified
// table names.
func WithDefaultCatalog(name string) ClientOption {
	return func(c *clientConfig) { c.defaultCatalog = name }
}

func WithDefaultDatabase(name string) ClientOption {
	return func(c *clientConfig) { c.defaultDatabase = name }
}

// InsertOption configures a single Insert call.
type InsertOption func(*insertConfig)

type insertConfig struct {
	idempotencyKey string
	path           WritePath
	onConflict     ConflictStrategy
}

// ViaGRPC forces the direct gRPC ingest path regardless of batch size.
// Useful when latency matters more than throughput.
func ViaGRPC() InsertOption {
	return func(c *insertConfig) { c.path = WritePathGRPC }
}

// ViaObjectStorage forces the S3-Parquet fast path regardless of batch
// size. Useful for benchmarks or when you know the batch is large.
func ViaObjectStorage() InsertOption {
	return func(c *insertConfig) { c.path = WritePathObjectStorage }
}

// WithIdempotencyKey sets the idempotency token. If omitted, a
// SortableID is generated automatically.
func WithIdempotencyKey(key string) InsertOption {
	return func(c *insertConfig) { c.idempotencyKey = key }
}

// OnConflict sets the behaviour for primary-key collisions.
func OnConflict(strategy ConflictStrategy) InsertOption {
	return func(c *insertConfig) { c.onConflict = strategy }
}

// ConflictStrategy for Insert.
type ConflictStrategy int

const (
	ErrorOnConflict ConflictStrategy = iota
	IgnoreOnConflict
	UpdateOnConflict
)

