// Package local is the lake-k8s docker-compose on-ramp. A ~15-line
// wrapper around lakeorm.Open with endpoints baked in for the local
// stack (SeaweedFS + Spark Connect on their default ports). Iceberg
// catalog is Hadoop-rooted at the SeaweedFS warehouse — no separate
// catalog service.
//
//	import (
//	    "github.com/datalake-go/lake-orm"
//	    lakeormlocal "github.com/datalake-go/lake-orm/local"
//	)
//
//	db, err := lakeormlocal.Open()
//	// ... everything else is ordinary lakeorm code
//
// Lives in a subpackage (not in the top-level `lakeorm` package) to
// avoid an import cycle with the driver/* subpackages — the real
// wiring imports every driver subpackage, each of which already
// imports the root. See lakeorm/local.go for the rationale.
package local

import (
	"fmt"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backends/s3"
	"github.com/datalake-go/lake-orm/dialects/iceberg"
	"github.com/datalake-go/lake-orm/drivers/spark"
)

// Option overrides a specific default. Rarely needed.
type Option func(*config)

type config struct {
	spark   string
	seaweed string
	bucket  string
	access  string
	secret  string
}

func defaults() *config {
	return &config{
		spark:   "sc://localhost:15002",
		seaweed: "http://localhost:8333",
		bucket:  "dorm-local",
		access:  "dorm",
		secret:  "dorm",
	}
}

func WithSpark(uri string) Option    { return func(c *config) { c.spark = uri } }
func WithSeaweed(uri string) Option  { return func(c *config) { c.seaweed = uri } }
func WithBucket(name string) Option  { return func(c *config) { c.bucket = name } }
func WithCreds(ak, sk string) Option { return func(c *config) { c.access, c.secret = ak, sk } }

// Open returns a Client configured for the lake-k8s local stack.
// Assumes the stack is running — `make docker-up` in the dorm repo or
// `docker compose up -d` in lake-k8s.
func Open(opts ...Option) (lakeorm.Client, error) {
	cfg := defaults()
	for _, o := range opts {
		o(cfg)
	}

	store, err := s3.New(fmt.Sprintf(
		"s3://%s/lake?endpoint=%s&path_style=true&access_key=%s&secret_key=%s",
		cfg.bucket, cfg.seaweed, cfg.access, cfg.secret,
	))
	if err != nil {
		return nil, fmt.Errorf("lakeormlocal: construct S3 backend: %w", err)
	}

	// Iceberg catalog wiring lives on the Spark Connect server (see
	// docker-compose.yaml) — Hadoop catalog rooted at the s3a://
	// warehouse. The Dialect just names it for qualified table refs.
	return lakeorm.Open(
		spark.Remote(cfg.spark),
		iceberg.Dialect(),
		store,
	)
}
