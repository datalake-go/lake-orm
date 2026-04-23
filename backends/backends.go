// Package backends defines the Backend interface — the abstraction
// lake-orm uses to put bytes somewhere — and exposes the public
// constructors for every concrete backend in a single, ergonomic
// surface.
//
// Each concrete backend lives in its own sub-package (s3, gcs, file,
// memory) and owns its SDK dependency directly; the constructors
// below re-export them so callers write `backends.S3(...)` rather
// than `s3.New(...)`. The sub-packages remain importable directly
// when callers want to reach backend-specific knobs (e.g. S3
// multipart tuning) without growing this re-export surface.
//
// Why this lives here and not at the root: the Backend interface is
// a contract shared between the Dialect (which plans staging-based
// writes through it) and the driver (which commits the staged
// parquet after the partition writer emits it). Putting the contract
// next to its implementations means a reader opening `backends/`
// sees the whole concept — the interface, every v0 impl, and the
// ergonomic constructors — in one place. The root package stays
// lean.
package backends

import (
	"context"
	"io"

	"github.com/datalake-go/lake-orm/backends/file"
	"github.com/datalake-go/lake-orm/backends/gcs"
	"github.com/datalake-go/lake-orm/backends/memory"
	"github.com/datalake-go/lake-orm/backends/s3"
	"github.com/datalake-go/lake-orm/types"
)

// Backend is where the bytes live. Each concrete backend owns its
// SDK directly (aws-sdk-go-v2 for S3, cloud.google.com/go/storage for
// GCS, stdlib for File / Memory) — no generic abstraction layer.
type Backend interface {
	Name() string

	// RootURI returns the URI that the Driver will interpolate into
	// its SQL. Client and Driver must resolve this string to the same
	// physical storage; endpoint/credential differences are handled
	// per-actor, not in Backend.
	RootURI() string

	TableLocation(tableName string) types.Location
	StagingPrefix(ingestID string) string

	// StagingLocation returns the absolute URI that Spark should read
	// from (e.g. "s3a://bucket/lake/staging/<id>"). Distinct from
	// StagingPrefix — Spark's Hadoop-AWS integration requires s3a://
	// even though the Backend's own SDK calls use s3://. The scheme
	// translation happens here, not in the Dialect.
	StagingLocation(ingestID string) types.Location

	Writer(ctx context.Context, key string) (io.WriteCloser, error)
	Reader(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)

	// CleanupStaging removes every object under prefix. Called by
	// Finalizer.Abort and by the staging-TTL janitor.
	CleanupStaging(ctx context.Context, prefix string) error
}

// S3 constructs an S3-compatible Backend from a DSN. See backends/s3
// for the full DSN grammar (endpoint, path_style, access_key, secret_key,
// session_token, region).
func S3(dsn string) (Backend, error) { return s3.New(dsn) }

// GCS constructs a GCS Backend stub. v0 stub — real SDK in v1+.
func GCS(dsn string) (Backend, error) { return gcs.New(dsn) }

// File constructs a local-filesystem Backend rooted at path. Creates
// the directory if needed.
func File(path string) (Backend, error) { return file.New(path) }

// Memory constructs an in-memory Backend. The name is only used for
// RootURI() rendering — it does not have to correspond to anything
// real. Intended for tests and quick local smoke runs.
func Memory(name string) Backend { return memory.New(name) }
