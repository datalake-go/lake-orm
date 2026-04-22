// Package backends exposes the public constructors for every Backend
// implementation in a single, ergonomic surface. Each concrete backend
// lives in its own sub-package (s3, gcs, file, memory) and owns its
// SDK dependency directly; this package re-exports the constructors
// so callers write `backends.S3(...)` rather than `s3.New(...)`.
//
// The sub-packages remain importable directly when callers want to
// reach backend-specific knobs (e.g. S3 multipart tuning) without
// growing this re-export surface.
package backends

import (
	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backends/file"
	"github.com/datalake-go/lake-orm/backends/gcs"
	"github.com/datalake-go/lake-orm/backends/memory"
	"github.com/datalake-go/lake-orm/backends/s3"
)

// S3 constructs an S3-compatible Backend from a DSN. See backend/s3
// for the full DSN grammar (endpoint, path_style, access_key, secret_key,
// session_token, region).
func S3(dsn string) (lakeorm.Backend, error) { return s3.New(dsn) }

// GCS constructs a GCS Backend stub. v0 stub — real SDK in v1+.
func GCS(dsn string) (lakeorm.Backend, error) { return gcs.New(dsn) }

// File constructs a local-filesystem Backend rooted at path. Creates
// the directory if needed.
func File(path string) (lakeorm.Backend, error) { return file.New(path) }

// Memory constructs an in-memory Backend. The name is only used for
// RootURI() rendering — it does not have to correspond to anything
// real. Intended for tests and quick local smoke runs.
func Memory(name string) lakeorm.Backend { return memory.New(name) }
