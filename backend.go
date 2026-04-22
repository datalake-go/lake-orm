package lakeorm

import (
	"context"
	"io"

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
