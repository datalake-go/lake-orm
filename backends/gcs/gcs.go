// Package gcs provides a GCS Backend. v0 stub — the constructor
// returns a concrete Backend whose operations all return
// ErrNotImplemented so the interface is exercisable in examples
// without requiring the real GCS SDK in v0 test paths.
//
// v1+: replace with cloud.google.com/go/storage direct, following the
// same "backend owns its SDK" discipline as backend/s3.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/datalake-go/lake-orm/types"
)

// ErrNotImplemented is returned by every operation on the v0 stub.
// Promoted to a shared sentinel in v1 when the SDK lands.
var ErrNotImplemented = errors.New("lakeorm/backend/gcs: v0 stub — bring cloud.google.com/go/storage in a v1 commit")

// Backend is the GCS implementation stub.
type Backend struct {
	bucket string
	root   string
}

// New parses "gs://bucket/prefix" and returns a stub backend.
func New(dsn string) (*Backend, error) {
	if len(dsn) < 5 || dsn[:5] != "gs://" {
		return nil, fmt.Errorf("gcs backend: expected gs:// DSN, got %q", dsn)
	}
	rest := dsn[5:]
	bucket, root := rest, ""
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			bucket = rest[:i]
			root = rest[i+1:]
			break
		}
	}
	return &Backend{bucket: bucket, root: root}, nil
}

func (b *Backend) Name() string    { return "gcs" }
func (b *Backend) RootURI() string { return "gs://" + b.bucket + "/" + b.root }

func (b *Backend) TableLocation(tableName string) types.Location {
	return types.Location{Scheme: "gs", Bucket: b.bucket, Path: b.root + "/tables/" + tableName}
}

func (b *Backend) StagingPrefix(ingestID string) string {
	return b.root + "/staging/" + ingestID
}

func (b *Backend) StagingLocation(ingestID string) types.Location {
	return types.Location{Scheme: "gs", Bucket: b.bucket, Path: b.StagingPrefix(ingestID)}
}

func (b *Backend) Writer(context.Context, string) (io.WriteCloser, error) {
	return nil, ErrNotImplemented
}

func (b *Backend) Reader(context.Context, string) (io.ReadCloser, error) {
	return nil, ErrNotImplemented
}
func (b *Backend) Delete(context.Context, string) error           { return ErrNotImplemented }
func (b *Backend) List(context.Context, string) ([]string, error) { return nil, ErrNotImplemented }
func (b *Backend) CleanupStaging(context.Context, string) error   { return ErrNotImplemented }
