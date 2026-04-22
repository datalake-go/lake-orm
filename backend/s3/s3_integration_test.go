//go:build integration

package s3_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/datalake-go/lake-orm/backend/s3"
	"github.com/datalake-go/lake-orm/testutils"
)

// TestS3Backend_RoundTrip is the minimum-viable integration test: put
// a few keys, list them, read one back, delete, list again. Runs
// against a MinIO testcontainer via testutils.NewMinIO. Gated behind
// the `integration` build tag so `go test ./...` skips it by default;
// run with `go test -tags=integration ./backend/s3/...` or
// `make integration-test`.
func TestS3Backend_RoundTrip(t *testing.T) {
	t.Parallel()
	minio := testutils.NewMinIO(t)
	bucket := minio.NewBucket(t)
	dsn := minio.DSN(bucket, "lake")

	backend, err := s3.New(dsn)
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}

	ctx := context.Background()

	// Write — uses the same code path the fast-path partition writer
	// hits, so regressions in the multipart/PutObject adapter show up
	// here first.
	key := "lake/staging/round-trip/part-00000.parquet"
	w, err := backend.Writer(ctx, key)
	if err != nil {
		t.Fatalf("backend.Writer: %v", err)
	}
	payload := bytes.Repeat([]byte("lakeorm"), 256) // 1 KiB
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// List — covers the ListObjectsV2 pagination wrapper.
	keys, err := backend.List(ctx, "lake/staging/round-trip/")
	if err != nil {
		t.Fatalf("backend.List: %v", err)
	}
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("List returned %v, want [%s]", keys, key)
	}

	// Read — covers the GetObject adapter.
	r, err := backend.Reader(ctx, key)
	if err != nil {
		t.Fatalf("backend.Reader: %v", err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip body mismatch: got %d bytes want %d", len(got), len(payload))
	}

	// CleanupStaging — covers the List+Delete fan-out used by
	// Finalizer.Abort and the staging janitor.
	if err := backend.CleanupStaging(ctx, "lake/staging/round-trip/"); err != nil {
		t.Fatalf("CleanupStaging: %v", err)
	}
	keys, err = backend.List(ctx, "lake/staging/round-trip/")
	if err != nil {
		t.Fatalf("backend.List after cleanup: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("CleanupStaging left %v behind", keys)
	}
}

// TestS3Backend_RootURI pins the URI the driver interpolates into SQL.
// If RootURI's rendering changes, every Iceberg / Delta Dialect
// implementation that embeds it in `LOCATION 's3a://...'` breaks
// silently. Cheap to pin.
func TestS3Backend_RootURI(t *testing.T) {
	t.Parallel()
	minio := testutils.NewMinIO(t)
	bucket := minio.NewBucket(t)

	b, err := s3.New(minio.DSN(bucket, "lake"))
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	want := "s3://" + bucket + "/lake"
	if got := b.RootURI(); got != want {
		t.Errorf("RootURI() = %q, want %q", got, want)
	}
}
