package parquet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"

	pq "github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog"
)

// These tests pin the parquet-go behaviors PartitionWriter depends on.
// If parquet-go ever regresses any of them, the byte-threshold check
// in PartitionWriter.Write silently stops working and the writer OOMs
// on bursty producers. Cheap to keep them here.

// TestParquetWriterBufferReflectsRows: Write() alone leaves buf.Len()
// at 0 because parquet-go parks rows in column buffers, but a
// follow-up Flush() emits the row group. If parquet-go ever changes
// to auto-flush on Write or to never emit until Close, we need to
// know immediately — the byte threshold stops tripping.
func TestParquetWriterBufferReflectsRows(t *testing.T) {
	type row struct {
		ID           string `parquet:"id"`
		FilePath     string `parquet:"file_path"`
		HasFakeVideo bool   `parquet:"has_fake_video"`
		DurationMs   int64  `parquet:"duration_ms"`
	}

	const numRows = 100_000
	rows := make([]row, numRows)
	for i := range rows {
		rows[i] = row{
			ID:           "3CSLfoj0JdmsnLNW3LCMi0596TW",
			FilePath:     "s3://example-bucket/some/long/path/vid_0000000.mp4",
			HasFakeVideo: i%2 == 0,
			DurationMs:   int64(5000 + i),
		}
	}

	buf := &bytes.Buffer{}
	w := pq.NewGenericWriter[row](buf)

	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatalf("buf.Len()=0 after Write+Flush — partition byte threshold can never trip")
	}
}

// TestParquetWriterFlushIsMonotonic: after each Write+Flush, buf.Len()
// must grow strictly. If parquet-go ever rewrites in place (e.g.
// compacting across row groups), the threshold check would see a
// shrinking value and miss the trigger.
func TestParquetWriterFlushIsMonotonic(t *testing.T) {
	type row struct {
		ID    string `parquet:"id"`
		Value int64  `parquet:"value"`
	}

	const batchSize = 5_000
	const batches = 10

	buf := &bytes.Buffer{}
	w := pq.NewGenericWriter[row](buf)

	prev := 0
	for i := 0; i < batches; i++ {
		batch := make([]row, batchSize)
		for j := range batch {
			batch[j] = row{ID: "abc", Value: int64(i*batchSize + j)}
		}
		if _, err := w.Write(batch); err != nil {
			t.Fatalf("batch %d write: %v", i, err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("batch %d flush: %v", i, err)
		}
		curr := buf.Len()
		if curr <= prev {
			t.Errorf("batch %d: buf.Len() did not grow — prev=%d curr=%d", i, prev, curr)
		}
		prev = curr
	}
}

// TestParquetWriterRoundTrip: calling Flush per Write must not corrupt
// the parquet file — closing and re-reading yields exactly the rows
// that went in, in order. Small row groups are valid parquet; this
// test pins that before we ship the per-write Flush.
func TestParquetWriterRoundTrip(t *testing.T) {
	type row struct {
		ID    string `parquet:"id"`
		Value int64  `parquet:"value"`
	}

	const numRows = 1_000
	want := make([]row, numRows)
	for i := range want {
		want[i] = row{ID: "row", Value: int64(i)}
	}

	buf := &bytes.Buffer{}
	w := pq.NewGenericWriter[row](buf)

	for _, chunk := range [][]row{want[:300], want[300:700], want[700:]} {
		if _, err := w.Write(chunk); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r := pq.NewGenericReader[row](bytes.NewReader(buf.Bytes()))
	defer r.Close()

	got := make([]row, numRows)
	n, err := r.Read(got)
	if err != nil && n != numRows {
		t.Fatalf("read: n=%d err=%v", n, err)
	}
	if n != numRows {
		t.Fatalf("read %d rows, want %d", n, numRows)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestParquetWriterTinyByteThresholdTrips mirrors PartitionWriter's
// real check pattern with a tiny threshold. Without per-write Flush
// this loop never trips; with the fix in place it has to trip on the
// first observation after Write.
func TestParquetWriterTinyByteThresholdTrips(t *testing.T) {
	type row struct {
		ID    string `parquet:"id"`
		Value int64  `parquet:"value"`
	}

	const batchSize = 100
	const threshold = 1_024
	const maxBatches = 50

	buf := &bytes.Buffer{}
	w := pq.NewGenericWriter[row](buf)

	tripped := false
	for i := 0; i < maxBatches; i++ {
		batch := make([]row, batchSize)
		for j := range batch {
			batch[j] = row{ID: "abc", Value: int64(i*batchSize + j)}
		}
		if _, err := w.Write(batch); err != nil {
			t.Fatalf("batch %d write: %v", i, err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("batch %d flush: %v", i, err)
		}
		if buf.Len() >= threshold {
			t.Logf("threshold tripped after batch %d at %d bytes", i, buf.Len())
			tripped = true
			break
		}
	}
	if !tripped {
		t.Errorf("byte threshold of %d never tripped after %d batches of %d rows",
			threshold, maxBatches, batchSize)
	}
}

// TestPartitionWriterFlushesAtByteThreshold is the end-to-end
// regression: small TargetBytes forces several rotations, and the
// union of uploaded parts must be a byte-for-byte reconstruction of
// what went in, in order.
//
// Uses an in-memory uploader so no external storage is required.
func TestPartitionWriterFlushesAtByteThreshold(t *testing.T) {
	type row struct {
		ID    string `parquet:"id"`
		Value int64  `parquet:"value"`
	}

	up := newMemUploader()
	schema := pq.SchemaOf(new(row))

	pw := NewPartitionWriter(
		context.Background(),
		up,
		"staging/test",
		schema,
		nil, // no conversion — user struct matches schema
		Config{TargetBytes: 8 << 10, TargetRows: 1_000_000},
		zerolog.Nop(),
	)

	const numRows = 50_000
	want := make([]row, numRows)
	for i := range want {
		want[i] = row{ID: fmt.Sprintf("id-%07d", i), Value: int64(i)}
	}

	const batchSize = 1_000
	for start := 0; start < numRows; start += batchSize {
		end := start + batchSize
		if end > numRows {
			end = numRows
		}
		batch := make([]any, 0, end-start)
		for _, r := range want[start:end] {
			batch = append(batch, r)
		}
		if err := pw.Write(batch); err != nil {
			t.Fatalf("write batch %d-%d: %v", start, end, err)
		}
	}
	if err := pw.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	if pw.PartCount() < 2 {
		t.Fatalf("expected the byte threshold to trip multiple times — got %d parts. "+
			"Either parquet-go is buffering everything again, or the threshold logic regressed.",
			pw.PartCount())
	}
	if pw.TotalRows() != numRows {
		t.Errorf("TotalRows()=%d, want %d", pw.TotalRows(), numRows)
	}

	keys := up.keys()
	sort.Strings(keys)
	if len(keys) != pw.PartCount() {
		t.Fatalf("upload count mismatch: uploader has %d keys, writer reports %d parts",
			len(keys), pw.PartCount())
	}

	got := make([]row, 0, numRows)
	for _, key := range keys {
		body := up.get(key)
		r := pq.NewGenericReader[row](bytes.NewReader(body))
		partRows := make([]row, r.NumRows())
		n, err := r.Read(partRows)
		if err != nil && n != int(r.NumRows()) {
			r.Close()
			t.Fatalf("read part %s: n=%d err=%v", key, n, err)
		}
		r.Close()
		got = append(got, partRows...)
	}
	if len(got) != numRows {
		t.Fatalf("union of parts has %d rows, want %d", len(got), numRows)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d differs: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// memUploader is a trivial in-memory Uploader so the regression suite
// runs without external storage.
type memUploader struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemUploader() *memUploader {
	return &memUploader{objects: map[string][]byte{}}
}

func (m *memUploader) Writer(_ context.Context, key string) (io.WriteCloser, error) {
	return &memWriteCloser{mu: &m.mu, target: m.objects, key: key}, nil
}

func (m *memUploader) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.objects))
	for k := range m.objects {
		out = append(out, k)
	}
	return out
}

func (m *memUploader) get(key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.objects[key]...)
}

type memWriteCloser struct {
	mu     *sync.Mutex
	target map[string][]byte
	key    string
	buf    bytes.Buffer
}

func (w *memWriteCloser) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *memWriteCloser) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.target[w.key] = append([]byte(nil), w.buf.Bytes()...)
	return nil
}
