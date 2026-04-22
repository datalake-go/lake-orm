package parquet

import (
	"bytes"
	"context"
	"fmt"
	"io"

	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/rs/zerolog"
)

// zstdCodec is the default compression. ZSTD runs ~2-3× smaller than
// Snappy on common lakehouse row shapes (high-cardinality keys,
// repeated path strings) at similar CPU cost, and every engine Spark
// hands the files to (PyArrow, Spark itself, DuckDB, Trino) reads it
// natively. Fewer bytes uploaded means faster object-storage PUTs and
// fewer GET round-trips downstream.
var zstdCodec = &zstd.Codec{}

// Default thresholds. Whichever trips first rotates a part.
//
// Bytes is the primary threshold — 128 MiB matches the Hive-style
// part size that PyArrow/Spark/DuckDB all tune for. Measured after
// serialization so it tracks actual on-disk size regardless of row
// width or compression ratio.
//
// Rows is the safety backstop — forces rotation at 5M rows even when
// compression keeps us under the byte cap. Prevents any single part
// from growing unbounded on highly-compressible data.
const (
	DefaultTargetBytes = 128 << 20
	DefaultTargetRows  = 5_000_000
)

// Config controls when a part rotates. Zero values use the defaults
// above.
type Config struct {
	TargetBytes int
	TargetRows  int
	// Compression is the parquet-go codec. nil falls back to ZSTD.
	Compression compress.Codec
}

func (c Config) bytes() int {
	if c.TargetBytes > 0 {
		return c.TargetBytes
	}
	return DefaultTargetBytes
}

func (c Config) rows() int {
	if c.TargetRows > 0 {
		return c.TargetRows
	}
	return DefaultTargetRows
}

func (c Config) codec() compress.Codec {
	if c.Compression != nil {
		return c.Compression
	}
	return zstdCodec
}

func (c Config) writerOption() pq.WriterOption {
	return pq.Compression(c.codec())
}

// Uploader is the minimal interface PartitionWriter needs from a
// Backend — open a writable stream at `key`. Any lakeorm.Backend
// satisfies it; tests pass an in-memory stub.
type Uploader interface {
	Writer(ctx context.Context, key string) (io.WriteCloser, error)
}

// RowConverter is the pluggable hook that turns a user's typed row
// (whatever struct shape the caller has) into the row parquet-go
// should write against the schema. When the schema was synthesized
// from lake tags on a different struct type, the converter projects
// user-struct fields into the synthesized struct. When the schema
// matches the user's struct directly, the converter is a no-op.
type RowConverter func(row any) any

// PartitionWriter streams rows into a live parquet writer backed by a
// bytes buffer. When the buffer or row count crosses the configured
// threshold, the writer closes the current part, uploads it, and
// opens a new one. Only the parquet column buffers stay in memory —
// not the raw row structs.
type PartitionWriter struct {
	ctx        context.Context
	uploader   Uploader
	prefix     string
	cfg        Config
	schema     *pq.Schema
	convert    RowConverter
	buf        *bytes.Buffer
	writer     *pq.Writer
	partNum    int
	rowsInPart int
	totalRows  int
	logger     zerolog.Logger
	partKeys   []string
}

// NewPartitionWriter constructs a writer against the given schema.
// The caller supplies:
//
//   - schema:  the parquet schema produced by lakeorm.BuildParquetSchema
//     (or any other *pq.Schema whose shape matches the rows
//     the converter returns).
//   - convert: maps a user-struct row into a row of the shape the
//     schema expects. Pass the identity function if the user
//     struct already matches the schema.
func NewPartitionWriter(
	ctx context.Context,
	uploader Uploader,
	prefix string,
	schema *pq.Schema,
	convert RowConverter,
	cfg Config,
	logger zerolog.Logger,
) *PartitionWriter {
	if convert == nil {
		convert = func(row any) any { return row }
	}
	buf := &bytes.Buffer{}
	return &PartitionWriter{
		ctx:      ctx,
		uploader: uploader,
		prefix:   prefix,
		cfg:      cfg,
		schema:   schema,
		convert:  convert,
		buf:      buf,
		writer:   pq.NewWriter(buf, schema, cfg.writerOption()),
		logger:   logger,
	}
}

// Write streams rows into the current part and rotates if the buffer
// or row count crossed the configured thresholds.
//
// Implementation note (CRITICAL): writer.Flush() MUST be called before
// checking buf.Len(). parquet-go holds rows in column buffers and
// only emits to the underlying io.Writer when a row group closes —
// without the explicit flush, buf.Len() stays at 0 across writes and
// the byte threshold never trips. Row groups end up smaller than if
// we closed them naturally, which compresses slightly worse — a price
// paid to make the byte threshold actually work, and thereby prevent
// unbounded memory growth on bursty producers.
func (pw *PartitionWriter) Write(rows []any) error {
	if len(rows) == 0 {
		return nil
	}
	for _, r := range rows {
		if r == nil {
			continue
		}
		if err := pw.writer.Write(pw.convert(r)); err != nil {
			return fmt.Errorf("write parquet row: %w", err)
		}
	}
	if err := pw.writer.Flush(); err != nil {
		return fmt.Errorf("flush parquet row group: %w", err)
	}
	pw.rowsInPart += len(rows)
	if pw.buf.Len() >= pw.cfg.bytes() || pw.rowsInPart >= pw.cfg.rows() {
		return pw.rotate()
	}
	return nil
}

// Flush closes the current part (if it has rows) and uploads it. Safe
// to call with zero rows buffered.
func (pw *PartitionWriter) Flush() error {
	if pw.rowsInPart == 0 {
		return nil
	}
	return pw.rotate()
}

func (pw *PartitionWriter) rotate() error {
	if err := pw.writer.Close(); err != nil {
		return fmt.Errorf("close parquet writer: %w", err)
	}

	key := fmt.Sprintf("%s/part-%05d.parquet", pw.prefix, pw.partNum)
	w, err := pw.uploader.Writer(pw.ctx, key)
	if err != nil {
		return fmt.Errorf("open writer for %s: %w", key, err)
	}
	if _, err := w.Write(pw.buf.Bytes()); err != nil {
		_ = w.Close()
		return fmt.Errorf("upload part %s: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close upload for %s: %w", key, err)
	}

	pw.logger.Debug().
		Str("key", key).
		Int("rows", pw.rowsInPart).
		Int("bytes", pw.buf.Len()).
		Msg("uploaded parquet part")

	pw.partKeys = append(pw.partKeys, key)
	pw.totalRows += pw.rowsInPart
	pw.partNum++
	pw.rowsInPart = 0
	pw.buf = &bytes.Buffer{}
	pw.writer = pq.NewWriter(pw.buf, pw.schema, pw.cfg.writerOption())
	return nil
}

// PartKeys returns the storage keys of every uploaded part. The
// Spark-side ingest statement uses the shared prefix to register them
// all at once.
func (pw *PartitionWriter) PartKeys() []string { return pw.partKeys }

// TotalRows is the cumulative row count across every flushed part —
// excludes rows still buffered in the current open part.
func (pw *PartitionWriter) TotalRows() int { return pw.totalRows }

// PartCount is the number of parts flushed so far.
func (pw *PartitionWriter) PartCount() int { return pw.partNum }
