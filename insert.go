package lakeorm

import (
	"github.com/datalake-go/lake-orm/structs"
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/datalake-go/lake-orm/internal/parquet"
	"github.com/parquet-go/parquet-go/compress"
)

// runFastPath executes a KindParquetIngest plan: stream records
// through the parquet partition writer, flush every part, then hand
// the plan to the Driver which issues one Spark statement across the
// whole prefix.
//
// The Client's semaphore bounds concurrent fast-path ingests — without
// it a bursty producer can OOM the process even when the partition
// writer is rotating correctly, because parquet row groups buffer in
// memory between Flush calls.
func (c *client) runFastPath(ctx context.Context, plan ExecutionPlan, schema *structs.LakeSchema, records any) error {
	if err := c.sem.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("lakeorm: acquire ingest slot: %w", err)
	}
	defer c.sem.Release(1)

	// Synthesize the parquet schema from the user's lake tags. Users
	// don't declare `parquet:"..."` tags; the lake tag is authoritative.
	ps, err := parquet.NewSchema(schema)
	if err != nil {
		return fmt.Errorf("lakeorm: build parquet schema: %w", err)
	}

	pw := parquet.NewPartitionWriter(
		ctx,
		&backendUploader{b: plan.Staging.Backend},
		plan.Staging.Prefix,
		ps.Schema(),
		ps.ConverterFor(plan.IngestID),
		parquet.Config{
			TargetBytes: c.cfg.fastPathThreshold,
			TargetRows:  parquet.DefaultTargetRows,
			Compression: compressionToParquet(c.cfg.compression),
		},
		c.cfg.logger,
	)

	if err := writeRecords(pw, records); err != nil {
		return fmt.Errorf("lakeorm: write records to partitioner: %w", err)
	}
	if err := pw.Flush(); err != nil {
		return fmt.Errorf("lakeorm: flush final part: %w", err)
	}
	if pw.TotalRows() == 0 {
		return nil
	}

	plan.Staging.PartKeys = pw.PartKeys()

	_, fin, err := c.driver.Execute(ctx, plan)
	if err != nil {
		if fin != nil {
			_ = fin.Abort(ctx)
		}
		return err
	}
	if fin == nil {
		return nil
	}

	if err := fin.Commit(ctx); err != nil {
		_ = fin.Abort(ctx)
		return err
	}
	return nil
}

// backendUploader adapts a lakeorm.Backend to fastpath.Uploader.
type backendUploader struct{ b Backend }

func (u *backendUploader) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	return u.b.Writer(ctx, key)
}

// writeRecords unpacks records of any shape (*T, []*T, []T) and hands
// them to the partition writer as a flat []any slice. The writer's
// row converter (built from parquet.NewSchema) handles the
// per-row projection into the parquet schema's synthesized struct.
func writeRecords(pw *parquet.PartitionWriter, records any) error {
	rv := reflect.ValueOf(records)
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return nil
		}
		return pw.Write([]any{rv.Elem().Interface()})
	case reflect.Slice, reflect.Array:
		out := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			item := rv.Index(i)
			for item.Kind() == reflect.Ptr {
				if item.IsNil() {
					item = reflect.Value{}
					break
				}
				item = item.Elem()
			}
			if !item.IsValid() {
				continue
			}
			out = append(out, item.Interface())
		}
		return pw.Write(out)
	case reflect.Struct:
		return pw.Write([]any{rv.Interface()})
	default:
		return fmt.Errorf("lakeorm: unsupported records type %v", rv.Kind())
	}
}

// schemaFromRecords resolves the structs.LakeSchema for the element type and
// reports the record count plus a coarse bytes-per-record estimate for
// the Dialect's fast-path routing decision.
func schemaFromRecords(records any) (*structs.LakeSchema, int, int, error) {
	rv := reflect.ValueOf(records)
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, 0, 0, nil
		}
		elem := rv.Elem()
		schema, err := structs.ParseSchema(elem.Type())
		if err != nil {
			return nil, 0, 0, err
		}
		return schema, 1, estimateRowBytes(elem.Type()), nil
	case reflect.Slice, reflect.Array:
		n := rv.Len()
		elemType := rv.Type().Elem()
		for elemType.Kind() == reflect.Ptr {
			elemType = elemType.Elem()
		}
		schema, err := structs.ParseSchema(elemType)
		if err != nil {
			return nil, 0, 0, err
		}
		return schema, n, n * estimateRowBytes(elemType), nil
	case reflect.Struct:
		schema, err := structs.ParseSchema(rv.Type())
		if err != nil {
			return nil, 0, 0, err
		}
		return schema, 1, estimateRowBytes(rv.Type()), nil
	default:
		return nil, 0, 0, fmt.Errorf("lakeorm: unsupported records type %v", rv.Kind())
	}
}

// estimateRowBytes is a coarse bytes-per-record estimate for routing
// small vs bulk writes. Uses reflect.Size for fixed fields plus
// per-string-field overhead; good enough for the threshold check.
func estimateRowBytes(t reflect.Type) int {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return int(t.Size())
	}
	total := int(t.Size())
	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i).Type
		if ft.Kind() == reflect.String {
			total += 32 // arbitrary average; the real string bytes live off-struct
		}
	}
	return total
}

// compressionToParquet maps lakeorm.Compression to parquet-go's codec.
// Returning nil lets internal/parquet.Config default to ZSTD.
func compressionToParquet(_ Compression) compress.Codec {
	// v0 only wires ZSTD (via the package default). The full set —
	// Snappy / Gzip / Uncompressed — lives in parquet-go/compress/*;
	// v1 threads the selection through when the WithCompression
	// option grows teeth.
	return nil
}

func reflectTypeOf(v any) reflect.Type {
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}
