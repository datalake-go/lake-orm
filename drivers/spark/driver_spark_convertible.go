package spark

import (
	"context"
	"fmt"
	"iter"
	"reflect"

	scsql "github.com/datalake-go/spark-connect-go/spark/sql"
	sctypes "github.com/datalake-go/spark-connect-go/spark/sql/types"

	"github.com/datalake-go/lake-orm/drivers"
	lkerrors "github.com/datalake-go/lake-orm/errors"
	"github.com/datalake-go/lake-orm/internal/scanner"
)

// drivers.Convertible implementation for the Spark driver.
//
// Source closures produced by the helpers in
// driver_spark_conversions.go return a sparksql.DataFrame. The
// methods below invoke the source, type-assert to the Spark-native
// DataFrame, walk rows via the DataFrame's StreamRows primitive,
// and decode each row into the caller-supplied Go struct via the
// shared internal/scanner.
//
// Lifecycle: the source's cleanup function is invoked via defer
// after the DataFrame is obtained, regardless of whether iteration
// completes, fails, or is interrupted. That returns any pool-
// borrowed session back to the pool at the end of the typed read.

// Compile-time assertion: *Driver satisfies drivers.Convertible.
var _ drivers.Convertible = (*Driver)(nil)

// Collect runs source and appends every decoded row into out
// (must be a *[]T).
func (d *Driver) Collect(ctx context.Context, source drivers.Source, out any) error {
	df, cleanup, err := sparkNativeFromSource(ctx, source)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	return collectSparkRows(ctx, df, out)
}

// First decodes the first row into out (*T) or returns
// errors.ErrNoRows.
func (d *Driver) First(ctx context.Context, source drivers.Source, out any) error {
	df, cleanup, err := sparkNativeFromSource(ctx, source)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	stream, err := df.StreamRows(ctx)
	if err != nil {
		return translateClusterError(err)
	}
	for row, rerr := range stream {
		if rerr != nil {
			return translateClusterError(rerr)
		}
		return scanSparkRow(row, out)
	}
	return lkerrors.ErrNoRows
}

// Stream yields decoded rows one at a time. sample is a *T used to
// discover the element type via reflect.
func (d *Driver) Stream(ctx context.Context, source drivers.Source, sample any) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		df, cleanup, err := sparkNativeFromSource(ctx, source)
		if err != nil {
			yield(nil, err)
			return
		}
		if cleanup != nil {
			defer cleanup()
		}

		elemType, err := elementTypeOf(sample)
		if err != nil {
			yield(nil, err)
			return
		}

		stream, err := df.StreamRows(ctx)
		if err != nil {
			yield(nil, translateClusterError(err))
			return
		}

		sc := scanner.NewScanner()
		for row, rerr := range stream {
			if rerr != nil {
				if !yield(nil, translateClusterError(rerr)) {
					return
				}
				continue
			}
			ptr := reflect.New(elemType)
			if err := sc.ScanRow(row.FieldNames(), row.Values(), ptr.Interface()); err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !yield(ptr.Elem().Interface(), nil) {
				return
			}
		}
	}
}

// sparkNativeFromSource invokes source, type-asserts the result to
// sparksql.DataFrame, and returns it alongside the source's cleanup
// hook (may be nil). Fails fast with a typed error when the source
// hands back something other than a Spark DataFrame.
func sparkNativeFromSource(ctx context.Context, source drivers.Source) (scsql.DataFrame, func(), error) {
	if source == nil {
		return nil, nil, fmt.Errorf("spark: Convertible source is nil")
	}
	native, cleanup, err := source(ctx)
	if err != nil {
		return nil, nil, err
	}
	df, ok := native.(scsql.DataFrame)
	if !ok {
		if cleanup != nil {
			cleanup()
		}
		return nil, nil, fmt.Errorf("spark: Convertible source returned %T, want sparksql.DataFrame", native)
	}
	return df, cleanup, nil
}

// collectSparkRows walks df.StreamRows end-to-end and appends each
// decoded row to the slice behind out (must be *[]T).
func collectSparkRows(ctx context.Context, df scsql.DataFrame, out any) error {
	outVal := reflect.ValueOf(out)
	if outVal.Kind() != reflect.Ptr || outVal.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("spark: out must be *[]T, got %T", out)
	}
	slice := outVal.Elem()
	elemType := slice.Type().Elem()
	stream, err := df.StreamRows(ctx)
	if err != nil {
		return translateClusterError(err)
	}
	sc := scanner.NewScanner()
	for row, rerr := range stream {
		if rerr != nil {
			return translateClusterError(rerr)
		}
		elem := reflect.New(elemType)
		if err := sc.ScanRow(row.FieldNames(), row.Values(), elem.Interface()); err != nil {
			return err
		}
		slice.Set(reflect.Append(slice, elem.Elem()))
	}
	return nil
}

// scanSparkRow decodes one row into dest (*T) via the shared
// reflection scanner.
func scanSparkRow(row sctypes.Row, dest any) error {
	sc := scanner.NewScanner()
	return sc.ScanRow(row.FieldNames(), row.Values(), dest)
}

// elementTypeOf unwraps *T → T via reflect. sample must be a
// pointer to a struct zero value.
func elementTypeOf(sample any) (reflect.Type, error) {
	t := reflect.TypeOf(sample)
	if t == nil || t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("spark: Stream sample must be *T, got %T", sample)
	}
	return t.Elem(), nil
}
