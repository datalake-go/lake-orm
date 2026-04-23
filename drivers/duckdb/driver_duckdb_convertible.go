package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"reflect"

	"github.com/datalake-go/lake-orm/drivers"
	lkerrors "github.com/datalake-go/lake-orm/errors"
	"github.com/datalake-go/lake-orm/internal/scanner"
)

// drivers.Convertible implementation for the DuckDB driver.
//
// Source closures produced by the helpers in
// driver_duckdb_conversions.go return a *sql.Rows. The methods
// below invoke the source, type-assert to *sql.Rows, iterate
// rows.Next() / rows.Scan(), and decode each row into the caller-
// supplied Go struct via the shared internal/scanner.
//
// Lifecycle: the source's cleanup function (returned alongside the
// *sql.Rows) is invoked via defer after iteration finishes, so
// rows.Close happens even when iteration returns early or errors.

// Compile-time assertion: *Driver satisfies drivers.Convertible.
var _ drivers.Convertible = (*Driver)(nil)

// Collect runs source and appends every decoded row into out
// (must be a *[]T).
func (d *Driver) Collect(ctx context.Context, source drivers.Source, out any) error {
	rows, cleanup, err := sqlRowsFromSource(ctx, source)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	return collectSQLRows(rows, out)
}

// First decodes the first row into out (*T) or returns
// errors.ErrNoRows.
func (d *Driver) First(ctx context.Context, source drivers.Source, out any) error {
	rows, cleanup, err := sqlRowsFromSource(ctx, source)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return rerr
		}
		return lkerrors.ErrNoRows
	}
	vals, err := scanIntoValues(rows, len(cols))
	if err != nil {
		return err
	}
	sc := scanner.NewScanner()
	return sc.ScanRow(cols, vals, out)
}

// Stream yields decoded rows one at a time. sample is a *T used to
// discover the element type via reflect.
func (d *Driver) Stream(ctx context.Context, source drivers.Source, sample any) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		rows, cleanup, err := sqlRowsFromSource(ctx, source)
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

		cols, err := rows.Columns()
		if err != nil {
			yield(nil, err)
			return
		}

		sc := scanner.NewScanner()
		for rows.Next() {
			vals, err := scanIntoValues(rows, len(cols))
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			ptr := reflect.New(elemType)
			if err := sc.ScanRow(cols, vals, ptr.Interface()); err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !yield(ptr.Elem().Interface(), nil) {
				return
			}
		}
		if rerr := rows.Err(); rerr != nil {
			yield(nil, rerr)
		}
	}
}

// sqlRowsFromSource invokes source, type-asserts the native to
// *sql.Rows, and returns it alongside the source's cleanup hook.
// Fails fast if source returns something other than *sql.Rows.
func sqlRowsFromSource(ctx context.Context, source drivers.Source) (*sql.Rows, func(), error) {
	if source == nil {
		return nil, nil, fmt.Errorf("duckdb: Convertible source is nil")
	}
	native, cleanup, err := source(ctx)
	if err != nil {
		return nil, nil, err
	}
	rows, ok := native.(*sql.Rows)
	if !ok {
		if cleanup != nil {
			cleanup()
		}
		return nil, nil, fmt.Errorf("duckdb: Convertible source returned %T, want *sql.Rows", native)
	}
	return rows, cleanup, nil
}

// collectSQLRows walks rows end-to-end and appends each decoded
// row into the slice behind out (must be *[]T).
func collectSQLRows(rows *sql.Rows, out any) error {
	outVal := reflect.ValueOf(out)
	if outVal.Kind() != reflect.Ptr || outVal.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("duckdb: out must be *[]T, got %T", out)
	}
	slice := outVal.Elem()
	elemType := slice.Type().Elem()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	sc := scanner.NewScanner()
	for rows.Next() {
		vals, err := scanIntoValues(rows, len(cols))
		if err != nil {
			return err
		}
		elem := reflect.New(elemType)
		if err := sc.ScanRow(cols, vals, elem.Interface()); err != nil {
			return err
		}
		slice.Set(reflect.Append(slice, elem.Elem()))
	}
	return rows.Err()
}

// scanIntoValues reads the current row into a fresh []any of size n.
func scanIntoValues(rows *sql.Rows, n int) ([]any, error) {
	vals := make([]any, n)
	holders := make([]any, n)
	for i := range vals {
		holders[i] = &vals[i]
	}
	if err := rows.Scan(holders...); err != nil {
		return nil, err
	}
	return vals, nil
}

// elementTypeOf unwraps *T → T via reflect. sample must be a
// pointer to a struct zero value.
func elementTypeOf(sample any) (reflect.Type, error) {
	t := reflect.TypeOf(sample)
	if t == nil || t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("duckdb: Stream sample must be *T, got %T", sample)
	}
	return t.Elem(), nil
}
