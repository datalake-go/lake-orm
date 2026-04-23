package lakeorm

import (
	"context"
	"fmt"
	"iter"

	"github.com/datalake-go/lake-orm/drivers"
)

// Query runs source against db's driver and materialises every
// decoded row into []T. The driver must implement drivers.Convertible
// (all v0 drivers do).
//
// Source is driver-native — build one with the driver's own
// conversion helper:
//
//	drv := db.Driver().(*spark.Driver)
//	users, err := lakeorm.Query[User](ctx, db,
//	    drv.FromSQL("SELECT * FROM users WHERE country = ?", "UK"))
//
// DuckDB / Databricks use the same shape with their own FromSQL /
// FromRows / FromTable helpers; anything the helpers don't cover
// can be expressed as a bare drivers.Source closure.
//
// The `spark:"..."` / `lake:"..."` tags on T bind result columns
// to fields. A field in T the source doesn't project surfaces as a
// schema-mismatch error.
func Query[T any](ctx context.Context, db Client, source drivers.Source) ([]T, error) {
	conv, err := convertibleFor(db)
	if err != nil {
		return nil, err
	}
	var out []T
	if err := conv.Collect(ctx, source, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// QueryStream runs source and yields T values one at a time with
// constant memory. Rangeable via Go 1.23's iter.Seq2:
//
//	drv := db.Driver().(*spark.Driver)
//	for row, err := range lakeorm.QueryStream[User](ctx, db,
//	    drv.FromSQL("SELECT * FROM users")) {
//	    if err != nil { break }
//	    // use row
//	}
//
// Schema mismatch surfaces through the iterator's error channel on
// the first row.
func QueryStream[T any](ctx context.Context, db Client, source drivers.Source) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		conv, err := convertibleFor(db)
		if err != nil {
			yield(zero, err)
			return
		}
		var sample T
		for v, rerr := range conv.Stream(ctx, source, &sample) {
			if rerr != nil {
				if !yield(zero, rerr) {
					return
				}
				continue
			}
			tv, ok := v.(T)
			if !ok {
				if !yield(zero, fmt.Errorf("lakeorm: stream produced %T, want %T", v, zero)) {
					return
				}
				continue
			}
			if !yield(tv, nil) {
				return
			}
		}
	}
}

// QueryFirst runs source and returns the first decoded row, or
// errors.ErrNoRows if source yielded zero rows.
func QueryFirst[T any](ctx context.Context, db Client, source drivers.Source) (*T, error) {
	conv, err := convertibleFor(db)
	if err != nil {
		return nil, err
	}
	var out T
	if err := conv.First(ctx, source, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// convertibleFor returns the driver's Convertible capability or
// an error naming the driver that failed to implement it. Every
// v0 driver (spark, databricks, databricksconnect, duckdb) does;
// this check is defensive against third-party drivers that don't.
func convertibleFor(db Client) (drivers.Convertible, error) {
	d := db.Driver()
	conv, ok := d.(drivers.Convertible)
	if !ok {
		return nil, fmt.Errorf("lakeorm: driver %q does not implement drivers.Convertible", d.Name())
	}
	return conv, nil
}
