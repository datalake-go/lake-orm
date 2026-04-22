package lakeorm

import (
	"context"
	"fmt"
	"iter"

	sparksql "github.com/datalake-go/spark-connect-go/spark/sql"

	"github.com/datalake-go/lake-orm/internal/scanner"
	lkerrors "github.com/datalake-go/lake-orm/errors"
)

// CollectAs materialises every row of df into []T. Uses the Spark-
// native fast path (`sparksql.Collect[T]`) when the DataFrame came
// from a Spark-family driver; falls back to a generic path that
// iterates `df.Stream(ctx)` through lakeorm's reflection scanner
// for every other driver (DuckDB, Databricks native, any future
// *sql.DB-shaped driver).
//
// Equivalent to:
//
//	df, _ := db.DataFrame(ctx, sql, args...)
//	rows, _ := lakeorm.CollectAs[Row](ctx, df)
//
// Use when the result shape is known at compile time — the
// `lake:"..."` / `lakeorm:"..."` / `spark:"..."` tags on T bind
// result columns to fields.
func CollectAs[T any](ctx context.Context, df DataFrame) ([]T, error) {
	if df == nil {
		return nil, fmt.Errorf("lakeorm: %w (DataFrame is nil)", lkerrors.ErrDriverMismatch)
	}
	if sparkDF, ok := sparkDataFrame(df); ok {
		return sparksql.Collect[T](ctx, sparkDF)
	}
	return collectViaStream[T](ctx, df)
}

// StreamAs yields decoded T values one at a time. Constant memory
// regardless of result size. Range directly:
//
//	for row, err := range lakeorm.StreamAs[User](ctx, df) { ... }
//
// Uses Spark's native stream when available; otherwise iterates
// through the Driver-agnostic Row stream + reflection scanner.
// Schema binding happens on the first row; a mismatch surfaces
// through the error channel on the first iteration.
func StreamAs[T any](ctx context.Context, df DataFrame) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		if df == nil {
			yield(zero, fmt.Errorf("lakeorm: %w (DataFrame is nil)", lkerrors.ErrDriverMismatch))
			return
		}
		if sparkDF, ok := sparkDataFrame(df); ok {
			for row, rerr := range sparksql.Stream[T](ctx, sparkDF) {
				if !yield(row, rerr) {
					return
				}
			}
			return
		}
		streamViaStream[T](ctx, df, yield)
	}
}

// FirstAs returns the first row of df decoded as T, or lkerrors.ErrNoRows
// if the DataFrame produced no rows.
func FirstAs[T any](ctx context.Context, df DataFrame) (*T, error) {
	if df == nil {
		return nil, fmt.Errorf("lakeorm: %w (DataFrame is nil)", lkerrors.ErrDriverMismatch)
	}
	if sparkDF, ok := sparkDataFrame(df); ok {
		row, err := sparksql.First[T](ctx, sparkDF)
		if err != nil {
			if err == sparksql.ErrNotFound {
				return nil, lkerrors.ErrNoRows
			}
			return nil, err
		}
		return row, nil
	}
	return firstViaStream[T](ctx, df)
}

// sparkDataFrame reports whether df's driver handle is a Spark
// DataFrame, returning the typed handle when it is. Non-Spark
// drivers return (nil, false) and the caller takes the generic
// fallback path.
func sparkDataFrame(df DataFrame) (sparksql.DataFrame, bool) {
	if df == nil {
		return nil, false
	}
	sparkDF, ok := df.DriverType().(sparksql.DataFrame)
	return sparkDF, ok
}

// collectViaStream is the driver-agnostic CollectAs — walks
// df.Stream(ctx) and scans each Row into a fresh T via the
// reflection scanner shared with Client.Query.
func collectViaStream[T any](ctx context.Context, df DataFrame) ([]T, error) {
	sc := scanner.NewScanner()
	var out []T
	for row, rerr := range df.Stream(ctx) {
		if rerr != nil {
			return out, rerr
		}
		var dest T
		if err := sc.ScanRow(row.Columns(), row.Values(), &dest); err != nil {
			return out, err
		}
		out = append(out, dest)
	}
	return out, nil
}

// streamViaStream is the driver-agnostic StreamAs body. Takes the
// yield function directly so the outer iter.Seq2 wrapper stays in
// StreamAs and this helper doesn't allocate another closure.
func streamViaStream[T any](ctx context.Context, df DataFrame, yield func(T, error) bool) {
	sc := scanner.NewScanner()
	var zero T
	for row, rerr := range df.Stream(ctx) {
		if rerr != nil {
			if !yield(zero, rerr) {
				return
			}
			continue
		}
		var dest T
		if err := sc.ScanRow(row.Columns(), row.Values(), &dest); err != nil {
			if !yield(zero, err) {
				return
			}
			continue
		}
		if !yield(dest, nil) {
			return
		}
	}
}

// firstViaStream is the driver-agnostic FirstAs. Returns lkerrors.ErrNoRows
// when the stream is empty. Terminates the iteration after the
// first successful row.
func firstViaStream[T any](ctx context.Context, df DataFrame) (*T, error) {
	sc := scanner.NewScanner()
	for row, rerr := range df.Stream(ctx) {
		if rerr != nil {
			return nil, rerr
		}
		var dest T
		if err := sc.ScanRow(row.Columns(), row.Values(), &dest); err != nil {
			return nil, err
		}
		return &dest, nil
	}
	return nil, lkerrors.ErrNoRows
}
