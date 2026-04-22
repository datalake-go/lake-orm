// Package drivers is the contract shared by every lake-orm driver.
// A Driver is what ships writes to a lakehouse engine and decodes
// reads back into user-declared Go types; concrete implementations
// for Spark Connect, DuckDB, Databricks SQL, and Databricks Connect
// sit in sibling sub-packages.
//
// Convertible is the read-side capability. It exists so lake-orm
// never has to own a query grammar: a caller hands the driver a
// closure that produces the driver's native row source (a Spark
// DataFrame, a *sql.Rows, an Arrow Record, whatever the driver
// decides is canonical), and the driver decodes each row into the
// user-supplied Go type. Drivers that implement Convertible
// participate in lakeorm.Query[T] / QueryStream[T] / QueryFirst[T].
//
// Per-driver conversion helpers (the FromSQL / FromDataFrame /
// FromRows / FromTable / FromRow families in each sibling driver
// package) build the closure for common cases so callers write one
// line instead of six. Anything the helpers don't cover can be
// expressed as a bare closure — five lines of glue at the call site,
// no framework primitives.
package drivers

import (
	"context"
	"iter"
)

// Convertible is the optional driver capability: given a Native
// function that produces the driver's own row source, decode each
// row into the user-supplied Go type.
//
// Three methods match the three typed-read shapes the top-level
// lakeorm helpers expose:
//
//   - Collect writes every decoded row into *[]T.
//   - First writes the first decoded row into *T (or returns
//     errors.ErrNoRows if the source yielded zero rows).
//   - Stream yields decoded rows one at a time as the driver walks
//     its native source; constant memory regardless of result size.
//
// The source closure returns any because different drivers return
// different native types — sparksql.DataFrame for Spark,
// *sql.Rows for DuckDB / Databricks SQL, etc. The driver's
// Convertible implementation knows how to decode ITS OWN native
// type into T and fails fast with a typed error when the closure
// hands back a type it doesn't recognise.
//
// The out parameter on Collect / First uses reflection to discover
// T — callers pass &[]T or &T, the driver reflects to find the
// element type, and drives decoding from there. Stream takes a
// sample *T (a pointer to a zero value) for the same reason.
type Convertible interface {
	// Collect runs source(ctx) to obtain a native row source and
	// decodes every row into out, which must be a *[]T.
	Collect(ctx context.Context, source func(context.Context) (any, error), out any) error

	// First runs source(ctx), decodes the first row into out
	// (a *T), and returns errors.ErrNoRows if the source is empty.
	First(ctx context.Context, source func(context.Context) (any, error), out any) error

	// Stream runs source(ctx) and yields each decoded row. sample
	// is a *T pointing at a zero value; the driver uses reflect
	// to discover T and yields each successive row as an any that
	// the caller type-asserts back to T. Constant memory.
	Stream(ctx context.Context, source func(context.Context) (any, error), sample any) iter.Seq2[any, error]
}
