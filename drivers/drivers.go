// Package drivers is the contract shared by every lake-orm driver.
// A Driver is what ships writes to a lakehouse engine and decodes
// reads back into user-declared Go types; concrete implementations
// for Spark Connect, DuckDB, Databricks SQL, and Databricks Connect
// sit in sibling sub-packages.
//
// Convertible is the read-side capability. It exists so lake-orm
// never has to own a query grammar, such as sql.Rows or Dataframes
// because we want to be able to support the underlying Driver callsites
// regardless of what drivers we decide to implement we pick.

// Insead, Source closure that produces the driver's native row source (a
// Spark DataFrame, a *sql.Rows, an Arrow Record, whatever the
// driver decides is canonical), and the driver decodes each row
// into the user-supplied Go type. Drivers that implement
// Convertible participate in lakeorm.Query[T] / QueryStream[T] /
// QueryFirst[T]. All we care about is whether our response is a 
// congruent array or a stream. Which I think is much better design.
//
// Per-driver conversion helpers — FromSQL / FromDataFrame /
// FromRows / FromTable / FromRow, each a method on the concrete
// driver type — build the Source for common cases so callers
// write one line instead of six:
//
//	drv := db.Driver().(*spark.Driver)
//	users, _ := lakeorm.Query[User](ctx, db, drv.FromSQL("SELECT * FROM users"))
//
// Anything the helpers don't cover can be expressed as a bare
// closure: five lines of glue at the call site, no framework
// primitives.
package drivers

import (
	"context"
	"iter"
)

// Source is the closure a caller hands to a Convertible read. It
// returns the driver's native row source when invoked — e.g. a
// sparksql.DataFrame for the Spark driver, a *sql.Rows for
// DuckDB / Databricks SQL. The concrete type is opaque to the
// caller; the Convertible implementation type-asserts to its own
// known native type and fails fast if something else came back.
//
// The cleanup function is the source's release hook: any resources
// acquired to produce the native (a borrowed session, an open
// *sql.Rows, a file handle) get released when the Convertible
// implementation is done iterating. May be nil when the source
// holds nothing that needs releasing. Convertible implementations
// call it via defer immediately after the source returns a
// non-nil native value, so the lifecycle always closes cleanly
// even if iteration returns early.
type Source func(ctx context.Context) (native any, cleanup func(), err error)

// Convertible is the optional driver capability: given a Source
// that produces the driver's own native row type, decode each row
// into the user-supplied Go target.
//
// The three methods match the three typed-read shapes the top-
// level lakeorm helpers expose:
//
//   - Collect walks the source end-to-end and writes every
//     decoded row into out, which must be a *[]T.
//   - First walks the source until the first row, writes the
//     decoded value into out (a *T), returns errors.ErrNoRows
//     if the source yielded zero rows.
//   - Stream yields one decoded row at a time as the driver
//     walks the source; constant memory regardless of result
//     size. sample is a *T so the driver reflects to discover T
//     once at the top of iteration.
//
// Reflection-via-out is the lingua franca between the driver and
// the typed helpers. Drivers that hold their decode path as a
// Go-generics call internally can still satisfy this interface by
// reflecting on out and dispatching.
type Convertible interface {
	Collect(ctx context.Context, source Source, out any) error
	First(ctx context.Context, source Source, out any) error
	Stream(ctx context.Context, source Source, sample any) iter.Seq2[any, error]
}
