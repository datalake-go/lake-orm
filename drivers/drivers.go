// Package drivers is the contract shared by every lake-orm driver.
// A Driver is what ships writes to a lakehouse engine and decodes
// reads back into user-declared Go types; concrete implementations
// for Spark Connect, DuckDB, Databricks SQL, and Databricks Connect
// sit in sibling sub-packages.
//
// Pre-iter.Seq2, Go had no standard way to express "a sequence of
// values you pull lazily." So if you wanted to ship a streaming API,
// you had to invent an interface, and that interface inevitably
// ended up carrying both jobs. sql.Rows is the canonical example
// but it's everywhere: bufio.Scanner, sql.Rows, *json.Decoder in
// streaming mode, every custom Iterator type in every library. They
// all conflate "I am a resource" with "I am a sequence" because the
// language didn't give you a way to separate them.
//
// Convertible is the read-side capability. It exists so lake-orm
// never has to own a query grammar, such as sql.Rows or DataFrames,
// because we want to be able to support the underlying driver
// callsites regardless of what drivers we pick.
//
// Instead, we use a source closure that produces the driver's native
// row source (a Spark DataFrame, a *sql.Rows, an Arrow Record,
// whatever the driver decides is canonical), and the driver decodes
// each row into the user-supplied Go type. Drivers that implement
// Convertible participate in lakeorm.Query[T] / QueryStream[T] /
// QueryFirst[T]. All we care about is whether our response is a
// congruent array or a stream of Ts, which I think is much better
// design than shoving a specific predicate down the throat of every
// callsite, as we have seen before.
//
// The advantage here is we can support different kinds of formats
// and provide a single contract which says "as long as you can
// return type T, I don't care what your query actually looks like".
// The contract the driver agrees to is narrow: given a Source and a
// target T, produce Ts. How the query is expressed, how rows are
// transported, and how decoding happens are all the driver's concern.
//
// Per-driver conversion helpers — FromSQL / FromDataFrame /
// FromRows / FromTable / FromRow, each a method on the concrete
// driver type — build the Source for common cases so callers write
// one line instead of six:
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
