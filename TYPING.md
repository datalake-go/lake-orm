# Typing philosophy

This is a design contract, not a feature description. It records why
lakeorm and the Spark Connect fork draw the typed / untyped line
where they do, so future contributors don't "improve" the design by
moving it.

## The rule

**`DataFrame` is untyped at the core. Typing is opt-in at the
materialization edge.**

Nowhere in the fork or in lakeorm does a `DataFrame` carry a Go type
parameter. Typed views (`DataFrameOf[T]`, the `lakeorm.CollectAs[T]`
/ `StreamAs[T]` / `FirstAs[T]` helpers, the `Query[T]` /
`QueryStream[T]` / `QueryFirst[T]` one-shot helpers) are wrappers
that bind to `T` at the moment the caller wants rows out. The
underlying handle is a plain `DataFrame` whose schema is known at
runtime, not compile time.

## Why

1. **Spark's own model is untyped.** DataFrame is
   `Dataset[Row]`-shaped everywhere — Scala, PySpark, Spark Connect
   wire protocol. A typed Go wrapper at the core would be a fiction
   that has to be unwrapped before every protocol call. The
   mismatch doesn't save the caller work; it shifts it.

2. **Joins and aggregates change the row shape.** A `DataFrame[User]`
   that joins to `Order` and projects
   `(user_id, email, count, total)` can't stay a `DataFrame[User]`
   — it's some new shape the writer didn't declare. Unified typed
   ORMs collapse here: either they force a pre-declared result
   type for every projection (noisy) or they erase the type for
   projections (dishonest). Untyped at the core sidesteps the
   whole problem — the caller binds to a freshly-written output
   struct exactly where the projection is known.

3. **Materialization is where the cost lives.** `Collect`, `Stream`,
   `First` are where a type parameter actually pays for itself: the
   wire bytes get decoded into `T` fields, schema-to-field binding
   gets validated, and a compile-time guarantee turns "this query
   returns rows that match this struct" into a real check. Typing
   at the edge — not at the handle — is where the cost and the
   benefit line up.

4. **CQRS by structure.** Writes bind one struct per table (the
   persisted-schema contract). Reads bind one struct per
   projection (the result-shape contract). Same tag grammar, two
   different lifetimes. The untyped `DataFrame` is the pivot point
   where a read stops being "the write's shape" and becomes
   "whatever the SQL asked for."

## Concrete shape

In the fork (`github.com/datalake-go/spark-connect-go/spark/sql`):

- `DataFrame` is untyped. Its method set is the same as upstream
  Apache Spark Connect-Go, additions only.
- `DataFrameOf[T]` is a thin wrapper around `DataFrame`. Constructed
  via `As[T any](df DataFrame) DataFrameOf[T]`; unwrapped via
  `(d DataFrameOf[T]).DataFrame()`.
- Terminal operations are typed: `(DataFrameOf[T]).Collect(ctx)`,
  `.Stream(ctx)`, `.First(ctx)`; free-function equivalents
  `Collect[T]`, `Stream[T]`, `First[T]`.
- Transformations (`Where`, `Select`, `Join`, `GroupBy`) stay on
  the untyped `DataFrame`. There is no `DataFrameOf[T].Where`. Users
  drop to `.DataFrame()`, compose transformations, then re-type at
  the edge when ready to collect.

In lakeorm (`github.com/datalake-go/lake-orm`):

- `lakeorm.DataFrame` is driver-agnostic and untyped. The
  `DriverType() any` escape hatch exposes the driver-native handle
  when callers need something the interface doesn't cover.
- `lakeorm.CollectAs[T](ctx, df)` and `lakeorm.StreamAs[T](ctx, df)`
  are the one-liners for materialising a read into a typed result
  struct. Under the hood they type-assert `DriverType()` to the
  fork's `DataFrame` and call `sparksql.Collect[T]` /
  `sparksql.Stream[T]`.
- `lakeorm.Query[T]` / `QueryStream[T]` / `QueryFirst[T]` are thin
  ergonomic wrappers — one-shot helpers that compose `db.DataFrame`
  with `CollectAs[T]` / `StreamAs[T]` / `FirstAs[T]` on the way
  through. They are NOT typed query builders. Filtering, ordering,
  joins, and projection belong in the SQL string passed as an
  argument — not in chainable `.Where(...).OrderBy(...).Limit(...)`
  methods. Writing the SQL explicitly keeps the read path auditable
  and matches the CQRS framing: one struct per projection.

## What not to do

- **Do not promote `DataFrameOf[T]` into lakeorm's public API as a
  wrapped type.** Typed DataFrames live in the fork. lakeorm
  consumes via helpers; the public `lakeorm.DataFrame` stays
  driver-agnostic.
- **Do not add typed transformations** (`DataFrameOf[T].Where`,
  `DataFrameOf[T].Join`, etc.). Transformations change row shape;
  the type parameter would lie the moment `Select` drops a column.
- **Do not type the `DataFrame` interface itself.** Once `DataFrame`
  carries a `T`, every upstream-additive PR to the fork has to
  track an additional generic constraint. The "additions only"
  invariant against upstream Apache Spark Connect-Go becomes
  impossible to maintain.
- **Do not convert streaming to channels.** `iter.Seq2[T, error]`
  is the correct post-Go-1.23 pattern. The fork's streaming APIs
  return `iter.Seq2`; do not regress them to channels to match
  pre-1.23 upstream style.

## Provenance

This philosophy was locked in during the v0.1.0 pre-launch cycle
after the typed wrapper (`DataFrameOf[T]` + `Collect[T]` family)
landed in the fork and the `lakeorm.CollectAs[T]` helper landed
in lakeorm. It is a design contract, not a design proposal.
