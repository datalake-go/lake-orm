# Typing philosophy

This is a design contract, not a feature description. It records why
lake-orm draws the typed / untyped line where it does, so future
contributors don't "improve" the design by moving it.

## The rule

**The driver's native row source is untyped. Typing binds at the
materialization edge via `drivers.Convertible`.**

The driver hands the caller a `drivers.Source` — a closure that,
when invoked, returns the driver's own native row carrier (a
`sparksql.DataFrame` for Spark, a `*sql.Rows` for DuckDB /
Databricks SQL). Nothing about that carrier has a Go type parameter.
The top-level helpers (`lakeorm.Query[T]` / `QueryStream[T]` /
`QueryFirst[T]`) invoke the source, walk its rows, and bind to `T`
at the moment the caller wants rows out.

## Why

1. **The engines' own models are untyped.** Spark's DataFrame is
   `Dataset[Row]`-shaped in every binding (Scala, PySpark, Spark
   Connect wire protocol). `*sql.Rows` is a tag soup of `any`. A
   typed Go wrapper at the core would be a fiction that has to be
   unwrapped before every protocol call. The mismatch doesn't save
   the caller work; it shifts it.

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
   `First` on `drivers.Convertible` are where the type parameter
   actually pays for itself: the wire bytes get decoded into `T`
   fields, schema-to-field binding gets validated, and a compile-
   time guarantee turns "this query returns rows that match this
   struct" into a real check. Typing at the edge — not at the
   handle — is where the cost and the benefit line up.

4. **CQRS by structure.** Writes bind one struct per table (the
   persisted-schema contract, routed via `Insert`). Reads bind one
   struct per projection (the result-shape contract, routed via
   `Query[T]`). Same tag grammar, two different lifetimes. The
   `drivers.Source` closure is the pivot point where a read stops
   being "the write's shape" and becomes "whatever the query asked
   for."

## Concrete shape

In `drivers/` (the shared contract):

- `drivers.Source` is a closure: `func(ctx) (native any, cleanup
  func(), err error)`. The driver-native type is opaque to the
  caller; the Convertible implementation type-asserts to its own
  known native type and fails fast on mismatch.
- `drivers.Convertible` is the optional driver capability — given
  a Source, decode each row into a user-supplied Go target. Three
  methods: `Collect(ctx, source, out)`, `First(ctx, source, out)`,
  `Stream(ctx, source, sample)`. The out parameter is reflected to
  discover `T`; no generic constraint leaks into the interface.

In `drivers/<name>/` (per-driver conversion helpers):

- `*spark.Driver.FromSQL` / `FromDataFrame` / `FromTable` /
  `FromRow` / `Session` — build Sources that yield a
  `sparksql.DataFrame` plus the session-pool release hook.
- `*duckdb.Driver.FromSQL` / `FromRows` / `FromTable` / `FromRow`
  — same shape over `*sql.Rows`.
- `*databricks.Driver.FromSQL` / `FromRows` / `FromTable` /
  `FromRow` — identical to DuckDB; both are `*sql.DB`-backed.

At the root (`github.com/datalake-go/lake-orm`):

- `lakeorm.Query[T](ctx, db, source)` — materialise the full
  result as `[]T`.
- `lakeorm.QueryStream[T](ctx, db, source)` — `iter.Seq2[T, error]`,
  constant memory.
- `lakeorm.QueryFirst[T](ctx, db, source)` — returns `*T, nil` or
  `lkerrors.ErrNoRows`.
- `Client.Driver() Driver` — type-assert to `*spark.Driver` /
  `*duckdb.Driver` / `*databricks.Driver` to reach the per-driver
  conversion helpers and the raw native handle.

These are NOT typed query builders. Filtering, ordering, joins,
and projection belong in the query expression passed via the
Source — whether that's a SQL string (`drv.FromSQL`), a chained
native DataFrame (`drv.FromDataFrame`), or a pre-opened `*sql.Rows`
(`drv.FromRows`). Writing the query explicitly keeps the read path
auditable and matches the CQRS framing: one struct per projection.

## What not to do

- **Do not introduce a typed `DataFrame`-shaped interface at the
  root.** Typed handles change shape the moment `Select` drops a
  column. The write path (typed, pre-declared) and the read path
  (typed at the projection edge) must stay separate.
- **Do not add typed transformations.** If a user needs `Where` /
  `Select` / `Join`, they compose them on the driver-native handle
  (e.g. `sparksql.DataFrame.Filter`) or in the SQL string. The
  `drivers.Source` closure is where the typed Go world meets the
  untyped engine world; transformations belong on the engine side
  of that boundary.
- **Do not convert streaming to channels.** `iter.Seq2[T, error]`
  is the correct post-Go-1.23 pattern.
- **Do not leak driver-native types into the root API.** Users who
  need `sparksql.DataFrame` methods type-assert
  `db.Driver().(*spark.Driver)` and call `Session(ctx)`; the root
  Client interface stays agnostic of the underlying engine.

## Provenance

This philosophy was first locked in during the v0.1.0 pre-launch
cycle with a typed DataFrame wrapper (`DataFrameOf[T]`) in the
fork + a `lakeorm.CollectAs[T]` helper at the root. It was
rewritten to the `drivers.Source` / `drivers.Convertible` shape
once the cost of maintaining `DataFrame` at the root — an
engine-specific abstraction leaking through the driver-agnostic
interface — stopped paying its way. The new shape is strictly
less rope: no wrapper type, no DriverType() unwrap, one typed
entry point that covers SQL, DataFrame chaining, and
`*sql.Rows`-shaped drivers the same way. It is a design contract,
not a design proposal.
