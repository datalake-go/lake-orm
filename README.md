# lake-orm

> `lake-orm` is the batteries-included ORM for building on the lakehouse in Go.

## The problem

Building reliable data systems on top of lakehouse technologies is harder than it should be.

Data is often written to staging tables, replayed through pipelines, and fixed downstream after the fact. Failures are handled asynchronously, and correctness is inferred later rather than enforced at the boundary.

Across organisations, the same ingestion and validation systems are rebuilt repeatedly, yet data quality issues still persist.

This leads to systems that are:

* difficult to reason about
* hard to debug
* prone to silent data quality issues

`lake-orm` takes a different approach: validate data at the application boundary and write it correctly once, using deterministic, request-scoped operations.

It implements transactional, synchronous semantics at the application layer without sacrificing batch performance.

A data-quality-first approach to the lakehouse.

Designed for production workloads and compatible with Databricks, Delta, and Iceberg.

Write correct data once, so you don’t have to fix it later.

CQRS-shaped, untyped at the core, typed at the edge.
Typed structs in, tagged columns out.

---

## Quick start

```go
package main

import (
    "context"

    "github.com/datalake-go/lake-orm"
    "github.com/datalake-go/lake-orm/backend"
    "github.com/datalake-go/lake-orm/dialect/iceberg"
    "github.com/datalake-go/lake-orm/driver/spark"
)

type User struct {
    ID    string `lake:"id,pk"         validate:"required"`
    Email string `lake:"email,mergeKey" validate:"required,email"`
}

func main() {
    ctx := context.Background()

    store, _ := backend.S3("s3://bucket/lake")
    db, _ := lakeorm.Open(spark.Remote("sc://host:15002"), iceberg.Dialect(), store)
    defer db.Close()

    _ = db.Migrate(ctx, &User{})
    _ = db.Insert(ctx, []*User{{ID: "u1", Email: "alice@example.com"}})

    users, _ := lakeorm.Query[User](ctx, db, "SELECT * FROM users WHERE id = ?", "u1")
    _ = users
}
```

---

`lake-orm` scopes every write to a single `ingest_id`.

Small writes can go direct; large writes are staged as Parquet on object storage and materialised with a single operation. This staging layer is temporary and request-scoped—it is not a bronze table or long-lived source of truth.

Each operation is bounded:

* **Insert paths** materialise exactly one ingest
* **Merge paths** reconcile exactly one ingest against the target table
* **All reads during a merge are filtered by `ingest_id`**

If any step fails, the request fails. There is no partially committed state and no downstream recovery process.

This keeps write semantics deterministic, synchronous, and easy to debug.

---

## Philosophy

Write correct data once. Prefer synchronous workflows with retries at the client, so you don’t have to fix it later.

### Rethinking the medallion architecture

The bronze layer commonly described as "best practice" exists for two practical reasons, neither of which is data quality.

Spark is pull-based—until Spark Connect, external applications could not push data into a running cluster. Data had to land in object storage first so Spark could read it.

The second is Delta's optimistic concurrency control: concurrent `MERGE` operations conflict at the file level, so teams batch incoming data into append-only tables and merge in bulk to minimise contention.

These are engine constraints, not principles of OLAP.

Software engineers have validated inputs at the application boundary for decades. A Go struct, a protobuf message, a Pydantic model — the type system is the contract, and the contract is enforced before anything touches storage.

Spark Connect, gRPC over HTTP/2, lets a stateful Go application dial directly into a cluster, construct a DataFrame from already-validated inputs, and write straight to Iceberg or Delta.

No bronze. No autoloader. No asynchronous pipeline that fails hours later.

`lake-orm` pushes validation upstream and writes validated, tagged structs directly into the lakehouse.

Read the long form at https://callumdempseyleach.tech/writing/medallion-architecture/

### Migrations: authoring, not evolution

The ORM emits ALTER TABLE statements when your struct drifts from the most-recent migration's recorded state. `MigrateGenerate` produces goose-format `.sql` files with `-- DESTRUCTIVE: <reason>` comments on risky ops and an `atlas.sum` manifest that catches post-generation edits.

But the library actively discourages schema evolution as a routine operation. Every emitted Up block carries a block comment reminding the reviewer that evolution is a tax on every downstream consumer and a common source of silent-wrong data. The right move when a schema needs to change is almost always to rethink the model — not to ship a column rename.

Schemas are contracts. Define them up front; clean data at the application boundary; let lake-orm enforce the shape. If your data doesn't fit, **fix the producer, not the table**. See the FAQ in the [wiki](https://github.com/datalake-go/lake-orm/wiki) for the long answer on handling unstructured upstream data.

---

## Features

* **Go structs as the schema contract.** Tag a field `lake:"id,pk"` and the column, primary-key, merge-key, partition, and nullability all flow from the struct. `lake:"..."`, `lakeorm:"..."`, and `spark:"..."` parse equivalently.
* **Validation at the application boundary.** `lakeorm.Validate(records)` wraps [go-playground/validator](https://github.com/go-playground/validator) — rules live on the standard `validate:"required,email,uuid,..."` struct tag, errors unwrap to `validator.ValidationErrors` for per-field HTTP-400 responses.
* **Strict JSON decoding.** `lakeorm.FromJSON[T](payload)` decodes HTTP / queue payloads straight into your tagged model and rejects any field the struct doesn't declare — the ingest-boundary corollary to "no schema evolution". The struct *is* the semantic layer.
* **Two write paths, one API.** Small batches go direct; large ones use Parquet staging plus a single Spark operation.
* **Typed reads.** `Query[T]`, `QueryStream[T]`, and `QueryFirst[T]`.
* **CQRS for complex reads.** Use DataFrame plus `CollectAs[T]` / `StreamAs[T]`.
* **Iceberg and Delta dialects.** Pluggable via a `Dialect` interface.
* **Multiple drivers.**

  * Spark Connect (self-hosted)
  * Databricks SQL (warehouse)
  * Databricks Connect (interactive clusters)
* **Pluggable backends.** S3, GCS, file, memory.
* **Migration *authoring* support.** Django-style file generation for schema drift; schema evolution itself is deliberately discouraged — see Philosophy.
* **UUIDv7 ingest IDs.** Batch-scoped identity and reconciliation.
* **No telemetry.** Only connects to configured endpoints.

---

## Architecture

Lakehouse systems are analytical and columnar. Traditional ORMs assume stable row shapes, but joins produce dynamic projections.

`lake-orm` uses a CQRS-style approach:

* Write-side: one struct per table
* Read-side: one struct per query shape

Typing is applied at the materialization edge.

```text
Go structs (lake:"..." tags)
    │
    ▼
lakeorm.Client ──► Dialect ──► Driver ──► Spark Connect
    │                                      │
    ├──► Query[T] / Stream[T]              ▼
    │                                 Iceberg / Delta
    │                                      │
    └──► DataFrame → CollectAs[T]          ▼
                                      Object storage
```

Large writes stream Parquet directly to object storage and are materialised with a single Spark operation.

---

## Upserts

Declare a `mergeKey` on a struct field and `Insert` flips from append semantics to upsert semantics. On the fast path the driver emits `MERGE INTO` instead of `INSERT INTO`, with the MERGE source filtered by the current operation's `_ingest_id` so retry-on-OCC-conflict is idempotent.

**Single mergeKey:**

```go
type Customer struct {
    ID    types.SortableID `lake:"id,pk"`
    Email string           `lake:"email,mergeKey"` // upsert identity
    Tier  string           `lake:"tier"`
}
```

Emitted SQL on the large-batch path (Iceberg / Delta):

```sql
MERGE INTO customers AS target
USING (SELECT * FROM <staging> WHERE _ingest_id = '<batch>') AS source
ON target.email = source.email
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Composite mergeKey** — declare `mergeKey` on multiple fields and every key becomes part of the ON clause, AND-joined. Useful when identity is a tuple (e.g. `(user_id, date)` for a daily position table, or `(tenant_id, resource_id)` for multi-tenant rows):

```go
type Position struct {
    UserID types.SortableID `lake:"user_id,mergeKey"`
    Date   time.Time        `lake:"date,mergeKey"`
    Value  int64            `lake:"value"`
    Note   string           `lake:"note"`
}
```

Emitted SQL:

```sql
MERGE INTO positions AS target
USING (SELECT * FROM <staging> WHERE _ingest_id = '<batch>') AS source
ON target.user_id = source.user_id AND target.date = source.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

`WHEN MATCHED THEN UPDATE SET *` updates every non-key column, which is usually what you want. If you need to update only a subset (e.g. keep `created_at` immutable on upsert), write the MERGE yourself via `Client.Exec` or `Client.DataFrame` and the engine's native SQL — the auto-routed path doesn't split non-key columns into update vs insert buckets.

For a full walkthrough see [`examples/`](examples/) (the `bulk` and upsert examples) and the [wiki](https://github.com/datalake-go/lake-orm/wiki)'s Advanced Functions page.

---

## Ingesting from JSON

`lakeorm.FromJSON[T](payload)` is the strict-decoding counterpart to `Validate`. Use it at the HTTP handler / queue consumer boundary when the upstream payload is JSON — it rejects any field your model doesn't declare, then runs the registered validator. If the decode or validation fails, return a 400 and don't touch the lakehouse.

```go
type Book struct {
    ID     string `json:"id"     lake:"id,pk"        validate:"required,uuid"`
    Title  string `json:"title"  lake:"title"        validate:"required"`
    Author string `json:"author" lake:"author"`
}

func handleCreateBook(w http.ResponseWriter, r *http.Request) {
    book, err := lakeorm.FromJSON[Book](readBody(r))
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
    if err := db.Insert(r.Context(), []*Book{book}); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusCreated)
}
```

Rejection semantics:

- **Extra top-level keys** — a JSON key that doesn't correspond to a declared struct field → decode error (encoding/json's `DisallowUnknownFields`).
- **Extra keys inside a declared nested struct** — same strict rule propagates down.
- **Type mismatches** — JSON object where the struct expects a string, JSON number where it expects a bool, etc. → decode error.
- **Missing required fields** — `validate:"required"` enforcement via the standard validator.
- **Trailing data** after the first JSON value — rejected. One payload, one call.

**Why strict.** lake-orm's write semantics treat the struct as the schema contract. Silent drop-on-ingest of unknown fields would make two very different problems indistinguishable at the ingest boundary: your producer is sending the wrong shape (fix the producer) vs. your model is out of date (update the model + ship the migration). Forcing the decode to error surfaces the question before the data lands.

`FromJSON` is a regular Go function, not a framework primitive. If you need looser semantics (e.g. you're legitimately accepting third-party JSON you don't control), decode with `encoding/json` directly, transform in Go code, and hand the validated result to `Insert`. The library doesn't hide anything; it just refuses to do the unsafe thing by default.

---

## Examples

* basic usage
* bulk inserts
* streaming reads
* joins (CQRS)
* validation
* ingest ID usage
* migrations
* Databricks integration

---

## Install

```bash
go get github.com/datalake-go/lake-orm
```

---

## Related

* lakehouse — full runtime composition
* spark-connect-go — Spark execution layer

---

## Under the hood

A brief for readers who want to know what's actually happening when they call `Insert` or `Query[T]`. Depth lives in [the wiki](https://github.com/datalake-go/lake-orm/wiki); this section is the five-minute version.

* **Struct tags are the contract.** `ParseSchema` reads `lake:"..."` (or `lakeorm:"..."`, or `spark:"..."`) once, caches the resolved `LakeSchema` per Go type. Column names, primary key, merge keys, partition intent, nullability all live there. The schema is what the Dialect consults to emit DDL and what the parquet writer uses to synthesise its row shape.
* **Composition at `Open`.** Three orthogonal pieces plug together into a `Client`: a `Driver` (transport — Spark Connect / Databricks SQL warehouse / Databricks Connect), a `Dialect` (DDL grammar, INSERT vs MERGE planning — Iceberg / Delta / DuckDB), and a `Backend` (where bytes live — S3, GCS, file, memory). Swapping one leaves the other two untouched.
* **Validation is pass-through.** `lakeorm.Validate(records)` calls `validator.Struct()` from go-playground/validator. `Validate` at the boundary before `Insert` — the same rules run either way, but surfacing failures early gives the HTTP handler a clean 400.
* **Writes are request-scoped.** `Client.Insert` generates a UUIDv7 `ingest_id`, hands a `WriteRequest` to the Dialect, gets back an `ExecutionPlan`. Plan kinds: `KindDirectIngest` (small batch, one bulk INSERT); `KindParquetIngest` (large append — stage parquet to `<warehouse>/<ingest_id>/part-*.parquet` through the Backend, then one `INSERT INTO target SELECT * FROM parquet.<staging>`); `KindParquetMerge` (merge-key present — same staging, but the driver emits `MERGE INTO target USING (SELECT * FROM staging WHERE _ingest_id = '<batch>') ON target.<mergeKey> = source.<mergeKey>`). The `_ingest_id` filter on the MERGE source bounds the operation to this batch and makes retry-after-OCC-conflict idempotent.
* **`_ingest_id` is system-managed.** Every table lake-orm creates carries the column; the parquet writer stamps each row with the current batch's ingest_id. User structs don't declare it — declaring `_ingest_id` is a schema-parse error. Reconciliation queries use a projection struct that adds the column back on the read side.
* **Reads are typed at the edge.** `Query[T]` → `db.DataFrame(ctx, sql)` → `CollectAs[T]`. For Spark-family drivers the DataFrame unwraps to `sparksql.DataFrame` and decoding happens through the fork's typed collect. For non-Spark drivers (DuckDB, Databricks SQL) the generic fallback iterates `DataFrame.Stream(ctx)` and scans each `Row` through lake-orm's reflection scanner. Unmapped columns are dropped silently — that's why `SELECT *` against a table with `_ingest_id` returns clean rows into a struct that doesn't declare it.
* **Migrations are file-based.** `MigrateGenerate` replays the most recent migration file's `State-JSON` header to reconstruct the prior schema, diffs against the current struct, emits one goose-format `.sql` file per changed table with `-- DESTRUCTIVE: <reason>` comments on risky ops. `atlas.sum` manifest catches post-generation edits. Apply-time execution happens through lake-goose (separate binary), not lake-orm.

---

> A lakehouse is the sum of its parts.
> `lake-orm` gives those parts structure.
