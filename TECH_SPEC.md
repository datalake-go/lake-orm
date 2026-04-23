# lakeorm tech spec

This document is the in-tree reference for how lakeorm's write path actually runs. It complements [`README.md`](README.md) (positioning + quick-start) and [`MIGRATIONS.md`](MIGRATIONS.md) (schema evolution). Read this when you want to know what a given `db.Insert(...)` does on the wire.

## Composition

```
User code ── lakeorm.Client ── lakeorm.Dialect ── lakeorm.Driver ── Spark Connect ── Iceberg / Delta
                  │
             lakeorm.Backend ── bytes on S3 / GCS / file / memory
```

Three orthogonal interfaces compose at `lakeorm.Open(driver, dialect, backend, ...)`:

- **Driver** — how we connect and execute. `spark.Remote` or `spark.Databricks`.
- **Dialect** — the data-dialect (DDL, DML, capabilities, semantics). `iceberg.Dialect` or `delta.Dialect`.
- **Backend** — where the bytes live. `backend.S3`, `backend.GCS`, `backend.File`, `backend.Memory`.

## Data flow: the three write paths

`db.Insert(ctx, records)` picks one of three paths based on the Dialect's `PlanInsert` verdict:

1. **Direct gRPC ingest** (small batches).
2. **The fast path** (large batches via object-storage staging).
3. **Raw-then-merge** (opt-in via `db.InsertRaw(...).ThenMerge(...)`).

The fast path is the most operationally significant and the most subtle. It's what lakeorm's throughput claim actually rides on.

### Prelude: the `ingest_id`

Every `Client.Insert` call — regardless of path — generates a UUIDv7 at entry to `client.Insert`:

```go
ingestUUID, err := uuid.NewV7()
ingestID := ingestUUID.String()
```

The id threads through:

- Any struct field carrying `spark:"column_name,auto=ingestID"` (field populated before `Validate`).
- The staging prefix (`Backend.StagingPrefix(ingestID)`).
- Log context via `ingest_id=<uuid>` structured field.
- The `Finalizer`'s cleanup target on `Abort`.

**Why UUIDv7 specifically:** the leading 48 bits are `unix_ts_ms`, which makes staging prefixes time-sortable in object-storage listings and makes `Client.CleanupStaging(ctx, olderThan)` possible without a state file — the cleanup job parses the timestamp out of each prefix name and deletes anything older than the threshold.

Scope rules for `auto=ingestID`:

- Reserved modifier. Declared on a string field via `spark:"column_name,auto=ingestID"`.
- Column name is whatever the caller wrote; the modifier triggers on the modifier, not the column name.
- Field type must be `string` or `*string`; any other type produces `ErrInvalidTag` at schema-parse time.
- Per Insert, one id, shared across every record — so downstream queries can filter by `ingest_id` to get a cohesive batch.

Full collision matrix in [`INGEST_ID.md`](https://github.com/datalake-go/lake-orm/issues/TODO) — in the meantime, `ingest_id.go` + `ingest_id_test.go` document the cases in code.

### The fast path

```
                   PartitionWriter                                  Driver.Execute
                          │                                               │
records ─► Validate ─► fast-path                                         INSERT
 (auto=    (may alloc   writer               staging prefix              INTO target
  ingest    new rows     (zstd-             (s3://bucket/_staging/        SELECT FROM
  ID         on type     parquet)            <ingest_id>/)                parquet.<prefix>
  first)    mismatch)       │                                               │
                            ▼                                               ▼
                         Backend.Writer                                Spark Connect
                         (part-00001.parquet,                           executes,
                          part-00002.parquet, ...)                      returns Finalizer
                                                                            │
                                                                            ▼
                                                                    Commit / Abort
```

**Step 1: generate ingest_id and populate auto-fields.** `Client.Insert` generates the UUIDv7, walks any `auto=ingestID` fields on each record, writes the id in place.

**Step 2: Validate.** `lakeorm.Validate(records)` runs every `validate=<name>` check declared on the schema. A failure here returns before any I/O happens.

**Step 3: compute staging prefix.** The Dialect's `PlanInsert` calls `Backend.StagingPrefix(ingestID)`. For `backend.S3("s3://my-bucket/lake")`, this returns `s3://my-bucket/lake/_staging/<ingest_id>/`. Specific to this ingest and not shared.

**Step 4: stream records through the partition writer.** The Dialect constructs a `PartitionWriter[T]` that wraps `parquet-go`'s generic writer over a `bytes.Buffer`. Rows are written one at a time; each `Write` call invokes `writer.Flush()` internally. This is the load-bearing **Flush-per-Write** pattern — without it, `parquet-go` buffers rows internally and never emits to the underlying buffer, defeating the byte-threshold trigger. Regression tests in `internal/parquet/partition_writer_test.go` pin this.

**Step 5: emit parts to the Backend.** Once the buffer reaches `cfg.PartitionByteThreshold` (default 128 MiB), the PartitionWriter closes the current part file, uploads it via `Backend.Writer(ctx, stagingKey)`, and starts a new part. A safety backstop at 5M rows per part handles pathologically small rows. After `pw.Close()` every part has been uploaded.

Part paths look like:

```
s3://my-bucket/lake/_staging/01928abc-def0-.../part-00001.parquet
s3://my-bucket/lake/_staging/01928abc-def0-.../part-00002.parquet
```

**Step 6: Dialect generates the commit plan.** Parts uploaded, the Dialect produces an `ExecutionPlan` whose statement is:

```sql
INSERT INTO <target-table>
SELECT * FROM parquet.`s3://my-bucket/lake/_staging/01928abc-def0-.../`
```

Dialect-specific polish (Iceberg partition pruning hints, Delta MERGE strategies) may layer on; the core pattern is "INSERT from the staged prefix."

**Step 7: Driver.Execute commits.** The driver runs the plan via `session.Sql(...)`. One Spark Connect RPC; the Spark Connect server (not the Go client) reads parquet from S3 and appends to the target table. Massive payloads never traverse the Go process — the Go process only orchestrated the staging.

**Step 8: Finalizer.** `Driver.Execute` returns a `Finalizer`. The Client calls:

- `Commit(ctx)` on success — usually a no-op because the `INSERT` already committed the table's metadata at the Spark layer. Records success in the logger.
- `Abort(ctx)` on failure — calls `Backend.CleanupStaging(stagingPrefix)` to delete the uploaded parts so they don't accumulate as orphaned storage.

### The write-path decision tree

```
Client.Insert(records)
  │
  ├─ generate ingest_id (UUIDv7)
  ├─ apply auto=ingestID fields
  ├─ Validate records
  │
  │  Dialect.PlanInsert picks a path:
  │
  ├─ if estimated bytes < FastPathThreshold (default 128 MiB):
  │    → DirectIngestPlan (gRPC)
  │      Driver.Execute streams via gRPC
  │      Finalizer.Commit is a no-op
  │      Finalizer.Abort is a no-op (nothing staged)
  │
  └─ else:
       stagingPrefix := Backend.StagingPrefix(ingestID)
       PartitionWriter streams records → parquet parts → Backend.Writer
       ParquetIngestPlan { target, source = stagingPrefix }
       Driver.Execute runs "INSERT INTO target SELECT FROM parquet.<prefix>"
       Finalizer.Commit records success
       Finalizer.Abort calls Backend.CleanupStaging(stagingPrefix)
```

## Implementation constraints (don't clean these up)

- **Flush-per-Write must be preserved.** Regression tests in `internal/parquet` pin the contract. Do not "clean up" the per-`Write` `Flush` call — the byte-threshold trigger depends on the buffer being observable after each row.
- **ZSTD compression is the default for staged parquet parts** — 2–3× smaller than Snappy for typical lakehouse workloads. Configurable via `lakeorm.WithCompression`.
- **Single-goroutine writer pattern.** `PartitionWriter` is not concurrency-safe. Each `Insert` call gets its own writer. No `sync.Pool` — allocation cost is negligible next to parquet encoding.
- **`cfg.MaxInflightIngests` semaphore** lives in the Client and gates how many concurrent `Insert` operations (each with its own writer + ingest_id + staging prefix) can run. Default 4. Prevents OOM under load.

## Cleanup: dealing with orphaned staging

Failed ingests (process crash between upload and `Driver.Execute`, context cancellation before `Abort` fires) leave parts under `_staging/<ingest_id>/` with no corresponding target-table data. `Client.CleanupStaging(ctx, olderThan)` walks the backend's `_staging/` namespace, parses each prefix as a UUIDv7, and deletes prefixes whose embedded `unix_ts_ms` is older than the threshold.

```go
report, err := db.CleanupStaging(ctx, 72*time.Hour)
// report.Deleted / report.Failed / report.Scanned
```

Safe to run concurrently with ongoing inserts: UUIDv7's time-sortability guarantees a prefix older than the TTL is not in-flight (unless clock skew exceeds the TTL, which is pathological).

Typical cadences:

- **Dev:** 24h TTL, run ad-hoc when disk usage grows.
- **Production:** 72h TTL, run as a nightly cron or k8s `Job`. 72h gives operators enough time to investigate a failed ingest before its staging evaporates.

Non-UUIDv7 prefixes under `_staging/` are skipped (not deleted). The namespace can carry operator-managed content that doesn't follow the UUIDv7 convention and `CleanupStaging` leaves it alone.

## Observability

Every `Insert` logs at three points with structured fields; `ingest_id` appears on every line:

```
level=info ingest_id=01928abc-... table=events rows=10000 path=fast-path staging_prefix=s3://...
level=info ingest_id=01928abc-... parts=3 bytes=402653184 duration_ms=4231    # staging complete
level=info ingest_id=01928abc-... result=commit rows_written=10000 total_duration_ms=8472
```

On abort:

```
level=error ingest_id=01928abc-... result=abort cause="driver: execute: connection reset" cleanup=scheduled
```

A single grep for `ingest_id=<uuid>` finds every event related to one operation across staging upload, driver execute, finalizer commit/abort, and any subsequent cleanup delete.

## Reading: Query[T] + drivers.Convertible

Reads go through a separate surface built on the `drivers.Convertible` capability every driver implements. The top-level helpers take a driver-native `drivers.Source` closure and decode each row into the caller-supplied Go struct:

- `lakeorm.Query[T](ctx, db, source)` — typed, buffered. Returns `[]T`.
- `lakeorm.QueryStream[T](ctx, db, source)` — typed, constant-memory `iter.Seq2[T, error]`.
- `lakeorm.QueryFirst[T](ctx, db, source)` — returns `*T, nil` or `lkerrors.ErrNoRows`.

Build the `Source` with the concrete driver's conversion helper. Reach the driver via `Client.Driver()`:

```go
drv := db.Driver().(*spark.Driver)
users, _ := lakeorm.Query[User](ctx, db, drv.FromSQL("SELECT * FROM users WHERE country = ?", "UK"))
```

Per-driver helpers:

- `*spark.Driver`: `FromSQL`, `FromDataFrame`, `FromTable`, `FromRow`, `Session`.
- `*duckdb.Driver` / `*databricks.Driver`: `FromSQL`, `FromRows`, `FromTable`, `FromRow`.

For joins and aggregates, declare a purpose-built result-shape struct and feed the same `Query[T]` helper — the projection struct is the contract, `spark:"..."` tags bind result columns to fields. Anything the helpers don't cover can be expressed as a bare `drivers.Source` closure.

See [`examples/arbitrary_spark_query/`](examples/arbitrary_spark_query/main.go) for a native Spark DataFrame chain decoded into a projection struct, [`examples/joins/`](examples/joins/main.go) for SQL-joined CQRS reads, and [`examples/stream/`](examples/stream/main.go) for the constant-memory streaming pattern.

## See also

- [`MIGRATIONS.md`](MIGRATIONS.md) — schema evolution via lake-goose.
- [`ingest_id.go`](ingest_id.go) + [`ingest_id_test.go`](ingest_id_test.go) — the UUIDv7 + auto=ingestID + CleanupStaging implementation and test matrix.
- [`internal/parquet/`](internal/parquet) — the `PartitionWriter` with the Flush-per-Write contract.
- [`github.com/datalake-go/spark-connect-go`](https://github.com/datalake-go/spark-connect-go) — the Spark Connect client lakeorm ships against.
- [`github.com/datalake-go/lake-goose`](https://github.com/datalake-go/lake-goose) — the goose fork that executes generated migration files against the Spark Connect `database/sql` driver.
