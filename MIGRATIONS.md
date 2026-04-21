# lakeorm migrations

Schema evolution for lakehouse tables, split across three roles:

- **lake-goose** — *runs* migrations. A fork of [pressly/goose](https://github.com/pressly/goose) with two added dialects: `iceberg` and `delta`. Executes `.sql` files against any `database/sql` driver that speaks Spark SQL (primarily [`datalake-go/spark-connect-go`](https://github.com/datalake-go/spark-connect-go)'s `database/sql` driver).
- **lakeorm** — *authors* migrations. `MigrateGenerate` diffs your struct tags against the schema state recorded in prior migration files (Django `makemigrations`-style) and emits a new `.sql` file with just the delta. Destructive operations land with `-- DESTRUCTIVE: <reason>` comments so reviewers see them in the PR diff.
- **Humans** — *review* destructive changes. The file on disk is the contract. There is no machine-enforced acknowledgement gate.

## Philosophy

**Models are the source of truth.** A Go struct tagged with `spark:"..."` is the desired table shape. Migration files record the journey — the ops that move the table from its prior state to match the struct.

**Prior state lives in the file header, not a separate catalog.** Each generated file carries a `-- State-JSON: {...}` header describing the table's shape *after* applying every Up statement in that file. `MigrateGenerate` replays the most recent file for a given table to reconstruct the prior state, then diffs the struct against it and emits only the new ops. This is Django's [`MigrationLoader`](https://docs.djangoproject.com/en/5.0/topics/migrations/#migration-files) pattern adapted to goose-format output.

**Execution is goose's job.** The `database/sql` driver at `datalake-go/spark-connect-go/spark/sql/driver` makes Spark Connect look like any other SQL database to goose (or `sqlc`, or `pgx`-shaped code, or an ad-hoc `go test` harness). lake-goose's two new dialects just dictate whether the `goose_db_version` bookkeeping table is Iceberg or Delta. User migrations are free to mix formats via `USING iceberg` / `USING DELTA` in the DDL they write.

**Destructive ops are a PR-review concern.** `DROP COLUMN`, `RENAME COLUMN`, type narrowing, and NOT-NULL tightening all surface as `-- DESTRUCTIVE: <reason>` comments. No `-- migrate:ack` slugs, no machine-enforced acknowledgements. The reviewer sees the comment in the PR diff and decides.

## How it works

```
┌────────────┐   MigrateGenerate(ctx, dir, &User{}, &Order{})
│ Go structs │──────────────────────────────────────────────┐
└────────────┘                                              │
                                                            ▼
                                          ┌─────────────────────────────────┐
                                          │ lakeorm — authoring              │
                                          │                                 │
                                          │ 1. Parse each struct → Schema   │
                                          │ 2. Replay latest .sql in dir    │
                                          │    → prior state per table      │
                                          │ 3. Diff → list of Change ops    │
                                          │ 4. Emit <ts>_<table>.sql:       │
                                          │     -- State-JSON: {...}        │
                                          │     -- +goose Up                │
                                          │     -- DESTRUCTIVE: reason      │
                                          │     ALTER TABLE ...             │
                                          │ 5. Update atlas.sum manifest    │
                                          └───────────────────┬─────────────┘
                                                              │
                                                      migrations/
                                                      ├── atlas.sum
                                                      ├── 20260419153012_users.sql
                                                      └── 20260420091523_orders.sql
                                                              │
                                                              ▼
                                          ┌─────────────────────────────────┐
                                          │ lake-goose — execution           │
                                          │                                 │
                                          │ goose -dir ./migrations iceberg  │
                                          │        "sc://host:15002" up     │
                                          │                                 │
                                          │ • Reads .sql files               │
                                          │ • sql.Open("spark", dsn)         │
                                          │ • Executes Up blocks             │
                                          │ • Updates goose_db_version       │
                                          └─────────────────────────────────┘
```

## Quickstart

Install the CLI:

```bash
go install github.com/datalake-go/lake-goose/cmd/goose@latest
```

Author:

```go
err := db.MigrateGenerate(ctx, "./migrations", &User{}, &Order{})
```

Review the generated file, keep or remove any `-- DESTRUCTIVE:` blocks, commit.

Apply:

```bash
goose -dir ./migrations iceberg "sc://localhost:15002" up
```

Or from Go, using goose as a library:

```go
import (
    "database/sql"

    "github.com/pressly/goose/v3"
    _ "github.com/datalake-go/spark-connect-go/spark/sql/driver"
)

db, _ := sql.Open("spark", "sc://localhost:15002")
_ = goose.SetDialect("iceberg")
_ = goose.Up(db, "./migrations")
```

## The State-JSON header

Each generated file starts with a one-line State-JSON comment:

```sql
-- lakeorm: generated 2026-04-20T09:15:23Z from models.User
-- Struct fingerprint: sha256:...
-- Dialect: iceberg
-- State-JSON: {"table_name":"users","fields":[{"column":"id","sql_type":"STRING","go_type":"string","nullable":false,"pk":true},{"column":"email","sql_type":"STRING","go_type":"string","nullable":false,"merge_key":true}]}

-- +goose Up
...
```

The JSON is the **target state** — what the table looks like after every Up statement in this file has run. When you later change the struct and re-run `MigrateGenerate`:

1. It finds the most recent file that declares `table_name: "users"`.
2. Reads the State-JSON there as the **prior state**.
3. Diffs your new struct against it.
4. Emits a new file with just the delta + a new State-JSON reflecting the latest shape.

No DESCRIBE TABLE round-trip required; no separate state database; no out-of-band state that can drift from the files.

## atlas.sum integrity

Every `MigrateGenerate` call rewrites an `atlas.sum` manifest at the migrations-dir root (same format as Ariga's [Atlas](https://atlasgo.io/concepts/migration-directory-integrity)):

```
h1:<base64(sha256(body))>
20260419153012_users.sql h1:<base64(sha256(file))>
20260420091523_orders.sql h1:<base64(sha256(file))>
```

Any post-generation edit to a `.sql` file changes its hash → changes the directory hash on line 1. Downstream tooling that verifies `atlas.sum` (Atlas itself, custom CI jobs, a future `lakeorm migrate --check` command) can surface drift cleanly.

## Destructive operations

The rule table, applied across Iceberg and Delta identically:

| Operation | Destructive | Reason |
|---|---|---|
| `ADD COLUMN` (nullable, no default) | no | |
| `ADD COLUMN` (NOT NULL) | yes | full-table scan to populate defaults |
| `ADD COLUMN` with default | yes | default value is a semantic choice — review the default |
| `DROP COLUMN` | yes | data loss; downstream readers may break |
| `RENAME COLUMN` | yes | downstream readers may break on the renamed column |
| Type widen (INT → BIGINT, FLOAT → DOUBLE) | no | |
| Type narrow (BIGINT → INT) | yes | overflow risk; requires full scan |
| `SET NOT NULL` on existing column | yes | scan-and-validate; rows with existing NULLs fail |
| `SET NULLABLE` on existing column | no | |

Destructive changes land in the file with the reason attached:

```sql
-- DESTRUCTIVE: data loss; downstream readers may break
ALTER TABLE users DROP COLUMN legacy_tier;
```

The reviewer decides. There is no ack slug, no `-- migrate:ack` contract, no gate other than the PR review itself.

## What's not here (v0)

- **`lakeorm migrate --check`** (CI drift detection). The pieces are in place — atlas.sum plus the State-JSON header — but the CLI wrapper that runs the diff and exits non-zero on drift is v1.
- **Catalog DESCRIBE cross-check**. v0 trusts the State-JSON header as the source of truth for prior state. v1 adds a DESCRIBE TABLE path through the driver and warns when file-state and catalog-state disagree.
- **Squashing / collapsing**. Django has `squashmigrations`; we don't yet. Generated files accumulate chronologically. For long-running projects, a squash pass is v1.
- **Execution from lakeorm.Client**. `MigrateStatus`, `MigratePlan`, `MigrateApply`, `DamVersion` were removed — execution is lake-goose's job, not lakeorm's.

## Pointers

- [`lake-goose`](https://github.com/datalake-go/lake-goose) — the goose fork with iceberg + delta dialects.
- [`spark-connect-go`](https://github.com/datalake-go/spark-connect-go) — the `database/sql` driver lake-goose executes against.
- [pressly/goose](https://github.com/pressly/goose) — upstream goose docs; every non-dialect behaviour carries over.
- [Django migrations](https://docs.djangoproject.com/en/5.0/topics/migrations/) — the `makemigrations` mental model this design adopts.
- [Ariga Atlas](https://atlasgo.io/concepts/migration-directory-integrity) — `atlas.sum` spec.
