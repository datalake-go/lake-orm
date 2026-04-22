# lake-orm conventions

This document is the contract every contributor follows when writing or reviewing code in lake-orm, its sibling repos (`lakehouse`, `lake-k8s`, `lake-dashboard`, `spark-connect-go`), and the wikis that document them.

It is not a wishlist. Every rule below is applied uniformly across the tree — no exceptions, no "but my file is special." The point of this document is that a reader can look at the filesystem, read a few method names, and already know how the system is implemented without opening a single file.

## Why consistency matters

The goal is that someone can look at the filesystem, read a few method names, and already know how the system is implemented without opening a single file. If every package uses the same structure (`structs.go`, `struct_tag.go`, `struct_schema.go`, `struct_validate.go`), and every method follows the same naming rules (`Find` vs `Fetch`, `Batch` prefix for slices, `ClassifyOp` not `Categorize`), then the codebase becomes predictable. You stop needing to check whether this particular package did things differently.

This has compounding benefits:

- Consistent naming means fewer one-off types, which means less dead code to accumulate and clean up later.
- Consistent structure means new packages are trivial to scaffold — copy the shape, fill in the domain.
- Consistent patterns mean code review is faster because reviewers aren't evaluating novel approaches, they're checking that the established approach was followed correctly.

**When in doubt, match what's already there.** If the existing code does it one way, do it the same way — even if you'd personally prefer something different. Local consistency beats individual preference. If the convention is wrong, the fix is to change the convention everywhere and update this document in the same PR, not to land a one-off exception.

---

## 1. File naming — every file is prefixed with the package's dominant noun

**Rule:** inside a package `foo`, every file is named `foo_<concept>.go`. Inside a nested package whose dominant noun is `bar`, every file is named `bar_<concept>.go`. Tests mirror the source file: `foo_tag.go` → `foo_tag_test.go`.

**Why:** reading the layout top-to-bottom the pattern is immediate. `ls drivers/spark/driver_spark_*` lands on every Spark driver file in contiguous lexicographic order. `grep -r "^package structs"` lands on the whole `structs` package. No ambiguity about "does session belong to the driver?" — the filename answers it. A new contributor looking for "where does the ingest ID get generated" runs `ls types/type_ingest_id*` and reads exactly those files, skipping every unrelated neighbour.

**Examples:**

```
lake-orm/
  lakeorm.go
  lakeorm_client.go
  lakeorm_client_impl.go
  lakeorm_dataframe.go
  lakeorm_driver.go
  lakeorm_migrate.go
  lakeorm_insert.go

structs/
  structs.go
  struct_tag.go
  struct_schema.go
  struct_table.go
  struct_validate.go
  struct_json.go
  struct_errors.go

drivers/spark/
  driver_spark.go
  driver_spark_remote.go
  driver_spark_cluster.go
  driver_spark_session.go
  driver_spark_sql.go
  driver_spark_factory.go

internal/migrations/
  migrations.go
  migration_classify.go
  migration_diff.go
  migration_generate.go
  migration_state.go
  migration_fingerprint.go
  migration_schema.go
  migration_slugify.go
  migration_sum.go

errors/
  errors.go
  error_cluster_not_ready.go

types/
  type_ingest_id.go
  type_object.go
  type_sortable_id.go
  type_system_columns.go
  type_table_name.go
```

**Package root file** (the one named after the package itself — `lakeorm.go`, `structs.go`, `migrations.go`) holds the package doc and, if short, entry-point functions. Everything else goes in a prefixed sibling.

---

## 2. Package docs — every package answers two questions

Every package opens with a `// Package X ...` doc block that answers:

1. **What it is** — one sentence, the thing itself.
2. **Why the concept exists** — the problem the package solves, the invariant it maintains.

**Do not** explain "why it's not at the root" or "why it's in its own package." That's circular: the reason is "because that's where it lives." The doc serves the reader asking *what is this for*, not the contributor speculating about architecture choices.

Every exported type also gets a godoc. When the name isn't self-evident from its signature, the doc explains the **why** — what problem the type solves that a reader couldn't infer from its fields. "Rows of result data" is not a useful doc for `type Row`; "the driver-agnostic row handle scanner consumes when no typed path is wired" is.

**Thirty-second test:** a reader who opens a package and reads only the doc block should be able to answer "why does this exist?" in under 30 seconds. If they can't, the doc hasn't done its job.

---

## 3. Verb taxonomy — one verb per concept, enforced across every package

When a concept has a canonical verb, every layer of the stack that touches the concept uses the same verb. No synonyms. No per-package variants.

| Concept | Canonical verb | Forbidden synonyms |
|---|---|---|
| Retrieve; absent is OK (returns nil/nil) | `Find<Subject>` | `Get`, `Lookup`, `Read`, `Locate` |
| Retrieve; absent is an error | `Fetch<Subject>` | `Require`, `MustGet`, `Load` |
| Boolean predicate | `Is<Subject>` | `Has`, `Check`, `Looks`, `Test`, `Matches` (regex is domain-native) |
| Slice op / batch | `Batch<Op>` | `Many`, `Bulk`, `All`, `Multi`, plain plural |
| Pure computation | `Compute<Subject>` | `Calc`, `Derive`, `Get`, `Make` |
| Categorize | `Classify<Subject>` | `Categorize`, `Tag`, `Determine` |
| Constructor | `new<Type>` private / `New<Type>` public | `Make`, `Build`, `Create`, `Construct` |
| Constructor for an error type | `NewErr<X>` matching `Err<X>` struct | `ClassifyCluster…`, `ToErrClusterNotReady` |
| Predicate for a typed error | `IsErr<X>` matching `Err<X>` | `HasErr…`, `Check…` |
| Validate; boundary check | `Validate` | `Check`, `Verify` (Verify would mean I/O probe — distinct) |
| Strict decode-and-validate from wire | `FromJSON[T]` / `FromJSONReader[T]` | the one place `From` is the right prefix — it's the decoder family |
| Emit an artefact to disk | `Generate<Artefact>` | pick one per codebase — lake-orm picked `Generate` |
| Remove abandoned state | `Cleanup<Subject>` | `Cleanse`, `Sweep`, `Prune`, `Gc` |
| Materialise typed rows from a DataFrame | `CollectAs[T]` / `StreamAs[T]` / `FirstAs[T]` | — `As` suffix is the canonical type-binding marker |
| Run a typed SQL read | `Query[T]` / `QueryStream[T]` / `QueryFirst[T]` | no third synonym |

**Concrete example — Find vs Fetch at the callsite:**

```go
// Find — absence is a normal case; caller decides what to do
schema, err := structs.FindSchema(ctx, reflect.TypeOf(&User{}))
if schema == nil {
    // not parsed yet, fall through to default derivation
}

// Fetch — absence means something went wrong
schema, err := structs.FetchSchema(ctx, reflect.TypeOf(&User{}))
if errors.Is(err, errors.ErrNotFound) {
    return status.Errorf(codes.NotFound, "no schema registered for %T", req)
}
```

The distinction runs end-to-end. If the DAO layer exposes `FindX` it returns `(nil, nil)` when nothing matches; if it exposes `FetchX` it returns a typed `ErrNotFound`. At the service layer, `FindVersionID` returns nil when no version exists, and a sibling `FetchVersionID` wraps it to turn nil into an error. Callers pick the shape that matches their semantic expectation — no ambiguity, no "did nil mean not-found or error?" tangent during review.

**Concept-naming rule:** if a concept has a public-API verb (`Find`, `Fetch`, `Is`, `Batch`, `Compute`, `Classify`, `Cleanup`, `Generate`) the same verb must be used at every layer of the stack. If `Classify` is the verb internally in `internal/migrations`, then the public surface (if any) uses `Classify` too — no `ClassifyOp` vs `OperationType` vs `ChangeKind` across layers for the same thing.

**One type, one constructor.** `StagingPrefix` → `NewStagingPrefix(uri, ingestID)`. Never `NewStagingPrefix` alongside `MakeStagingPrefix` or `StagingPrefixFrom(...)`.

**Pre-flight audit** before any release: grep every exported function, group by verb prefix, flag any verb used in two senses, or any concept named by two different verbs. Fix before merging.

---

## 4. Constructors — every struct has one

Every exported struct gets `NewX`. Every unexported struct that carries dependencies gets `newX`. If the struct is a pure-data value (no constructor logic), a `NewX(fields...) X` that returns the value literal is still preferred — future field additions flow through the constructor without touching every callsite.

Constructors for errors follow the `NewErrX` / `IsErrX` pair rule (see §3). The three identifiers — the struct `ErrX`, the constructor `NewErrX`, the predicate `IsErrX` — always grep together.

No struct is ever built with a struct literal outside the constructor's own file unless it's a pure data transfer object the caller legitimately fills field-by-field (e.g. option structs passed to dialects during plan construction).

---

## 5. Public surface minimisation — push to `internal/` by default

The root `lakeorm` package advertises exactly what `README.md` and `examples/` demonstrate to users. Everything else is `internal/`.

**On the root SDK surface:**

- `Client` interface + `Open()` constructor
- `Query[T]` / `QueryStream[T]` / `QueryFirst[T]` + `CollectAs[T]` / `StreamAs[T]` / `FirstAs[T]`
- `structs.Validate` / `structs.FromJSON[T]` / `structs.FromJSONReader[T]` / `structs.TableNamer`
- `backends.S3` / `backends.GCS` / `backends.File` / `backends.Memory`
- `drivers/spark.Remote`, `drivers/duckdb.Driver`, `drivers/databricks.Driver`, `drivers/databricksconnect.Driver`
- `dialects/iceberg.Dialect`, `dialects/delta.Dialect`, `dialects/duckdb.Dialect`
- `errors.Err*` + `errors.NewErr*` + `errors.IsErr*`
- `types.IngestID`, `types.SortableID`, `types.Location`, `types.SystemIngestIDColumn`

**Not on the surface (lives in `internal/`):**

- `internal/scanner` — row-to-struct reflection scanner
- `internal/parquet` — parquet schema synthesis and partition writer
- `internal/migrations` — diff + classify + emit goose files
- `internal/sqlbuild` — SELECT-clause builder

**Rule of thumb:** if no external example, README section, or wiki page calls the identifier, it's internal. Tests don't count — tests can live inside the internal package.

---

## 6. Interface placement — interfaces live in the package they contract

The `Driver` interface lives in `drivers/`. The `Dialect` interface lives in `dialects/`. The `Backend` interface lives in `backends/`. Not at the root.

**Why:** grepping for `drivers.Driver` lands on the interface contract and the implementations in sibling directories with one go command. Having the interface at the root and implementations in a subpackage puts the contract two packages away from the implementations that satisfy it.

The `Client` interface stays at the root because the root package is the composition root — `Client` is what `lakeorm.Open(...)` returns, and the top-level SDK surface is what `lakeorm.<X>` means.

---

## 7. Method ordering — callers above callees

Within a file, private methods are ordered by call precedence — a method appears above the methods it calls, so you read top-down and follow the flow without jumping around.

**Order:**

1. Constructor (`NewX` / `newX`).
2. Public methods in logical order — the entry points callers reach for.
3. Private methods, each above the methods it calls, down to standalone helpers at the bottom.

No `// --- Public methods ---` section comments. The ordering itself is the structure; a separator comment is redundant.

**Exception:** a file that contains only types + their accessor methods (e.g. `types/type_ingest_id.go`) can alphabetise its methods — call precedence doesn't apply when there are no internal callers.

---

## 8. Error messages — always prefixed, never anonymous

Every `fmt.Errorf` or `errors.New` starts with `"lakeorm: "` (or the specific subpackage identifier — `"migrations: "`, `"scanner: "`, `"iceberg: "`). An unwrapped error string must identify the source library.

Error **sentinels** are declared in the `errors` subpackage alphabetically, with one blank line between unrelated groups if the list grows long. Error **types** live in their own `error_<name>.go` file with the `NewErrX` constructor and `IsErrX` predicate in the same file.

---

## 9. Migrations — one package, one responsibility

`internal/migrations` is lakeorm's authoring side of schema evolution. Given a tagged Go struct and the State-JSON header of the most-recent migration file for the same table, it computes the diff, classifies each change as destructive or safe per the dialect rule table, and emits one goose-format `.sql` file with `-- DESTRUCTIVE: <reason>` comments on anything a reviewer should notice.

**Execution is lake-goose's job, not lakeorm's.** This package writes files; lake-goose runs them. The asymmetry is deliberate — keeping the writer and the runner in separate binaries means users can pin different versions of each, swap runners for a different dialect without touching the authoring side, and a single failure mode is always "the file on disk is wrong" rather than "the runner bailed mid-apply."

**Manifest:** every `MigrateGenerate` call rewrites `lakeorm.sum` at the migrations-dir root. Line 1 is `h1:<base64(sha256(body))>`; each subsequent line is `<filename> h1:<base64(sha256(file))>`. Any post-generation edit changes the top-line hash. A future `lakeorm migrate --check` or custom CI job verifies the manifest; writing is automatic, verification is a reviewer / CI concern.

---

## 10. CQRS — writes bind to persisted types; reads bind to projection types

The write-side struct is the persisted-schema contract. One struct per table. `lake:"..."` tags describe the on-disk shape.

The read-side struct is the projection-shape contract. One struct per query shape. Joins and aggregates produce rows that don't match any write-side struct; declare a fresh projection struct, materialise through `Query[T]` / `QueryStream[T]` / `QueryFirst[T]` (closure-based, driver-native source in).

Unified-ORM "one struct everywhere" patterns collapse under joins. The CQRS split sidesteps the whole problem.

**Whole types over partial projections.** Prefer returning full structs from simple reads rather than creating one-off projection types that hold a subset of columns. If `Query[User]` returns the complete `User`, callers read `.Email`, `.CreatedAt`, whatever they need — the handler picks what to expose; the query doesn't try to guess.

Projection types are the right call for **high-cardinality cross-table joins where the column savings actually matter**. A `CountryAggregate{Country, OrderCount, RevenuePence}` pulled from a 50M-row join is worth the dedicated type. A `UserEmail{ID, Email}` pulled from a 10K-row users table is not — just `Query[User]` and let the caller reach for `.Email`. Constantly creating one-off projection structs leads to an explosion of types that accumulate as dead code; you end up reading and writing 4× the code for exactly the same output. Fewer one-off structs means less dead code, more reuse, and a codebase that's easier to read and follow.

See [`TYPING.md`](TYPING.md) for the design contract.

---

## 11. The tag trio — `lake:"..."`, `lakeorm:"..."`, `spark:"..."` parse identically

All three tag keys parse identically. Pick whichever reads best in a given codebase. A single field carrying non-empty values on two keys is a parse-time `structs.ErrInvalidTag`.

`dorm:"..."` is deliberately unrecognised — the library rename was a one-way move.

---

## 12. No re-exports for back-compat during pre-v1

lake-orm is pre-v1. When a type or function moves, callers update. We do not carry `type X = otherpkg.X` aliases or `var Y = otherpkg.Y` re-exports during the pre-v1 reshape. Aliases rot into forever-compat debt; breaking cleanly is the expected cost of pre-v1.

Post-v1, any breaking move requires a new major version. Until then, break freely.

---

## 13. No `//go:build enterprise` tags, no feature flags, no telemetry

Carries over from [CONTRIBUTING.md](CONTRIBUTING.md)'s Design Axioms. lake-orm is vendorless, reproducible, and you-run-it. Pull requests that add a vendor dependency, an `enterprise` build tag, a feature-flag mechanism, or a telemetry call go back for a rethink.

---

## 14. Services vs clients — where business logic lives

This distinction tells you where to look for logic, and it tells every PR reviewer where a new responsibility belongs.

- A **service** has business logic. It validates inputs, coordinates between dependencies, and enforces domain rules. The root `lakeorm.Client` is a service — it generates ingest IDs, validates records before writing, hands the plan to the Dialect, orchestrates the Finalizer. Users of the service get a clean typed API; the service's methods own the correctness story.
- A **client** (in the "wraps an external system" sense) has no business logic. It talks to one thing and exposes a clean interface. Every concrete driver (`drivers/spark.Remote`, `drivers/duckdb.Driver`) is a client — it forwards a plan to the engine, returns a row source, closes on shutdown. The driver doesn't validate records, route write paths, or decide on merge semantics; those are the service's job.

**If you're adding something that just talks to an external system, it's a client — live in `drivers/`, `backends/`, or `dialects/`. If you're adding something that validates, transforms, or coordinates, it's a service — live in the root `lakeorm` package or in `internal/` if it's an implementation detail.** The distinction keeps layer violations easy to spot: a driver that has started calling `Validate()` or deciding on merge strategies has drifted into service territory and needs to delegate back up the stack.

---

## 15. The Go struct is the source of truth

The tagged Go struct — `User{ID string \`lake:"id,pk"\`; Email string \`lake:"email,mergeKey" validate:"required,email"\`}` — is the authoritative description of the data in the lakehouse. Everything else — the CREATE TABLE DDL, the parquet schema, the goose migration files, the fingerprint in the manifest, the projection shapes on the read side — is derived from it.

When a domain concept changes — a new field, a type change, a merge-key addition, a rename — the struct changes first, in the same PR that ships any migration or downstream consumer update. A generated migration that doesn't match the current struct is a bug; a struct change without a corresponding migration PR is a bug; a comment or doc that describes a different shape is a bug.

**There is one source of truth and it lives in the Go code.** Migration files, generated SQL, wiki examples, and any future codegen must match what the struct says. Any divergence surfaces first at the next `MigrateGenerate` run (via the `lakeorm.sum` manifest hash) or at fingerprint-comparison time (in a future `AssertSchema` / `lakeorm migrate --check`) — whichever happens first, the tooling catches it before production does.

---

## 16. Generated files carry `_gen.go` and are never hand-edited

Any file produced by a generator — mocks, proto stubs, future label-schema expansions, any `go generate`-driven artefact — is named `<concept>_gen.go` (or prefixed per rule 1: `<package>_<concept>_gen.go`). The `_gen.go` suffix is a promise that the file is rewritten on the next `go generate` run; hand edits disappear.

Every generator lives under `tools/<generator-name>/` with its own `main.go` and a `//go:generate go run .` directive pointing at it. Shared codegen utilities live in `tools/codegenutils/` (when that pattern lands — not yet required at v0).

Lake-orm at v0 has one generator in spirit (`MigrateGenerate`), but the files it produces are timestamped goose-format SQL, not `_gen.go` — those are artefacts users edit-review-commit, not regenerated code. When future codegen lands (proto-derived label schemas, Iceberg-catalog introspection, etc.), this rule governs it.

---

## 17. End-to-end gate — release-ready proof

Before cutting any release, the full demo path must run clean from an empty clone:

```bash
git clone https://github.com/datalake-go/lake-orm.git
cd lake-orm
make docker-up                   # SeaweedFS + Spark Connect local stack
go run ./examples/basic          # write + typed query
go run ./examples/migrations     # MigrateGenerate + lakeorm.sum
go run ./examples/arbitrary_spark_query   # Convertible + native Spark DataFrame
goose -dir ./migrations iceberg "sc://localhost:15002" up   # lake-goose applies the generated migrations
```

If any step fails, the release isn't ready. This is the repeatable full path every contributor runs before marking a branch mergeable.

---

## See also

- [`CONTRIBUTING.md`](CONTRIBUTING.md) — Design Axioms + PR etiquette.
- [`MIGRATIONS.md`](MIGRATIONS.md) — the three-role split (lakeorm authors, lake-goose runs, humans review).
- [`TYPING.md`](TYPING.md) — CQRS + typed-at-the-edge design contract.
- [`TECH_SPEC.md`](TECH_SPEC.md) — write-path lifecycle + ingest_id threading.
