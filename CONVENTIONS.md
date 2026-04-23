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

**Nuance — prefix when there's a family, not when the file IS the concept.** Inside `types/` each file IS the top-level concept (ingest_id, location, sortable_id), so `ingest_id.go` is the right name — a `type_ingest_id.go` prefix is redundant because the package is already `types`. Inside `drivers/spark/`, `driver_spark.go` and `driver_spark_remote.go` belong to a family (there are multiple driver variants), so the prefix disambiguates within the family. A good test: if you split the concept across multiple files, those files should share a prefix (`cluster.go` + `cluster_helpers.go`, not `cluster.go` + `helpers.go`). If the concept lives in one file, bare name.

**Why this matters.** You shouldn't need to open a file to know which family it belongs to. `ls drivers/spark/driver_spark_*` tells you the driver family's files in one glance; you read them in order, you skip the rest. `grep -rn "^package migrations"` lands on every migration-related file in one command. Without the prefix you're guessing from bare filenames (`cluster.go` — cluster of what?), opening speculatively, and bouncing back out.

**What this accomplishes.** Navigation is free. A reader looking for "where does the driver talk to Spark?" runs `ls drivers/spark/driver_spark_*` and reads those files, period. A new contributor scaffolding a fresh driver family copies the shape. File listings visually cluster related work in alphabetical order because the shared prefix sorts them together.

---

## 2. Package docs — every package answers two questions

Every package opens with a `// Package X ...` doc block that answers:

1. **What it is** — one sentence, the thing itself.
2. **Why the concept exists** — the problem the package solves, the invariant it maintains.

**Do not** explain "why it's not at the root" or "why it's in its own package." That's circular: the reason is "because that's where it lives." The doc serves the reader asking *what is this for*, not the contributor speculating about architecture choices.

Every exported type also gets a godoc. When the name isn't self-evident from its signature, the doc explains the **why** — what problem the type solves that a reader couldn't infer from its fields. "Rows of result data" is not a useful doc for `type Row`; "the driver-agnostic row handle scanner consumes when no typed path is wired" is.

**Thirty-second test:** a reader who opens a package and reads only the doc block should be able to answer "why does this exist?" in under 30 seconds. If they can't, the doc hasn't done its job.

**Why this matters.** A reader arriving cold at `internal/parquetschema` or `drivers/spark` shouldn't have to infer purpose from filenames, type names, and method signatures working backwards. The package doc is the one place that states the invariant the rest of the package maintains. Without it, every reader re-derives the purpose for themselves; some get it wrong. With it, one author writes the answer once and every reader lands on the same understanding.

**What this accomplishes.** Onboarding cost drops — a new contributor can read six package docs in ten minutes and know the whole architecture without reading any code. Design conversations compress — when someone proposes moving logic into a package, the package's stated invariant immediately tells you whether it fits or not. Stale docs become a bug — when the doc and the code disagree, one of them is wrong, and whichever is wrong gets fixed in the next PR instead of rotting quietly for a release cycle.

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

**Why this matters.** Two reasons, both load-bearing.

**You know the behaviour from the signature, not the body.** `Get` is ambiguous: it might return `(nil, nil)` when the row is missing, or it might return an error. You have to read the function body — or worse, call it and check at runtime — to find out. `Find` and `Fetch` remove that ambiguity by construction: `Find` means absence is a normal case and the caller gets `(nil, nil)`; `Fetch` means absence is an error. The reader never has to look past the signature. Every caller of every lake-orm function in the codebase gets this property for every canonical verb.

**Grep becomes a navigation tool.** `grep -rn "^func Fetch"` finds every "absence is an error" callsite in one command. `grep -rn "^func Find"` finds every "absence is fine" callsite. `grep -rn "^func Is"` finds every predicate. If the codebase lets `Get` / `Lookup` / `Find` / `Read` all mean "retrieve," none of those greps work — you'd have to grep all four, sort, deduplicate, and then read each one to see which variant it really was. Locking one verb per concept means grep is the answer, not the start of the investigation.

**What this accomplishes.** Review compresses. Readers stop context-switching to the docstring for every retrieval call. Dead code surfaces sooner — if a `GetFoo` exists alongside `FindFoo` and the rule forbids both, one of them was never really needed, and the cleanup removes one for free. New contributors learn each verb exactly once; familiar contributors never guess.

---

## 4. Constructors — every struct has one

Every exported struct gets `NewX`. Every unexported struct that carries dependencies gets `newX`. If the struct is a pure-data value (no constructor logic), a `NewX(fields...) X` that returns the value literal is still preferred — future field additions flow through the constructor without touching every callsite.

Constructors for errors follow the `NewErrX` / `IsErrX` pair rule (see §3). The three identifiers — the struct `ErrX`, the constructor `NewErrX`, the predicate `IsErrX` — always grep together.

No struct is ever built with a struct literal outside the constructor's own file unless it's a pure data transfer object the caller legitimately fills field-by-field (e.g. option structs passed to dialects during plan construction).

**Why this matters.** Two things go wrong without a constructor. First, field additions become a cross-repo sweep: add a `MetricsRegistry` field to `client`, and every place that built `client{...}` by hand now silently zero-initialises it; compiler doesn't help, tests might pass. With a single constructor, the new argument is a compiler error at every callsite — you can't forget. Second, invariants have no enforcement site: if the only way to set `idempotencyTTL` was via a struct literal, callers who forget produce instances with `time.Duration(0)` and every timeout happens immediately. A constructor is the one place those invariants live, checked once, for everyone.

**What this accomplishes.** The invariant stays in the constructor and out of the caller's head. When a new field with a non-zero default arrives, you change the constructor and every existing caller is automatically correct. New contributors reading a package find the "how to build one" answer at the top of the file instead of reverse-engineering from callers. And the `Err<X>` / `NewErr<X>` / `IsErr<X>` triad stays grep-trivial — three identifiers, one concept, one file.

---

## 5. Public surface minimisation — push to `internal/` by default

The root `lakeorm` package advertises exactly what `README.md` and `examples/` demonstrate to users. Everything else is `internal/`.

**On the root SDK surface:**

- `Client` interface + `Open()` constructor + `Client.Driver()` accessor
- `Query[T]` / `QueryStream[T]` / `QueryFirst[T]` over `drivers.Source`
- `drivers.Source` + `drivers.Convertible` contract in `drivers/drivers.go`
- `structs.Validate` / `structs.FromJSON[T]` / `structs.FromJSONReader[T]` / `structs.TableNamer`
- `backends.S3` / `backends.GCS` / `backends.File` / `backends.Memory`
- `drivers/spark.Remote`, `drivers/duckdb.New`, `drivers/databricks.New`, `drivers/databricksconnect.Driver`
- Per-driver `FromSQL` / `FromDataFrame` / `FromTable` / `FromRow` / `FromRows` / `Session` / `DB` on the concrete `*Driver` types
- `dialects/iceberg.Dialect`, `dialects/delta.Dialect`, `dialects/duckdb.Dialect`
- `errors.Err*` + `errors.NewErr*` + `errors.IsErr*`
- `types.IngestID`, `types.SortableID`, `types.Location`, `types.SystemIngestIDColumn`

**Not on the surface (lives in `internal/`):**

- `internal/scanner` — row-to-struct reflection scanner
- `internal/parquet` — parquet schema synthesis and partition writer
- `internal/migrations` — diff + classify + emit goose files
- `internal/sqlbuild` — SELECT-clause builder

**Rule of thumb:** if no external example, README section, or wiki page calls the identifier, it's internal. Tests don't count — tests can live inside the internal package.

**Reward the reader.** Every exported identifier costs a reader cycles. A future contributor (or a future AI agent) opens the codebase, sees `types.SparkTableName`, spends time understanding what it is and how it relates to the rest, then discovers nothing uses it. That's wasted attention on something the library never intended to ship. Unexport it or delete it. The reader's time is a budget, and every leaked-out helper in the public surface is a tiny tax on that budget every time the file gets opened.

**Concrete candidates to reap in future PRs** (updated as the surface gets reviewed):

- ~~`types.SparkTableName` / `types.ParseSparkTableName`~~ — reaped: zero callers, never on contract.
- ~~`DataFrame` / `Row` / `RowStream` / `ColumnInfo` at root~~ — reaped in phase-2f with the Convertible restructure; the interface was a forced abstraction each driver had to implement whether or not it had a native DataFrame concept.
- ~~`QueryBuilder` / `Client.Query(ctx)` / `dynamicQuery` / `Client.DataFrame(ctx, sql, args)`~~ — reaped in phase-2f with the Convertible restructure; the typed `Query[T]` family over `drivers.Source` is the only documented read path.
- ~~`Driver.DataFrame` / `Driver.ExecuteStreaming`~~ — reaped in phase-2f; the read-side lives on `drivers.Convertible`, which is an optional driver capability.
- ~~`Dialect.PlanQuery` / `QueryRequest` / `OrderSpec` / `NewOrderSpec`~~ — reaped in phase-2g as dead code after `dynamicQuery` went away.
- ~~`Driver` / `Finalizer` / `Result` / `ExecResult` at root~~ — moved to `drivers/drivers.go` in phase-2g (Rule 6: interfaces live in the package they contract).
- ~~`ExecutionPlan` / `PlanKind` / `StagingRef` / `WriteRequest` / `WritePath` at root~~ — moved to `drivers/drivers.go` in phase-2g alongside the Driver contract.
- ~~`Dialect` at root~~ — moved to `dialects/dialects.go` in phase-2g.
- ~~`Backend` at root~~ — moved to `backends/backends.go` in phase-2g.
- `drivers/spark.SessionPool` — impl detail; users never construct session pools themselves, they let `Remote()` do it.

**Why this matters.** Every public identifier is a contract — a promise that its signature, semantics, and behaviour won't change without a major version bump. More public identifiers means more promises to keep, which means more churn, deprecation shims, and "we can't fix this because it'd break user X" conversations later. Keeping the surface small keeps the promises small. If nothing outside the repo uses `internal/scanner.NewScanner`, it has zero users constraining it — the library is free to change its signature, its decoding strategy, its whole existence, without notice.

**What this accomplishes.** Refactoring stays cheap. The `internal/` prefix is a compiler-enforced "do not depend on this" sign for downstream projects. When someone asks "can we rename this?", the answer is "yes if it's in `internal/`, possibly-with-a-deprecation-cycle if it's public." Release notes shrink — only intentional surface additions need mentioning. And the 30-second-onboarding test (see §2) gets easier: a reader skimming the SDK surface sees exactly what users actually call, not every helper the maintainers chose to expose by accident.

---

## 6. Interface placement — interfaces live in the package they contract

The `Driver` interface lives in `drivers/`. The `Dialect` interface lives in `dialects/`. The `Backend` interface lives in `backends/`. Not at the root.

**Why:** grepping for `drivers.Driver` lands on the interface contract and the implementations in sibling directories with one go command. Having the interface at the root and implementations in a subpackage puts the contract two packages away from the implementations that satisfy it.

The `Client` interface stays at the root because the root package is the composition root — `Client` is what `lakeorm.Open(...)` returns, and the top-level SDK surface is what `lakeorm.<X>` means.

**What this accomplishes.** A reader asking "what does a driver have to implement?" runs `cat drivers/drivers.go` and reads the contract, then opens any sibling (`drivers/spark/`, `drivers/duckdb/`) and sees the implementation side-by-side. When the interface and its implementations live in different packages, that conversation needs two `cd` commands and a mental link — and when the interface moves or a new implementation lands, there's a higher chance something falls out of sync. Colocating the interface with its family keeps the two sides in one `grep -r` reach, which is the mechanism that keeps them honest.

---

## 7. Method ordering — callers above callees

Within a file, private methods are ordered by call precedence — a method appears above the methods it calls, so you read top-down and follow the flow without jumping around.

**Order:**

1. Constructor (`NewX` / `newX`).
2. Public methods in logical order — the entry points callers reach for.
3. Private methods, each above the methods it calls, down to standalone helpers at the bottom.

No `// --- Public methods ---` section comments. The ordering itself is the structure; a separator comment is redundant.

**Exception:** a file that contains only types + their accessor methods (e.g. `types/ingest_id.go`) can alphabetise its methods — call precedence doesn't apply when there are no internal callers.

**Why this matters.** Top-to-bottom reading works. You open the file, you read the constructor, you read the public methods in the order callers reach for them, and every time a private helper shows up you've already seen the caller that triggered it. No scrolling back, no "where does this get called from?" detour, no `grep` to find the caller. When methods are sorted alphabetically (or at random), every helper is a cliffhanger and reading the file takes three passes: one for shape, one for control flow, one for the detail you're actually after.

**What this accomplishes.** Code review is measurably faster because reviewers can read in a single direction. `// --- Private methods ---` separator comments become redundant — the ordering IS the structure, and a reader can tell public from private from context without a comment announcing it. New methods land in the right place because there's exactly one right place: above their callees, below their callers.

---

## 8. Error messages — always prefixed, never anonymous

Every `fmt.Errorf` or `errors.New` starts with `"lakeorm: "` (or the specific subpackage identifier — `"migrations: "`, `"scanner: "`, `"iceberg: "`). An unwrapped error string must identify the source library.

Error **sentinels** are declared in the `errors` subpackage alphabetically, with one blank line between unrelated groups if the list grows long. Error **types** live in their own `error_<name>.go` file with the `NewErrX` constructor and `IsErrX` predicate in the same file.

**Why this matters.** When an error bubbles up through a caller's service and hits their logger as a string, it's often the only signal anyone has about which library produced it. `"no rows"` could come from anywhere. `"lakeorm: no rows"` can be traced to this repo in one grep. Operators who hit a stack trace at 3 AM find the right codebase in seconds; SREs who write log-matching rules can target one library deterministically.

**What this accomplishes.** Observability becomes cheaper. Log aggregators can filter by prefix. Test assertions that check for a specific error can match on prefix without brittle full-string comparisons. Callers deliberating whether to retry can see at a glance which library owns the failure. And the prefix nudges authors toward thinking about the error as a library emission — "does this message tell the user what went wrong?" — instead of a throwaway `fmt.Errorf("oh no")`.

---

## 9. Migrations — one package, one responsibility

`internal/migrations` is lakeorm's authoring side of schema evolution. Given a tagged Go struct and the State-JSON header of the most-recent migration file for the same table, it computes the diff, classifies each change as destructive or safe per the dialect rule table, and emits one goose-format `.sql` file with `-- DESTRUCTIVE: <reason>` comments on anything a reviewer should notice.

**Execution is lake-goose's job, not lakeorm's.** This package writes files; lake-goose runs them. The asymmetry is deliberate — keeping the writer and the runner in separate binaries means users can pin different versions of each, swap runners for a different dialect without touching the authoring side, and a single failure mode is always "the file on disk is wrong" rather than "the runner bailed mid-apply."

**Manifest:** every `MigrateGenerate` call rewrites `lakeorm.sum` at the migrations-dir root. Line 1 is `h1:<base64(sha256(body))>`; each subsequent line is `<filename> h1:<base64(sha256(file))>`. Any post-generation edit changes the top-line hash. A future `lakeorm migrate --check` or custom CI job verifies the manifest; writing is automatic, verification is a reviewer / CI concern.

**Why this matters.** The authoring side and the runner side have different release cadences, different failure modes, and different on-call responders. If lake-orm emitted files AND applied them, every runtime bug in the apply path would be a lake-orm bug — users on old lake-orm versions would have to upgrade to get fixes they didn't ask for. By splitting the job, users pin the authoring version to whatever their build uses and pin the runner version independently; each evolves without pulling the other with it. And `-- DESTRUCTIVE:` comments land in the .sql file itself, not in some out-of-band annotation system, so the PR diff IS the review — no external state to check.

**What this accomplishes.** Failures get cleaner ownership — "MigrateGenerate produced a wrong file" vs "lake-goose applied the wrong file" are separable bugs with separable fix paths. Users can adopt a newer lake-orm for its authoring improvements without upgrading their runner, or vice versa. And the manifest hash means an `atlas.sum`-style tamper check works with any tool that reads the format — users can write their own verifier in five lines of shell.

---

## 10. CQRS — writes bind to persisted types; reads bind to projection types

The write-side struct is the persisted-schema contract. One struct per table. `lake:"..."` tags describe the on-disk shape.

The read-side struct is the projection-shape contract. One struct per query shape. Joins and aggregates produce rows that don't match any write-side struct; declare a fresh projection struct, materialise through `Query[T]` / `QueryStream[T]` / `QueryFirst[T]` (closure-based, driver-native source in).

Unified-ORM "one struct everywhere" patterns collapse under joins. The CQRS split sidesteps the whole problem.

**Whole types over partial projections.** Prefer returning full structs from simple reads rather than creating one-off projection types that hold a subset of columns. If `Query[User]` returns the complete `User`, callers read `.Email`, `.CreatedAt`, whatever they need — the handler picks what to expose; the query doesn't try to guess.

Projection types are the right call for **high-cardinality cross-table joins where the column savings actually matter**. A `CountryAggregate{Country, OrderCount, RevenuePence}` pulled from a 50M-row join is worth the dedicated type. A `UserEmail{ID, Email}` pulled from a 10K-row users table is not — just `Query[User]` and let the caller reach for `.Email`. Constantly creating one-off projection structs leads to an explosion of types that accumulate as dead code; you end up reading and writing 4× the code for exactly the same output. Fewer one-off structs means less dead code, more reuse, and a codebase that's easier to read and follow.

**Why this matters.** Unified-ORM libraries (GORM, ActiveRecord, Django's ORM) pick one struct per table and reuse it for both writes and reads. The pattern collapses the moment a query joins two tables or aggregates over them: the result shape isn't any single persisted struct, and the ORM either forces you to declare a hand-written projection type for every query (noisy), or erases the type to `map[string]any` (dishonest). CQRS sidesteps this by accepting that writes and reads have different shape contracts — one struct per table on the write side, one struct per projection on the read side — so neither side has to lie about the other.

**What this accomplishes.** Every read is honestly typed to exactly what the SQL returns. Every write is typed to exactly what's persisted. Refactoring a table doesn't force a sweep through every projection struct; refactoring a query doesn't force a sweep through every write callsite. The asymmetry is obvious rather than hidden, which means the library can optimise the write path (fast-path parquet staging, mergeKey-driven MERGE routing) without worrying about read ergonomics, and the read path can expose native DataFrame escape hatches (see `Convertible` in `drivers/drivers.go`) without worrying about write correctness.

See [`TYPING.md`](TYPING.md) for the design contract.

---

## 11. The tag trio — `lake:"..."`, `lakeorm:"..."`, `spark:"..."` parse identically

All three tag keys parse identically. Pick whichever reads best in a given codebase. A single field carrying non-empty values on two keys is a parse-time `structs.ErrInvalidTag`.

`dorm:"..."` is deliberately unrecognised — the library rename was a one-way move.

**Why this matters.** A struct tagged for lake-orm needs to round-trip through driver layers too — the Spark driver's typed DataFrame helpers read `spark:"..."` natively. Forcing users to pick ONE name locks them into either "the ORM name" or "the driver name"; accepting all three lets the same struct serve both roles without duplicate tags. Forbidding `dorm:"..."` prevents the rename from rotting into a fourth synonym that gets added back by someone who didn't realise it was retired.

**What this accomplishes.** One struct, one tag declaration, usable across the whole ecosystem. No "my struct works in lake-orm but not in the Spark fork's typed collect because my tag is the wrong name" friction. And the conflict-detection path (two non-empty keys on one field) catches drift at parse time — you can't accidentally end up with two sources of truth on a single field because the parser rejects the ambiguity.

---

## 12. No re-exports for back-compat during pre-v1

lake-orm is pre-v1. When a type or function moves, callers update. We do not carry `type X = otherpkg.X` aliases or `var Y = otherpkg.Y` re-exports during the pre-v1 reshape. Aliases rot into forever-compat debt; breaking cleanly is the expected cost of pre-v1.

Post-v1, any breaking move requires a new major version. Until then, break freely.

**Why this matters.** Aliases are the most innocuous-looking way to introduce technical debt. "Just one type alias to avoid breaking external callers" becomes five aliases becomes twenty, and five years later every refactor has to consider "does this affect the aliases?" Go's type-alias rules also make them subtly different from the originals (no method receivers on an alias, etc.), so they're not actually transparent. Pre-v1 is the one moment in a library's life when breaking is free — users have signed on for instability by not waiting for v1. Using that budget now costs nothing; not using it now means carrying the debt forever.

**What this accomplishes.** Every move in phase 2 (structs/ relocation, errors/ move, lakeorm_ prefix, types/ rename) completed without a single compatibility shim. The tree stays readable — one name per identifier, discoverable with grep. When v1 finally ships, the API will be the intentional shape we chose, not the historical accretion of "where things used to be" that aliases preserve.

---

## 13. No `//go:build enterprise` tags, no feature flags, no telemetry

Carries over from [CONTRIBUTING.md](CONTRIBUTING.md)'s Design Axioms. lake-orm is vendorless, reproducible, and you-run-it. Pull requests that add a vendor dependency, an `enterprise` build tag, a feature-flag mechanism, or a telemetry call go back for a rethink.

**Why this matters.** "Free and open source" libraries that carry enterprise tags, telemetry, or feature flags aren't free in the sense that matters — they're free until the maintainer decides to flip a flag. lake-orm is intentionally structured so that no such flip is possible: there is no premium tier to gate, no metric endpoint to call home from, no conditional-compilation seam through which the library's behaviour could diverge from what you read in the source. A clone + `go build` is the whole library, with no runtime surprises.

**What this accomplishes.** Operators deploying lake-orm know what they're running because they can read it. Security reviews don't need to audit "where does this call out to?" — the answer is "only to the endpoints the caller configured." Forks stay trivially possible because there's no vendored authentication layer or licence check to strip out. And the library stays operable in air-gapped environments (ML pipelines on isolated networks, regulated finance stacks, on-prem deployments) without special configuration.

---

## 14. Services vs clients — where business logic lives

This distinction tells you where to look for logic, and it tells every PR reviewer where a new responsibility belongs.

- A **service** has business logic. It validates inputs, coordinates between dependencies, and enforces domain rules. The root `lakeorm.Client` is a service — it generates ingest IDs, validates records before writing, hands the plan to the Dialect, orchestrates the Finalizer. Users of the service get a clean typed API; the service's methods own the correctness story.
- A **client** (in the "wraps an external system" sense) has no business logic. It talks to one thing and exposes a clean interface. Every concrete driver (`drivers/spark.Remote`, `drivers/duckdb.Driver`) is a client — it forwards a plan to the engine, returns a row source, closes on shutdown. The driver doesn't validate records, route write paths, or decide on merge semantics; those are the service's job.

**If you're adding something that just talks to an external system, it's a client — live in `drivers/`, `backends/`, or `dialects/`. If you're adding something that validates, transforms, or coordinates, it's a service — live in the root `lakeorm` package or in `internal/` if it's an implementation detail.** The distinction keeps layer violations easy to spot: a driver that has started calling `Validate()` or deciding on merge strategies has drifted into service territory and needs to delegate back up the stack.

**Why this matters.** When business logic is scattered — some in the driver, some in the dialect, some in the client — every bug investigation starts with "where does validation happen again?" Answer varies per feature. With the split, the answer is always "the service layer." Drivers stay simple (one external integration, one file), which keeps them easy to audit, easy to swap (new Spark alternative, new ClickHouse target), and easy to write the hundredth time when someone adds driver #N+1.

**What this accomplishes.** Swappability. A new driver author doesn't need to learn lake-orm's write-path routing, merge semantics, or validation rules — they implement five driver methods and the service layer orchestrates them. Bug ownership is unambiguous: a correctness bug in "write path routing" is a service bug (root lakeorm), a bug in "Spark Connect returned garbage" is a client bug (drivers/spark). Reviewers catch layer violations on sight.

---

## 15. The Go struct is the source of truth

The tagged Go struct — `User{ID string \`lake:"id,pk"\`; Email string \`lake:"email,mergeKey" validate:"required,email"\`}` — is the authoritative description of the data in the lakehouse. Everything else — the CREATE TABLE DDL, the parquet schema, the goose migration files, the fingerprint in the manifest, the projection shapes on the read side — is derived from it.

When a domain concept changes — a new field, a type change, a merge-key addition, a rename — the struct changes first, in the same PR that ships any migration or downstream consumer update. A generated migration that doesn't match the current struct is a bug; a struct change without a corresponding migration PR is a bug; a comment or doc that describes a different shape is a bug.

**There is one source of truth and it lives in the Go code.** Migration files, generated SQL, wiki examples, and any future codegen must match what the struct says. Any divergence surfaces first at the next `MigrateGenerate` run (via the `lakeorm.sum` manifest hash) or at fingerprint-comparison time (in a future `AssertSchema` / `lakeorm migrate --check`) — whichever happens first, the tooling catches it before production does.

**Why this matters.** ORMs that split the schema across "the struct" and "a separate migration DSL" and "a schema YAML file" inevitably end up with three sources of truth that drift. lake-orm rejects that category error by deriving everything from the struct — the migration .sql is computed, the State-JSON fingerprint is computed, the CREATE TABLE DDL is computed. When the struct changes, there is literally nothing else that can disagree because nothing else is authoritative. The struct IS the contract; every other artefact is a view of it.

**What this accomplishes.** Schema changes become a one-file edit. The PR shows the struct diff; `MigrateGenerate` produces the corresponding .sql; the reviewer checks the struct, the SQL, and the destructive-ops comments in one pass. No "did you remember to update the YAML?" review tax. No "the migration says X but the code expects Y" production incidents. The tooling's job is to catch the divergence before it ships; the convention's job is to prevent the divergence from being introduced at all.

---

## 16. Generated files carry `_gen.go` and are never hand-edited

Any file produced by a generator — mocks, proto stubs, future label-schema expansions, any `go generate`-driven artefact — is named `<concept>_gen.go` (or prefixed per rule 1: `<package>_<concept>_gen.go`). The `_gen.go` suffix is a promise that the file is rewritten on the next `go generate` run; hand edits disappear.

Every generator lives under `tools/<generator-name>/` with its own `main.go` and a `//go:generate go run .` directive pointing at it. Shared codegen utilities live in `tools/codegenutils/` (when that pattern lands — not yet required at v0).

Lake-orm at v0 has one generator in spirit (`MigrateGenerate`), but the files it produces are timestamped goose-format SQL, not `_gen.go` — those are artefacts users edit-review-commit, not regenerated code. When future codegen lands (proto-derived label schemas, Iceberg-catalog introspection, etc.), this rule governs it.

**Why this matters.** A file that's generated and editable sends mixed signals: readers don't know whether fixing a typo will survive the next `go generate`. The `_gen.go` suffix is an unambiguous "don't touch this" marker — editors that see it, linters that see it, and contributors that see it all know the source of truth is the generator, not the file. Without the convention, hand-edits accumulate in files that will eventually be blown away, and either the edits are lost at the next run or the generator is paused indefinitely to preserve them.

**What this accomplishes.** Generated files are safely regeneratable. CI can run `go generate ./... && git diff --exit-code` to catch any divergence between the generator's current output and the committed files. Reviewers ignore `_gen.go` files on PRs and focus on the generator source or the proto/template change that produced them.

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

**Why this matters.** Unit tests cover individual functions; they don't prove the whole system works end-to-end. A release where `go test ./...` passes but `make docker-up && go run ./examples/basic` dies on the first insert has shipped a broken library. The gate is the one check that exercises every layer — driver, dialect, backend, migration authoring, migration execution, typed query — against the actual stack a user will run. If the gate passes, every example in the README works; if the gate fails, the README is lying.

**What this accomplishes.** Releases correlate with working library. Users who follow the README can reproduce every step because the maintainer just did. Regression windows shrink — a bug that breaks the gate catches before the commit that introduced it lands, not after users report it in production. And contributors who add a new feature (a new driver, a new dialect) have a fixed bar to clear: your feature runs in the gate, or it doesn't ship.

---

## See also

- [`CONTRIBUTING.md`](CONTRIBUTING.md) — Design Axioms + PR etiquette.
- [`MIGRATIONS.md`](MIGRATIONS.md) — the three-role split (lakeorm authors, lake-goose runs, humans review).
- [`TYPING.md`](TYPING.md) — CQRS + typed-at-the-edge design contract.
- [`TECH_SPEC.md`](TECH_SPEC.md) — write-path lifecycle + ingest_id threading.
