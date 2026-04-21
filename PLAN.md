# v0.1.0 pre-launch plan

This is the work list to close before public announcement. Scope: the five repos under `github.com/datalake-go` plus the `caldempsey/spark-connect-go` fork.

Posture: keep datalake-go repos private until they're ready. The fork stays at `datalake-go/spark-connect-go` and remains public — it's the only public-facing surface in this cycle, and it's where `Dataset[T]` / `Collect[T]` live for any outside consumer to try today.

None of the work below is architectural — the shipping design is right. It's cleanup before URLs start getting pasted into Hacker News comments.

## Work buckets

**Typing philosophy** — lock in the design so future contributors don't try to promote `Dataset[T]` into the core. One doc.

**Migration architecture pivot** — scrap the custom `migrate` CLI. Implement `database/sql` in the `datalake-go/spark-connect-go` fork so any `database/sql`-aware tool (goose, sqlc, pgx users, ad-hoc scripts) can target a lakehouse. Fork `pressly/goose` as `datalake-go/goose-spark` with one purely additive change: a dialect plugin that teaches goose how to write its own version table in Spark SQL against our driver.

Three roles, no overlap:

- **goose + Spark Connect driver = migration executor.** Reads `.sql` files, runs them against a Spark Connect endpoint, records applied migrations in goose's version table.
- **lakeorm = migration authoring + model diffing.** Detects that a Go struct (model) has changed and emits a timestamped `.sql` file with the diff, Django-`makemigrations`-style. Loudly comments destructive operations so a reviewer notices them in a PR.
- **Humans = review and approval for destructive changes.** `DROP COLUMN` / `RENAME COLUMN` / type narrowing show up as plain comments in the committed file; they're a PR-review concern, not a runtime gate.

No safety analyzer anywhere. No machine-enforced acknowledgements. The file on disk is the contract the reviewer signed off on. The fork is named `goose-spark` because the point is *adding Spark compatibility to goose* — Spark Connect is the transport that makes it possible, but the user-facing value is "goose now supports Spark."

**Renames** — cosmetic but load-bearing. Renames `dorm` → `lakeorm`, cleans up `datalakeorm` legacy copy.

**Sweeps** — mechanical find-and-replace across docs and code once the rename decisions are pinned.

**Small structural gaps** — `UnderlyingSpark()` baking a driver assumption into the method name; `AssertSchema` stubbed; `Local()` returning an error.

## Work items, in execution order

Ordered by blast radius — highest first so later PRs don't revisit the same files. Each item is one or more PRs (noted per item). Items that touch more than one repo list them explicitly.

### 1. Rename the library: `dorm` → `lakeorm`

Repo: **dorm → lakeorm** (massive — every .go file, every README, every doc).

Module path `github.com/datalake-go/lake-orm` → `github.com/datalake-go/lake-orm`. Package `package dorm` → `package lakeorm` everywhere. Every `dorm.Client`, `dorm.Open`, `dorm.Query[T]` identifier → `lakeorm.Client` / `lakeorm.Open` / `lakeorm.Query[T]`. Local dir `/dorm` → `/lakeorm`. Prose references to "dorm" → "lakeorm" where we mean the library (but "dorm" in a historical context like commit messages stays — that's history).

**User actions on GitHub:** rename repo `datalake-go/dorm` → `datalake-go/lakeorm`.

### 2. Add `database/sql` driver to `datalake-go/spark-connect-go`

Repo: **datalake-go/spark-connect-go** (public, additive PR).

The shipping code today exposes `session.Sql(ctx, query)` returning a `DataFrame`. Add a parallel `database/sql`-compatible surface so any tool that speaks `database/sql` can target a Spark Connect endpoint:

- `driver.Driver` implementation with `Open(dsn)` → opens a SparkSession under the hood.
- `driver.Connector` + `driver.DriverContext` for the modern context-aware construction.
- `driver.Conn` + `driver.ConnPrepareContext` + `driver.ExecerContext` + `driver.QueryerContext`.
- `driver.Stmt` + `driver.StmtExecContext` + `driver.StmtQueryContext`.
- `driver.Tx` — stub that errors on `Begin()` (lakehouse commit semantics live in the table format, not in Spark Connect transactions).
- `sql.Register("spark", &sparkDriver{})` in an `init()` so `sql.Open("spark", "sc://...")` works.

Translation: `ExecContext` → `session.Sql(ctx, interpolated)` (no rows). `QueryContext` → `session.Sql(ctx, interpolated)` → `sql.Rows` wrapping the DataFrame's result batches. Parameter binding maps `driver.Value` to Spark Connect's parameterised-query proto fields.

Why this matters beyond migrations: any `database/sql`-aware tool (goose, sqlc, pgx users, ad-hoc scripts, `go test` harnesses using `sql.DB`) can target a lakehouse without learning Spark Connect's native API. It's strictly more useful than a single-purpose migration wrapper.

Same strict invariant as the typed wrapper: **zero changes to the existing `DataFrame` API**. New files (`spark/sql/driver/driver.go`, etc); upstream diff stays additions-only.

Tests: unit-test the driver shape (dsn parsing, args binding, result wrapping) against a mock SparkSession; integration test against the `datalake-go/k8s` stack.

### 3. Fork `pressly/goose` as `datalake-go/goose-spark`

Repo: **datalake-go/goose-spark** (new, public fork).

Fork `pressly/goose` at the latest tagged release. Add one piece only: a dialect plugin that teaches goose how to write its own version table in Spark SQL against the `database/sql` driver from work item 2.

Concrete additions (per goose v3's dialect plugin shape):

- `internal/dialect/spark.go` — one new file implementing the dialect:
  - `CreateVersionTable` → DDL emitting `CREATE TABLE IF NOT EXISTS <table> (...) USING <iceberg|delta>`.
  - `InsertVersion` / `DeleteVersion` → `INSERT INTO` / `DELETE FROM` against the version table.
  - `ListMigrations` → `SELECT version_id, tstamp, is_applied FROM ... ORDER BY version_id`.
  - `MigrationExists` → `SELECT` with the canonical version_id equality.
- A single switch-case addition in `dialect.go` registering `dialect.Spark`.
- `go.mod` adds `github.com/datalake-go/spark-connect-go` and a single blank import `import _ "github.com/datalake-go/spark-connect-go/spark/sql/driver"` so `sql.Open("spark", dsn)` resolves.

That's it. **Nothing else changes.** No modifications to goose's file parser, execution loop, transaction handling, CLI flags, provider constructor, or public API. Diff against upstream = one new file + one switch-case + one import + one `go.mod` line. Rebases against upstream tags stay mechanical.

Format selection (iceberg vs delta) rides on the DSN: `sc://host:port?format=iceberg`. The driver interprets the parameter; the dialect reads it from the connection. Goose itself stays ignorant.

User action on GitHub: fork `pressly/goose` into the `datalake-go` org as `goose-spark`. Stays public (fork of a public repo).

### 4. Shrink lakeorm migration surface; archive `datalake-go/migrate`

Repo: **lakeorm** (code removal), **migrate** (archive on GitHub).

With goose-spark handling execution, lakeorm's migration responsibilities collapse to model-diffing:

- **Generate goose-format files from struct diffs** — `Client.MigrateGenerate(ctx, dir, structs...) ([]string, error)`. Django-`makemigrations`-style: detect that a tagged struct has changed since the last committed migration, emit a timestamped `.sql` file carrying the ALTER TABLE statements that would bring the catalog into line with the code. Destructive operations (`DROP COLUMN`, `RENAME COLUMN`, type-narrowings) land in the file with a loud `-- DESTRUCTIVE: <op>` informational comment so the reviewer sees them in the PR diff.
- **Assert catalog schema matches compiled structs** — `Client.AssertSchema(ctx, structs ...any)`. Unchanged.

Removed from `Client` (no longer lakeorm's job):

- `Client.MigrateStatus` — users run `goose-spark status` or import goose's provider as a library.
- `Client.MigratePlan` — same.
- `Client.MigrateApply` — same.
- `Client.DamVersion` / `MigrateVersion` — same.

Sentinel errors removed:

- `ErrDamNotFound` / `ErrMigrateNotFound` — no subprocess any more.
- `ErrUnsafeChangesRequireGeneration` — no machine-gated safety; the informational comments do the review-time work instead.

Files deleted from lakeorm:

- `internal/migrations/dam.go` (Runner, ExitMap, exec-seam, error wrappers).
- `internal/migrations/dam_test.go`, `dam_testmain_test.go` (re-exec helper process trick).
- `internal/migrations/migrate/` subpackage: `Classify` + `Verdict` + `AckSlug` fields simplify to a `Destructive bool` flag that drives the `-- DESTRUCTIVE:` comment emission. No rule table versions, no ack correlation, no `-- migrate:ack` comments emitted by the generator.
- `migrate.go` body shrinks to just `MigrateGenerate` + `AssertSchema`.

Repos to archive:

- `datalake-go/migrate` — archive on GitHub with a redirect note in the README pointing at `datalake-go/goose-spark` for execution and `datalake-go/lakeorm` for generation. User action.

**Documentation rewrite** (part of this work item):

- Rename `DORM_MIGRATIONS.md` → `MIGRATIONS.md` in the lakeorm repo (after the `dorm → lakeorm` rename in work item 1).
- Rewrite the entire `MIGRATIONS.md` body around the new architecture. Sections:
  - **Philosophy** — three roles (goose + driver executes; lakeorm authors + diffs; humans review). One line on why goose exists and what problem it solves for Spark teams.
  - **How it works** — lakeorm's Spark backplane runs against the latest Spark Connect; Spark Connect is the default driver at v0.1.0 (future drivers like DuckDB, Arrow Flight, iceberg-go plug in alongside, not through Spark). Because the Spark Connect fork now implements `database/sql`, any Go tool that speaks `database/sql` — including goose — can target a Spark lakehouse. goose-spark is the thin fork that teaches goose the Spark dialect so the version table lands in the right place.
  - **Quickstart** — concrete commands:
    ```bash
    go install github.com/datalake-go/goose-spark/cmd/goose-spark@latest
    goose-spark -dir ./migrations spark "sc://localhost:15002?format=iceberg" up
    ```
  - **Authoring migrations with lakeorm** — `Client.MigrateGenerate(ctx, dir, structs...)` example; how destructive ops appear as `-- DESTRUCTIVE:` comments; reviewer workflow.
  - **Reviewing destructive changes** — the one-paragraph statement of intent: file-on-disk is the contract; review destructive ops in PR; no machine gate.
  - **Pointers** — link to `datalake-go/goose-spark` for executor docs; link to `datalake-go/spark-connect-go`'s driver package for DSN grammar and connection options; link to upstream `pressly/goose` for general goose concepts that carry over.

- Update the lakeorm **README.md** Migrations section (currently references `dam` and subprocess-based invocation): rewrite as a ~8-line overview pointing at `MIGRATIONS.md`. Keep it tight; the README is marketing-shaped, the doc is technical.

- Update **CONTRIBUTING.md** where it references the migration executor (currently says "migrate executes them"): change to "goose-spark executes them" and link to the new doc.

- Remove any references to `ErrDamNotFound`, `ErrMigrateNotFound`, `Client.DamVersion`, `Client.MigrateStatus/Plan/Apply` from docs / examples / code comments.

**Note on Django parity:** the "derive current schema from migration history" capability (Django's actual `makemigrations` does this by reading previous migration files, not the live catalog) is a v1+ feature. At v0.1.0 lakeorm's generator still treats the starting state as empty, so the emitted files are bootstrap-only. For a v0.1.0 user: commit the first file, then author subsequent ALTER TABLE migrations by hand or re-run `MigrateGenerate` and hand-edit to subtract already-applied statements. v1 wires migration-history parsing so regeneration after every struct change emits only the incremental diff — true Django parity. Call this out in `MIGRATIONS.md` so users know the v0.1.0 workflow upfront.

### 5. Rename `UnderlyingSpark()` → `DriverType()` + add `lakeorm.CollectAs[T]` / `lakeorm.StreamAs[T]` helpers

Repo: **lakeorm**.

`UnderlyingSpark()` bakes a driver assumption into the method name; every future driver either needs `UnderlyingDuckDB()` or users rename call sites. `DriverType()` is driver-agnostic, same mechanism, honest about what it does. Matches `sql.DB.Driver()` / `net.Conn` via `syscall.Conn` precedent in stdlib.

`CollectAs[T](ctx, df) ([]T, error)` and `StreamAs[T](ctx, df) iter.Seq2[T, error]` become the one-liners that hide the type-assertion-to-fork-DataFrame from call sites.

Update `examples/joins/main.go` + `teste2e/joins_e2e_test.go` to use `lakeorm.CollectAs[T]` (the one-line form the README promises).

One PR.

### 6. Multi-tag support: accept `spark:"..."` and `lakeorm:"..."` as equivalent aliases

Repo: **lakeorm**.

Current parser reads `spark:"..."` only. Add `lakeorm:"..."` as an equivalent alias. **Not** `dorm:"..."` — that would retain the legacy name post-rename. Conflicting tags (both present on one field) error at parse time with `ErrInvalidTag`.

Canonical example in README + CONTRIBUTING stays `spark:"..."`; the `lakeorm:"..."` alias is a one-line note for users who prefer the library-native name. ~40 lines + tests.

### 7. `datalakeorm` → `dorm` → `lakeorm` legacy copy cleanup

Repos: **lakeorm**, **migrate**, **dashboard**, **k8s**.

A few places still carry "datalakeorm" as legacy copy from before the rename. After the `dorm → lakeorm` rename, some places also carry "dorm" when they mean "the library." Sweep:

- READMEs' "datalakeorm is..." → "lakeorm is..."
- `CONTRIBUTING.md` header "Contributing to datalakeorm" → "Contributing to lakeorm"
- `LICENSE` copyright lines
- `SPONSORS.md` prose
- FAQ copy referencing old names
- Sponsor URLs if they reference `/datalakeorm` or `/dorm` specifically

Doc-only; no code.

### 8. `datalake-go/k8s` → `datalake-go/lake-k8s`

Repo: **k8s → lake-k8s**.

The k8s repo emits travelling artifacts (Docker images, Helm charts). `ghcr.io/datalake-go/lake-k8s/spark-connect:<version>` is self-describing when pulled in isolation; `ghcr.io/datalake-go/k8s/...` is not. The apparent redundancy buys real self-description.

**User actions on GitHub:** rename repo `datalake-go/k8s` → `datalake-go/lake-k8s`.

Sweep references in READMEs, Chart.yaml, values.yaml, skaffold.yaml, docker-compose.

### 9. `.github/FUNDING.yml` verify / add

Repo: **lakeorm**.

Verify the file exists, points at `caldempsey` for GitHub Sponsors, uses the correct Open Collective slug.

### 10. `lakeorm.Local()` quickstart story

Repo: **lakeorm**.

`lakeorm.Local()` returns an error telling users to import the `local` subpackage. README says "one line to start" but the canonical entry point doesn't work. Update the README quickstart to show the explicit import form:

```go
import lakeormlocal "github.com/datalake-go/lake-orm/local"
db, err := lakeormlocal.Open()
```

v1 can implement a registered-driver pattern so `lakeorm.Local()` actually works. Out of scope for v0.

### 11. `AssertSchema` v0 — ship stubbed, README note

Repo: **lakeorm**.

`AssertSchema(ctx, structs ...any)` validates fingerprints but returns `ErrNotImplemented` for populated calls because DESCRIBE TABLE isn't wired. Users wiring this into service startup will get failures in dev.

Going with Option A (per the spec doc's recommendation given constraints): ship stubbed, add a one-line note in the README quickstart that `AssertSchema` is v1.

### 12. Typing philosophy doc (`TYPING.md` in lakeorm + CONTRIBUTING reference)

Repo: **lakeorm**.

Lock in the "DataFrame stays untyped at the core, typing is opt-in at the materialization edge" architecture. ~60 lines. Content: exact wording from the spec doc.

Prevents future contributor churn from someone trying to "improve" the design by promoting `Dataset[T]` into the core.

### 13. Audit typed wrapper on `datalake-go/spark-connect-go` against upstream-additive invariant

Repo: **datalake-go/spark-connect-go** (public).

Single new file `spark/sql/typed.go` adding:

- `DataFrameOf[T any]` typed wrapper
- `As[T any](df DataFrame) DataFrameOf[T]`
- `(d DataFrameOf[T]) DataFrame() DataFrame` — unwrap (inverse of `As`)
- `(d DataFrameOf[T]) Collect(ctx) ([]T, error)`
- `(d DataFrameOf[T]) Stream(ctx) iter.Seq2[T, error]`
- `(d DataFrameOf[T]) First(ctx) (*T, error)`
- Free-function equivalents: `Collect[T]`, `Stream[T]`, `First[T]`

**Hard invariant:** zero changes to the untyped `DataFrame` API. No changing `<-chan T` returns to `iter.Seq2` on existing methods. No changing `Row`. No removing methods, no renaming methods, no adjusting signatures. Diff against upstream `apache/spark-connect-go` master is additions only.

Some of this surface was already added in earlier cycles (via feat/typed-helpers, feat/dataset-methods, feat/sqlas-tableas). This item is about auditing what's there against the strict "additions only" invariant and opening the one canonical clean PR for public announcement.

## Items explicitly not doing

- **Do not rename `datalake-go/dashboard` → `datalake-go/datalake-go-dashboard`.** No travelling artifacts; short name is fine.
- **Do not promote `DataFrameOf[T]` into lakeorm's public API as a wrapped type.** Typed DataFrames live in the fork; lakeorm consumes via `CollectAs[T]` / `StreamAs[T]` / `Query[T]`. `lakeorm.DataFrame` interface stays driver-agnostic (`DriverType()` for escape-hatch access).
- **Do not add `Insert` / `Merge` / `Upsert` / `Delete` to `DataFrame` in the fork.** Those are ORM layer concerns; they live in lakeorm.
- **Do not add typed variants of DataFrame transformations** (no `DataFrameOf[T].Where`, etc). Users drop to `.DataFrame()` and re-type at the edge when ready to collect.
- **Do not convert streaming APIs to channels.** `iter.Seq2[T, error]` is the correct pattern post-Go 1.23. Fork's untyped API stays as-is if upstream is on channels.
- **Do not keep the `dorm:"..."` struct-tag alias.** The library is renamed to `lakeorm`; `dorm:"..."` would be a legacy name with no supporting brand. Only `spark:"..."` and `lakeorm:"..."` are recognised.
- **Do not build a custom migration executor.** That job belongs to goose (via the `goose-spark` fork). `lakeorm` generates files; `goose-spark` applies them.
- **Do not modify goose's core in `goose-spark`.** One new dialect file plus a switch-case addition. Every rebase against upstream stays mechanical.
- **Do not modify the fork's untyped `DataFrame` API when adding the `database/sql` driver.** The driver sits alongside DataFrame (`spark/sql/driver/`), not inside it. Upstream diff stays additions-only.
- **Do not build Lighthouse.** Reserved namespace only.

## GitHub-side actions needed from you

These are repo-level operations I can't do through pushes. GitHub redirects old URLs server-side, so timing is flexible — my pushes to the old URL will resolve through to the renamed repo.

1. **Rename `datalake-go/dorm` → `datalake-go/lakeorm`.** Needed for work item 1; my pushes will keep working against either URL during the window.
2. **Fork `pressly/goose` into `datalake-go/goose-spark`.** Needed for work item 3. Public fork; the additive dialect change lands as a PR against `datalake-go/goose-spark` main.
3. **Archive `datalake-go/migrate`** after work item 4 lands. README gets a one-line redirect note pointing at `datalake-go/goose-spark`.
4. **Rename `datalake-go/k8s` → `datalake-go/lake-k8s`.** Needed for work item 8.

I'll flag the GitHub action on each PR that depends on it.

The fork stays at `datalake-go/spark-connect-go`. No transfer needed.

## Current state (before this plan)

- **Branch `refactor/dam-to-migrate` in dorm** — three file renames staged (`dam.go` → `migrations.go`; `dam_test.go` → `runner_test.go`; `dam_testmain_test.go` → `runner_testmain_test.go`). Content sweep not started. **Superseded by the migration pivot** — these files will be deleted entirely in work item 4, not renamed. Discard this branch.

## Execution sequence

Overall: ~13 PRs across 5 repos.

Dependencies:

- Work item 1 (dorm → lakeorm) is the largest blast and affects every subsequent lakeorm-side item. Do first.
- Work items 2 + 3 (database/sql driver + goose-spark) are independent of the rename and can land in parallel on the fork. They feed into work item 4.
- Work item 4 (shrink lakeorm migration surface) depends on 3 landing so users have somewhere to point their `goose-spark apply` at.
- Work items 5+ are independent cleanups; order is flexible.

Proposed order:

1. **Rename `dorm` → `lakeorm`** in the dorm repo. One large PR. Blocks every subsequent lakeorm-side item.
2. **Add `database/sql` driver** to `datalake-go/spark-connect-go`. Public additive PR. Independent of rename.
3. **Fork `pressly/goose` into `datalake-go/goose-spark`**; add the `spark` dialect. Public PR in the new fork.
4. **Shrink lakeorm migration surface.** Remove Client.MigrateStatus / Plan / Apply / Version; keep MigrateGenerate + AssertSchema. Delete `internal/migrations/dam.go`, `dam_test.go`, `dam_testmain_test.go`. Archive `datalake-go/migrate` on GitHub.
5. `UnderlyingSpark` → `DriverType` + `CollectAs[T]` / `StreamAs[T]` helpers (one lakeorm PR).
6. Multi-tag support — spark + lakeorm aliases (one lakeorm PR).
7. `datalakeorm` / `dorm` legacy copy cleanup (one PR per repo; 3 PRs — lakeorm, dashboard, k8s).
8. k8s → lake-k8s rename + sweeps (one PR).
9. FUNDING.yml verify / add.
10. `lakeorm.Local()` quickstart note.
11. AssertSchema README note.
12. Typing philosophy `TYPING.md`.
13. Audit the typed wrapper on `datalake-go/spark-connect-go` against upstream-additive invariant (likely no-op; the surface is already merged).

After all merged, the ecosystem has:

- `lakeorm` — the typed lakehouse ORM (read path, fast-path write, streaming, CQRS examples, e2e tests).
- `datalake-go/spark-connect-go` — the Spark Connect client with both the typed `Dataset[T]` surface and a `database/sql` driver, both strictly additive to upstream.
- `datalake-go/goose-spark` — a goose fork that speaks Spark Connect via the new driver. `lakeorm` emits goose-format files; users apply them with `goose-spark up ./migrations`.

That's a credible v0.1.0.
