# Contributing to lakeorm

Thanks for considering a contribution. This document covers the practicalities; the shorter version is that patches are welcome and the project tries to be a low-friction place to land them.

## A note on contributions and licensing

All contributions are accepted under the MIT license and will remain MIT-licensed in perpetuity. We do not ask contributors to sign a CLA that would permit relicensing. This is a deliberate commitment: contributors can trust that their code will never be moved to a restrictive license.

## What the project ships

- The Go library at `github.com/datalake-go/lake-orm` (this repo).
- The sibling Helm chart at [`github.com/datalake-go/lake-k8s`](https://github.com/datalake-go/lake-k8s).
- The embeddable metrics dashboard at [`github.com/datalake-go/lake-dashboard`](https://github.com/datalake-go/lake-dashboard) (River-style `http.Handler`).
- The Spark Connect Go fork at [`github.com/datalake-go/spark-connect-go`](https://github.com/datalake-go/spark-connect-go) — carries fork-only improvements (typed `DataFrameOf[T]`, `database/sql` driver, configurable gRPC transport) that are on a slower drip into upstream.

Migration execution lives in [`github.com/datalake-go/lake-goose`](https://github.com/datalake-go/lake-goose), a pressly/goose fork adding `iceberg` and `delta` dialects. lakeorm authors migration files (`MigrateGenerate`); lake-goose applies them against the `database/sql` driver in `spark-connect-go`. If your contribution is about execution, ledger bookkeeping, or goose dialect behaviour, it belongs in lake-goose rather than here.

## Getting the stack running locally

```bash
git clone https://github.com/datalake-go/lake-orm.git
cd lakeorm
make docker-up           # SeaweedFS + Spark Connect
go run ./examples/basic
```

First boot takes 60-90 seconds while Spark resolves JARs. Subsequent starts are immediate.

## Running tests

```bash
make test                # unit tests, no external deps
make integration-test    # adjacent per-package tests via testcontainers (-tags=integration)
make e2e-test            # black-box client tests against docker-compose (-tags=e2e)
```

Unit tests live next to the code they test. Integration tests each bring up their own testcontainer, so CI runners need only Docker. E2E tests live in `teste2e/` and drive the full stack via the public API.

## Commit and PR conventions

Commits and PR titles use `<type>/<issue-number>: <description>`, where type is one of `feat`, `fix`, `refactor`, `docs`, `chore`. Examples:

- `feat/17: migrate subpackage + lakeorm.SchemaFingerprint`
- `fix/8: fall back to archive.apache.org when dlcdn 404s`
- `refactor/12: drop local Prometheus + Grafana services`
- `docs/16: scope MIGRATIONS.md to lakeorm's side of the split`

PR bodies follow a leetcode shape: Problem, Intuition, Implementation, Tests. The goal is a reader who hasn't seen the branch can still reconstruct why each change exists.

Issue bodies follow a Rob-Pike-succinct shape: What, Why, Proposal. Two or three paragraphs total is often enough.

## Code style

Read [`CONVENTIONS.md`](CONVENTIONS.md) first — it codifies the file-naming rule, the verb taxonomy (Find/Fetch/Is/Batch/Compute/Classify/ErrX ↔ NewErrX ↔ IsErrX), the package-doc rule, the public-surface-minimisation principle, and the end-to-end release gate. Every rule there is applied uniformly across the tree with no exceptions. If your PR introduces a pattern that diverges, either the PR follows the convention or the convention changes — one consistency, not many.

- `gofmt` for formatting, `go vet` and `staticcheck` for basic hygiene.
- Godoc comments on exported types, functions, and methods. No ASCII block separators (`// ----`, `// ====`) — godoc renders multi-paragraph comments clearly without them.
- Comments explain the *why*, not the *what*. Well-named identifiers already explain the what.
- No references to internal planning docs in shipped code. If a piece of context is load-bearing enough that a future reader needs it, pull it into the comment; if it isn't, leave it out.

## Design axioms

The project has a few long-running commitments that influence what we accept and reject:

- **Vendorless, reproducible, you-run-it.** Every piece of the stack is open-source and self-hostable. Pull requests that add a dependency on a vendor-hosted service go back for a rethink.
- **No telemetry.** The library makes network calls only to endpoints the user configured. No opt-in, no opt-out, no "anonymous aggregate usage."
- **CQRS-style reads.** Writes bind `spark:"..."`-tagged entities; reads shaped by joins and aggregates bind `spark:"..."`-tagged result structs scanned via `lakeorm.CollectAs[T]`. Unified ORM mappings collapse under joins and we do not try to make them work.
- **Typing is opt-in at the materialization edge.** `DataFrame` stays untyped at the core; `DataFrameOf[T]` in the fork and `lakeorm.CollectAs[T]` / `StreamAs[T]` / `Query[T]` in lakeorm are the typed seams. PRs that promote typed DataFrames into the core interface go back for a rethink — see [`TYPING.md`](TYPING.md) for the full design contract.
- **No "enterprise edition" scaffolding.** No feature flags, no license checks, no `//go:build enterprise` tags. See `context/MONETIZATION.md` for the full posture.

## Reporting issues

Short, Rob-Pike-shaped issues are easier to triage than long ones:

- **What:** describe the observable thing.
- **Why:** describe why it matters or why the current behavior is wrong.
- **Proposal:** sketch a direction, even a partial one.

For security-sensitive issues, contact callumdempseyleach@gmail.com directly rather than filing a public issue.

## Code of conduct

Be civil. Disagreements over design are fine; personal attacks are not. Maintainers reserve the right to close PRs or issues that violate this in spirit.

## Licence

MIT. See [`LICENSE`](LICENSE).
