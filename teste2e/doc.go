// Package teste2e is the black-box end-to-end suite for lakeorm.
//
// Where adjacent integration tests (backend/s3/*_test.go,
// dialect/iceberg/*_test.go, …) exercise one package at a time with
// minimal dependencies, teste2e drives the full stack: Go client →
// lakeorm.Open → Driver → Dialect → Backend → Spark Connect → Iceberg →
// SeaweedFS. Each test treats lakeorm.Client as opaque and asserts on
// observable outcomes (rows written, rows read, files staged).
//
// # Running
//
// The suite is build-tag gated (`e2e`) so `go test ./...` skips it by
// default. Run with:
//
//	go test -tags=e2e ./teste2e/...
//
// Each test brings up its own testcontainer stack via testutils —
// MinIO (SeaweedFS-like S3) and optionally a pre-baked
// Spark Connect image. When the Spark Connect testcontainer isn't
// available (the `dorm-spark-connect` image isn't published yet),
// tests that need Spark skip themselves via t.Skip; tests that can
// run against just the Backend + in-memory components still run.
//
// # Why a separate suite
//
//   - Build-tag isolation keeps the default `go test` cycle fast.
//   - Black-box framing prevents tests from leaking into internal
//     package details — if this suite passes, the public contract
//     holds regardless of internal refactors.
//   - One place to look for "does the whole thing actually work?"
package teste2e
