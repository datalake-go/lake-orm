// Package testutils is the shared test harness for lakeorm.
//
// It is a public package — not internal — because both the adjacent
// integration tests (backend/s3/*_test.go, dialect/iceberg/…)
// and the black-box teste2e suite need the same primitives:
// testcontainer-backed MinIO/SeaweedFS and Spark Connect, plus
// Factory-style helpers for generating seed data.
//
// # Containers
//
// Each constructor (MinIO, SparkConnect) returns a running container
// plus an endpoint the test can point its client at. Cleanup is
// registered via t.Cleanup so tests don't leak containers on failure.
// Zero network flag-passing required — containers expose whatever
// ports they need and return the resolved endpoint strings.
//
// # Factory
//
// Factory owns the per-test seed data: unique bucket names, unique
// ingest IDs, deterministic KSUID sequences when tests need
// ordering. One Factory per test; Factory methods are chainable.
//
// # Why testcontainers, not mocks
//
// Mocks of MinIO and Spark Connect diverge from the real servers
// faster than anyone can maintain them. Tests that run against real
// containers catch version-compatibility regressions the moment they
// land upstream; mocks silently pass.
package testutils
