// Package dorm is the Go ORM for data lakes. Typed structs are the
// schema contract; validation happens at the application boundary;
// writes go directly to the target Iceberg or Delta table via Spark
// Connect — no bronze landing zone by default.
//
// Entry points:
//
//	lakeorm.Open(driver, dialect, backend, opts...)
//	lakeorm.Query[T](db)             // typed query builder
//	lakeorm.Validate(records)        // boundary-validate before I/O
//
// Composition is always three pieces: a Driver (transport — how we
// connect and execute), a Dialect (the data-dialect — DDL, DML,
// capabilities, semantics; Iceberg vs Delta), and a Backend (where
// the bytes live). Each is a stable public interface; concrete
// implementations live under driver/, dialect/, and backend/.
package lakeorm

import (
	"context"
	"fmt"

	"golang.org/x/sync/semaphore"
)

// Open composes a Client from a Driver, a Dialect, a Backend, and
// options. All three positional arguments are required.
func Open(driver Driver, dialect Dialect, backend Backend, opts ...ClientOption) (Client, error) {
	if driver == nil {
		return nil, fmt.Errorf("lakeorm.Open: driver is required")
	}
	if dialect == nil {
		return nil, fmt.Errorf("lakeorm.Open: dialect is required")
	}
	if backend == nil {
		return nil, fmt.Errorf("lakeorm.Open: backend is required")
	}

	cfg := newDefaultClientConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	c := &client{
		driver:  driver,
		dialect: dialect,
		backend: backend,
		cfg:     cfg,
		scanner: NewScanner(),
		sem:     semaphore.NewWeighted(int64(cfg.maxInflightIngests)),
	}

	return c, nil
}

// OpenFromConfig loads lakeorm.toml from disk and opens a Client. v0 stub
// — real TOML parsing lands alongside the `cmd/dorm stack` subcommand.
func OpenFromConfig(_ context.Context, _ string) (Client, error) {
	return nil, fmt.Errorf("lakeorm.OpenFromConfig: %w (use lakeorm.Open or lakeorm.Local in v0)", ErrNotImplemented)
}

// Verify runs an end-to-end reachability probe against an opened
// Client. Writes a small probe parquet via the Backend, reads it back
// via the Driver, compares. Returns nil if all three legs (client →
// backend, driver → backend, URI agreement) succeed.
//
// v0 stub — implementation lands in a follow-up commit. The signature
// is stable so callers can wire it into service startup today.
func Verify(_ context.Context, _ Client) error {
	return fmt.Errorf("lakeorm.Verify: %w (planned: probe parquet + round-trip)", ErrNotImplemented)
}
