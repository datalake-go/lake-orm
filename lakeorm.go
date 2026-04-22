// Package lakeorm is the Go ORM for data lakes. Typed structs are the
// schema contract; validation happens at the application boundary;
// writes go directly to the target Iceberg or Delta table via Spark
// Connect — no bronze landing zone by default.
//
// Entry points:
//
//	lakeorm.Open(driver, dialect, backend, opts...)
//	lakeorm.Query[T](ctx, db, sql, args...)   // typed read
//	structs.Validate(records)                 // boundary-validate before I/O
//
// Composition is always three pieces: a Driver (transport — how we
// connect and execute), a Dialect (the data-dialect — DDL, DML,
// capabilities, semantics; Iceberg vs Delta), and a Backend (where
// the bytes live). Each is a stable public interface; concrete
// implementations live under drivers/, dialects/, and backends/.
package lakeorm

import (
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
		sem:     semaphore.NewWeighted(int64(cfg.maxInflightIngests)),
	}

	return c, nil
}
