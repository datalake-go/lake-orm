package duckdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datalake-go/lake-orm/drivers"
)

// Per-driver conversion helpers for the DuckDB driver.
//
// Each helper returns a drivers.Source — a closure that, when
// invoked by a Convertible method (Collect / First / Stream),
// produces a *sql.Rows plus a cleanup hook that calls rows.Close.
//
// The helpers exist so callers write one line at the call site:
//
//	drv := db.Driver().(*duckdb.Driver)
//	users, _ := lakeorm.Query[User](ctx, db, drv.FromSQL("SELECT * FROM users"))
//
// Anything the helpers don't cover can be expressed as a bare
// closure — the driver doesn't care where the *sql.Rows comes from.

// FromSQL builds a Source that runs sql with args via
// (*sql.DB).QueryContext and hands back the resulting *sql.Rows.
// The cleanup hook calls rows.Close.
func (d *Driver) FromSQL(sqlStr string, args ...any) drivers.Source {
	return func(ctx context.Context) (any, func(), error) {
		rows, err := d.db.QueryContext(ctx, sqlStr, args...)
		if err != nil {
			return nil, nil, fmt.Errorf("duckdb: query: %w", err)
		}
		return rows, func() { _ = rows.Close() }, nil
	}
}

// FromRows builds a Source that hands back an already-opened
// *sql.Rows. Use when the caller has run QueryContext themselves
// and wants the typed decode path over the result. Cleanup is the
// caller's responsibility — the Source returns nil cleanup so the
// Convertible impl doesn't close rows the caller still owns.
func (d *Driver) FromRows(rows *sql.Rows) drivers.Source {
	return func(_ context.Context) (any, func(), error) {
		if rows == nil {
			return nil, nil, fmt.Errorf("duckdb: FromRows given nil *sql.Rows")
		}
		return rows, nil, nil
	}
}

// FromTable builds a Source that reads a whole table by name.
// Equivalent to FromSQL("SELECT * FROM <name>"); kept as a named
// helper for readability.
func (d *Driver) FromTable(name string) drivers.Source {
	return d.FromSQL("SELECT * FROM " + name)
}

// FromRow is the single-row variant of FromSQL. Structurally
// identical — the "single row" semantic is enforced by the
// Convertible.First call at the other end, which stops iterating
// after the first row and returns errors.ErrNoRows when the source
// yielded zero rows.
func (d *Driver) FromRow(sqlStr string, args ...any) drivers.Source {
	return d.FromSQL(sqlStr, args...)
}
