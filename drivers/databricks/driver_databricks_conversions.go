package databricks

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datalake-go/lake-orm/drivers"
)

// Per-driver conversion helpers for the Databricks native driver.
//
// Each helper returns a drivers.Source — a closure that, when
// invoked by a Convertible method (Collect / First / Stream),
// produces a *sql.Rows plus a cleanup hook that calls rows.Close.
//
// Shape-identical to the DuckDB helpers; both drivers are backed by
// *sql.DB. The helpers exist so callers write one line at the call
// site:
//
//	drv := db.Driver().(*databricks.Driver)
//	users, _ := lakeorm.Query[User](ctx, db, drv.FromSQL("SELECT * FROM users"))

// FromSQL builds a Source that runs sql with args via
// (*sql.DB).QueryContext.
func (d *Driver) FromSQL(sqlStr string, args ...any) drivers.Source {
	return func(ctx context.Context) (any, func(), error) {
		rows, err := d.db.QueryContext(ctx, sqlStr, args...)
		if err != nil {
			return nil, nil, fmt.Errorf("databricks: query: %w", err)
		}
		return rows, func() { _ = rows.Close() }, nil
	}
}

// FromRows builds a Source that hands back an already-opened
// *sql.Rows. Cleanup is the caller's responsibility — the Source
// returns nil cleanup so the Convertible impl doesn't close rows
// the caller still owns.
func (d *Driver) FromRows(rows *sql.Rows) drivers.Source {
	return func(_ context.Context) (any, func(), error) {
		if rows == nil {
			return nil, nil, fmt.Errorf("databricks: FromRows given nil *sql.Rows")
		}
		return rows, nil, nil
	}
}

// FromTable builds a Source that reads a whole table by name.
func (d *Driver) FromTable(name string) drivers.Source {
	return d.FromSQL("SELECT * FROM " + name)
}

// FromRow is the single-row variant of FromSQL.
func (d *Driver) FromRow(sqlStr string, args ...any) drivers.Source {
	return d.FromSQL(sqlStr, args...)
}
