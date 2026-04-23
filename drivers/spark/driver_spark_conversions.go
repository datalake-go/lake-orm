package spark

import (
	"context"
	"fmt"

	scsql "github.com/datalake-go/spark-connect-go/spark/sql"

	"github.com/datalake-go/lake-orm/drivers"
)

// Per-driver conversion helpers for the Spark driver.
//
// Each helper returns a drivers.Source — a closure that, when
// invoked by a Convertible method (Collect / First / Stream),
// produces the Spark-native sparksql.DataFrame plus a cleanup hook
// responsible for returning any pool-borrowed session.
//
// The helpers exist so callers write one line at the call site:
//
//	drv := db.Driver().(*spark.Driver)
//	users, _ := lakeorm.Query[User](ctx, db, drv.FromSQL("SELECT * FROM users"))
//
// Anything the helpers don't cover can be expressed as a bare
// closure — the driver doesn't care where the DataFrame comes from,
// only that the cleanup hook releases whatever session produced it.

// FromSQL builds a Source that borrows a session, runs sql (with
// ? placeholders rendered from args), and hands back the resulting
// DataFrame. The cleanup hook returns the session to the pool.
func (d *Driver) FromSQL(sql string, args ...any) drivers.Source {
	return func(ctx context.Context) (any, func(), error) {
		s, err := d.pool.Borrow(ctx)
		if err != nil {
			return nil, nil, err
		}
		rendered, err := renderSQL(sql, args)
		if err != nil {
			d.pool.Return(s)
			return nil, nil, err
		}
		df, err := s.Sql(ctx, rendered)
		if err != nil {
			d.pool.Return(s)
			return nil, nil, translateClusterError(err)
		}
		return df, func() { d.pool.Return(s) }, nil
	}
}

// FromDataFrame builds a Source that hands back an already-created
// DataFrame. The caller owns the session that produced it, so the
// cleanup hook is nil — nothing for the driver to release.
//
// Used when the caller has chained Spark operations on a DataFrame
// (GroupBy / Agg / Join / Window functions) and wants the typed
// decode path over the result. Paired with Session() for the
// acquire side.
func (d *Driver) FromDataFrame(df scsql.DataFrame) drivers.Source {
	return func(_ context.Context) (any, func(), error) {
		if df == nil {
			return nil, nil, fmt.Errorf("spark: FromDataFrame given nil DataFrame")
		}
		return df, nil, nil
	}
}

// FromTable builds a Source that reads a whole table by name. Equivalent
// to FromSQL("SELECT * FROM <name>") but goes through
// SparkSession.Table which lets Spark push column pruning and
// predicate pushdown further.
func (d *Driver) FromTable(name string) drivers.Source {
	return func(ctx context.Context) (any, func(), error) {
		s, err := d.pool.Borrow(ctx)
		if err != nil {
			return nil, nil, err
		}
		df, err := s.Table(name)
		if err != nil {
			d.pool.Return(s)
			return nil, nil, translateClusterError(err)
		}
		return df, func() { d.pool.Return(s) }, nil
	}
}

// FromRow is the single-row variant of FromSQL. Structurally
// identical — the "single row" semantic is enforced by the
// Convertible.First call at the other end, which stops iterating
// after the first row and returns errors.ErrNoRows when the source
// yielded zero rows. Kept as a named helper because
// lakeorm.QueryFirst[T](ctx, db, drv.FromRow("...")) reads more
// clearly than reusing FromSQL at a First site.
func (d *Driver) FromRow(sql string, args ...any) drivers.Source {
	return d.FromSQL(sql, args...)
}

// Session borrows a session from the pool and returns it alongside
// a release hook the caller must invoke (typically via defer) when
// done with the session. The escape hatch for callers that need to
// chain native DataFrame operations (GroupBy, Agg, Join, Window)
// before handing the result to FromDataFrame for typed decode.
//
//	sess, release, err := drv.Session(ctx)
//	if err != nil { return err }
//	defer release()
//	df, err := sess.Sql(ctx, "SELECT ...")
//	agg := df.GroupBy("country").Agg(...)
//	out, err := lakeorm.Query[CountryAgg](ctx, db, drv.FromDataFrame(agg))
func (d *Driver) Session(ctx context.Context) (scsql.SparkSession, func(), error) {
	s, err := d.pool.Borrow(ctx)
	if err != nil {
		return nil, nil, err
	}
	return s, func() { d.pool.Return(s) }, nil
}
