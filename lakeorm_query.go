package lakeorm

import (
	"context"
	"iter"

	"github.com/datalake-go/lake-orm/internal/sqlbuild"
)

// Query runs a SQL statement against db and materialises every row
// into []T. One-shot ergonomic wrapper over db.DataFrame +
// CollectAs[T] — equivalent to:
//
//	df, err := db.DataFrame(ctx, sql, args...)
//	if err != nil { return nil, err }
//	return CollectAs[T](ctx, df)
//
// but compressed to a single call. Use when the result shape is
// known at compile time — the `spark:"..."` tags on T bind result
// columns to fields.
//
// Query is a top-level convenience, not a typed-query-builder
// abstraction. Filtering, ordering, projection, and joins belong in
// the SQL string, not in chainable Where/OrderBy/Select methods.
// Typed DataFrame transformations are deliberately absent — see
// TYPING.md for the design contract.
func Query[T any](ctx context.Context, db Client, sql string, args ...any) ([]T, error) {
	df, err := db.DataFrame(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return CollectAs[T](ctx, df)
}

// QueryStream runs a SQL statement and yields T values one at a
// time with constant memory. Rangeable via Go 1.23's iter.Seq2:
//
//	for row, err := range lakeorm.QueryStream[User](ctx, db,
//	    "SELECT * FROM users WHERE country = ?", "UK") {
//	    if err != nil { break }
//	    // use row
//	}
//
// Schema mismatch (a field in T that the projection doesn't contain)
// surfaces through the iterator's error channel on the first row.
func QueryStream[T any](ctx context.Context, db Client, sql string, args ...any) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		df, err := db.DataFrame(ctx, sql, args...)
		if err != nil {
			yield(zero, err)
			return
		}
		for row, rerr := range StreamAs[T](ctx, df) {
			if !yield(row, rerr) {
				return
			}
		}
	}
}

// QueryFirst runs a SQL statement and returns the first matching
// row decoded as T. Returns lkerrors.ErrNoRows if the result is empty.
// Ergonomic wrapper over db.DataFrame + FirstAs[T].
func QueryFirst[T any](ctx context.Context, db Client, sql string, args ...any) (*T, error) {
	df, err := db.DataFrame(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return FirstAs[T](ctx, df)
}

// --- dynamic (non-generic) query builder ---------------------------
//
// dynamicQuery backs Client.Query for cases where the result type
// isn't known at compile time — typical of code-generated consumers
// or reflection-heavy DSLs. The typed path (Query[T] / QueryStream[T]
// / QueryFirst[T]) is preferred when T is known.

type dynamicQuery struct {
	client  *client
	ctx     context.Context
	table   string
	where   whereClause
	orderBy []OrderSpec
	columns []string
	limit   int
	offset  int
}

type whereClause struct {
	sql  string
	args []any
}

func (q *dynamicQuery) From(table string) QueryBuilder { q.table = table; return q }

func (q *dynamicQuery) Select(cols ...string) QueryBuilder {
	q.columns = cols
	return q
}

func (q *dynamicQuery) Where(sqlStr string, args ...any) QueryBuilder {
	q.where = whereClause{sql: sqlStr, args: args}
	return q
}

func (q *dynamicQuery) OrderBy(col string, desc bool) QueryBuilder {
	q.orderBy = append(q.orderBy, OrderSpec{Column: col, Desc: desc})
	return q
}

func (q *dynamicQuery) Limit(n int) QueryBuilder  { q.limit = n; return q }
func (q *dynamicQuery) Offset(n int) QueryBuilder { q.offset = n; return q }

func (q *dynamicQuery) Collect(ctx context.Context) ([][]any, error) {
	df, err := q.DataFrame(ctx)
	if err != nil {
		return nil, err
	}
	return df.Collect(ctx)
}

func (q *dynamicQuery) DataFrame(ctx context.Context) (DataFrame, error) {
	order := make([]sqlbuild.OrderSpec, 0, len(q.orderBy))
	for _, o := range q.orderBy {
		order = append(order, sqlbuild.OrderSpec{Column: o.Column, Desc: o.Desc})
	}
	sql, args := sqlbuild.Select{
		Columns: q.columns,
		Table:   q.table,
		Where:   q.where.sql,
		Args:    q.where.args,
		OrderBy: order,
		Limit:   q.limit,
		Offset:  q.offset,
	}.Build()
	return q.client.driver.DataFrame(ctx, sql, args...)
}
