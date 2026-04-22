package lakeorm

import (
	"context"
)

// QueryBuilder is the dynamic (non-generic) query entry point for cases
// where the result type isn't known at compile time. Prefer the
// top-level Query[T] generic when possible.
type QueryBuilder interface {
	From(table string) QueryBuilder
	Where(sql string, args ...any) QueryBuilder
	OrderBy(col string, desc bool) QueryBuilder
	Limit(n int) QueryBuilder
	Offset(n int) QueryBuilder
	Select(cols ...string) QueryBuilder

	Collect(ctx context.Context) ([][]any, error)
	DataFrame(ctx context.Context) (DataFrame, error)
}
