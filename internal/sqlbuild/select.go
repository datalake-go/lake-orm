// Package sqlbuild renders the generic SELECT shape shared by every
// Dialect's read path. The shape is
//
//	SELECT <cols> FROM <table>
//	[WHERE <expr>] [ORDER BY <col> [DESC], ...] [LIMIT <n>] [OFFSET <n>]
//
// orthogonal to dialect-specific concerns like table-name qualification
// or partition handling — each Dialect is expected to resolve the
// table name upstream and hand a ready-to-interpolate value in.
package sqlbuild

import (
	"fmt"
	"strings"
)

// OrderSpec is one ORDER BY clause. Kept as a local value type to
// spare callers an import of the top-level dorm package for a
// two-field struct.
type OrderSpec struct {
	Column string
	Desc   bool
}

// Select carries every piece of the shape. Zero values mean "omit
// this clause" — empty Columns implies SELECT *, empty Where skips
// the WHERE clause, zero Limit/Offset skip those clauses.
type Select struct {
	Columns []string
	Table   string
	Where   string
	Args    []any
	OrderBy []OrderSpec
	Limit   int
	Offset  int
}

// Build renders s into a SQL string and returns the args slice
// unchanged for downstream parameter binding. Args are passed
// through rather than interpolated here because placeholder
// semantics are a Driver concern — Spark Connect quotes inline,
// other drivers might bind through gRPC parameters natively.
func (s Select) Build() (string, []any) {
	cols := s.Columns
	if len(cols) == 0 {
		cols = []string{"*"}
	}
	var b strings.Builder
	b.WriteString("SELECT ")
	b.WriteString(strings.Join(cols, ", "))
	b.WriteString(" FROM ")
	b.WriteString(s.Table)
	if s.Where != "" {
		b.WriteString(" WHERE ")
		b.WriteString(s.Where)
	}
	for i, o := range s.OrderBy {
		if i == 0 {
			b.WriteString(" ORDER BY ")
		} else {
			b.WriteString(", ")
		}
		b.WriteString(o.Column)
		if o.Desc {
			b.WriteString(" DESC")
		}
	}
	if s.Limit > 0 {
		fmt.Fprintf(&b, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		fmt.Fprintf(&b, " OFFSET %d", s.Offset)
	}
	return b.String(), s.Args
}
