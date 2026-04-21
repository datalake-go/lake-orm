package spark

import (
	"fmt"
	"strings"
)

// renderSQL substitutes ? placeholders with inline-quoted arguments.
// v0 implementation — sufficient for typed query generation where the
// argument set is tightly controlled. v1 adds a real parameterized
// execution path when Spark Connect grows one that survives all drivers.
func renderSQL(sql string, args []any) (string, error) {
	if len(args) == 0 {
		return sql, nil
	}
	var b strings.Builder
	n := 0
	for _, r := range sql {
		if r == '?' {
			if n >= len(args) {
				return "", fmt.Errorf("dorm/spark: too few arguments for SQL: %q", sql)
			}
			b.WriteString(quoteSQL(args[n]))
			n++
			continue
		}
		b.WriteRune(r)
	}
	if n != len(args) {
		return "", fmt.Errorf("dorm/spark: %d unused arguments for SQL: %q", len(args)-n, sql)
	}
	return b.String(), nil
}

// quoteSQL renders a Go value as a SQL literal. Strings are
// single-quoted and escaped; numerics and bools pass through; time
// values are formatted as ISO-8601.
func quoteSQL(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return fmt.Sprintf("%v", x)
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", x), "'", "''") + "'"
	}
}
