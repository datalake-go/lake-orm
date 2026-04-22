package migrations

import "strings"

// Migration filename slugification.
//
// Every emitted migration file is named
//
//	<YYYYMMDDHHMMSS>_<slug>.sql
//
// The slug is the target table name, lowercased, with every non-
// alphanumeric rune collapsed to a single underscore and leading /
// trailing underscores trimmed. This keeps the filename
// grep-friendly and filesystem-portable without depending on the
// caller's table-naming conventions.

// SlugifyTable normalises a table name for use in a migration
// filename.
func SlugifyTable(name string) string {
	var b strings.Builder
	lastUnderscore := true
	for _, r := range strings.ToLower(name) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}
