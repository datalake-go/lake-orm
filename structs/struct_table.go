package structs

import (
	"reflect"
	"strings"
	"sync"
	"unicode"
)

// Table-name derivation.
//
// By default the table name for a tagged struct is the snake_case
// pluralisation of the Go type name: User → users, BoundingBox →
// bounding_boxes, Fish → fishes. That's what you get from
// ParseSchema on a fresh type.
//
// Override when the Go name doesn't match the intended schema name:
//
//	structs.Table(&LegacyInventoryRecord{}, "inventory")
//
// Call this once at package init before any Insert / Query / Migrate
// touches the type; the override is process-local, lives in a
// sync.Map so tests can reassign without racing, and invalidates
// the ParseSchema cache for that type so the next parse picks up
// the new name.

// tableOverride maps reflect.Type → overridden table name. Consulted
// by ParseSchema before it falls back to defaultTableName.
var tableOverride sync.Map // reflect.Type -> string

// Table overrides the derived table name for a Go type. Call once
// at package init before any Insert / Query / Migrate touches the
// type.
func Table(model any, name string) {
	t := reflect.TypeOf(model)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	tableOverride.Store(t, name)
	// Invalidate the parse cache so the next ParseSchema call
	// picks up the override — otherwise a cached pre-override
	// schema shadows the new name.
	schemaCache.Delete(t)
}

// defaultTableName returns the snake_case plural form of the Go
// type name: "User" → "users", "BoundingBox" → "bounding_boxes".
func defaultTableName(t reflect.Type) string {
	return pluralize(toSnake(t.Name()))
}

// pluralize applies the minimal English pluralisation rules that
// cover the common Go-type-name cases.
func pluralize(s string) string {
	switch {
	case strings.HasSuffix(s, "s"),
		strings.HasSuffix(s, "x"),
		strings.HasSuffix(s, "z"),
		strings.HasSuffix(s, "ch"),
		strings.HasSuffix(s, "sh"):
		return s + "es"
	case strings.HasSuffix(s, "y"):
		return s[:len(s)-1] + "ies"
	default:
		return s + "s"
	}
}

// toSnake converts CamelCase to snake_case. "BoundingBox" → "bounding_box".
func toSnake(s string) string {
	var b strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if i > 0 && unicode.IsUpper(r) {
			prev := runes[i-1]
			if unicode.IsLower(prev) || (i+1 < len(runes) && unicode.IsLower(runes[i+1])) {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}
