package structs

import (
	"reflect"
	"strings"
	"unicode"
)

// Table-name derivation.
//
// By default the table name for a tagged struct is the snake_case
// pluralisation of the Go type name: User → users, BoundingBox →
// bounding_boxes, Fish → fishes. That covers the common case.
//
// When the Go type name doesn't match the intended schema name —
// legacy naming, a struct that serves a renamed table, one shape
// addressing multiple physical tables via composition — implement
// TableNamer on the struct:
//
//	type LegacyInventoryRecord struct {
//	    ID  string `lake:"id,pk"`
//	    SKU string `lake:"sku"`
//	}
//
//	func (LegacyInventoryRecord) TableName() string { return "inventory" }
//
// ParseSchema consults TableName() before falling back to the
// default derivation. The interface attaches the override to the
// struct's declaration site — grep "func.*TableName" finds every
// override in one pass — and avoids the init-order hazards of a
// package-level registry.

// TableNamer lets a user-defined struct declare its table name
// directly, bypassing the default snake_case-plural derivation.
type TableNamer interface {
	TableName() string
}

// resolveTableName returns the declared table name for a Go type:
// the TableNamer result if the struct implements it, otherwise the
// default derivation.
func resolveTableName(t reflect.Type) string {
	// Honour TableNamer on both value and pointer receivers. New(t)
	// gives us a pointer; .Elem() then dereferences for the
	// value-receiver check.
	if namer, ok := reflect.New(t).Interface().(TableNamer); ok {
		return namer.TableName()
	}
	if namer, ok := reflect.New(t).Elem().Interface().(TableNamer); ok {
		return namer.TableName()
	}
	return defaultTableName(t)
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
