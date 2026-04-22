package goose

import (
	"reflect"
	"time"
)

// Diff computes the semantic changes needed to transform current into
// target. current may be nil, in which case the result is a single
// OpCreateTable change carrying the target schema.
//
// Pure function: no I/O, no driver calls. The caller supplies current
// (typically obtained via a Dialect-aware DESCRIBE TABLE through the
// Driver) and target (built from a struct via whatever boundary
// adapter the host code ships — lakeorm's is migrate_client.go).
// Diff is deterministic and sorts nothing — changes are returned in
// the order: drops, adds in target's field order, then type /
// nullability changes in target's field order.
//
// formatName selects type rendering ("iceberg" or "delta"); the
// difference is cosmetic in v0 (both speak Spark SQL) but kept for
// forward-compatibility with dialect-specific extensions.
func Diff(current, target *Schema, formatName string) []Change {
	if target == nil {
		return nil
	}
	if current == nil {
		return []Change{{
			Op:     OpCreateTable,
			Table:  target.TableName,
			Schema: target,
		}}
	}

	currentByColumn := map[string]Field{}
	for _, f := range current.Fields {
		if f.Ignored {
			continue
		}
		currentByColumn[f.Column] = f
	}
	targetByColumn := map[string]Field{}
	for _, f := range target.Fields {
		if f.Ignored {
			continue
		}
		targetByColumn[f.Column] = f
	}

	var changes []Change

	// Drops — any column in current that target doesn't carry.
	for _, f := range current.Fields {
		if f.Ignored {
			continue
		}
		if _, ok := targetByColumn[f.Column]; !ok {
			changes = append(changes, Change{
				Op:     OpDropColumn,
				Table:  target.TableName,
				Column: f.Column,
			})
		}
	}

	// Adds + type/nullability changes — walk target in declaration
	// order so the generated ALTERs follow the struct's shape.
	for _, f := range target.Fields {
		if f.Ignored {
			continue
		}
		prev, ok := currentByColumn[f.Column]
		if !ok {
			changes = append(changes, Change{
				Op:       OpAddColumn,
				Table:    target.TableName,
				Column:   f.Column,
				Type:     sqlType(f.GoType, formatName),
				Nullable: f.Nullable,
			})
			continue
		}
		// Type change.
		if !sameType(prev.GoType, f.GoType) {
			if isWidening(prev.GoType, f.GoType) {
				changes = append(changes, Change{
					Op:      OpWidenType,
					Table:   target.TableName,
					Column:  f.Column,
					OldType: sqlType(prev.GoType, formatName),
					Type:    sqlType(f.GoType, formatName),
				})
			} else {
				changes = append(changes, Change{
					Op:      OpNarrowType,
					Table:   target.TableName,
					Column:  f.Column,
					OldType: sqlType(prev.GoType, formatName),
					Type:    sqlType(f.GoType, formatName),
				})
			}
		}
		// Nullability change.
		if prev.Nullable != f.Nullable {
			op := OpSetNullable
			if !f.Nullable {
				op = OpSetNotNull
			}
			changes = append(changes, Change{
				Op:       op,
				Table:    target.TableName,
				Column:   f.Column,
				Nullable: f.Nullable,
			})
		}
	}

	return changes
}

func sameType(a, b reflect.Type) bool {
	return normalizeType(a) == normalizeType(b)
}

// normalizeType collapses pointer indirection; the lake tag parser
// records pointer-ness via IsPointer/IsNullable flags, so the
// underlying element type is what Diff compares.
func normalizeType(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func isWidening(oldT, newT reflect.Type) bool {
	oldT = normalizeType(oldT)
	newT = normalizeType(newT)
	if oldT == nil || newT == nil {
		return false
	}
	// Integer widenings: smaller int kind → larger.
	ints := map[reflect.Kind]int{
		reflect.Int8: 8, reflect.Int16: 16, reflect.Int32: 32, reflect.Int64: 64,
		reflect.Int: 64,
	}
	if ow, ook := ints[oldT.Kind()]; ook {
		if nw, nok := ints[newT.Kind()]; nok {
			return nw > ow
		}
	}
	// Float32 → Float64.
	if oldT.Kind() == reflect.Float32 && newT.Kind() == reflect.Float64 {
		return true
	}
	return false
}

// sqlType renders a reflect.Type into the format's Spark SQL type
// name. Iceberg and Delta share the primitive type set; the
// formatName parameter is reserved for future divergence (Iceberg's
// TIMESTAMPTZ vs Delta's TIMESTAMP, generated columns, etc.).
func sqlType(t reflect.Type, _ string) string {
	t = normalizeType(t)
	if t == nil {
		return "STRING"
	}
	switch t.Kind() {
	case reflect.String:
		return "STRING"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int64:
		return "BIGINT"
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return "INT"
	case reflect.Float32:
		return "FLOAT"
	case reflect.Float64:
		return "DOUBLE"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "BINARY"
		}
		return "STRING"
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP"
		}
		return "STRING"
	default:
		return "STRING"
	}
}
