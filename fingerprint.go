package lakeorm

import (
	"github.com/datalake-go/lake-orm/structs"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// SchemaFingerprint returns a stable SHA-256 over the expected
// structs.LakeSchema for v (or reflect.Type(v) if v is a reflect.Type).
// The fingerprint is:
//
//   - deterministic across process invocations
//   - insensitive to field declaration order within a struct (so
//     struct refactors that only reorder fields don't invalidate
//     previously-applied migrations)
//   - sensitive to column names, SQL-level types (reflect.Kind
//     plus the time.Time special case), nullability, pk/mergeKey
//     membership, and the table name
//
// Client.AssertSchema hashes the compiled expectation and compares
// against the catalog's runtime fingerprint; a mismatch surfaces as
// a startup error rather than a silent drift.
func SchemaFingerprint(v any) (string, error) {
	var t reflect.Type
	switch vv := v.(type) {
	case reflect.Type:
		t = vv
	case nil:
		return "", fmt.Errorf("lakeorm.SchemaFingerprint: nil value")
	default:
		t = reflect.TypeOf(v)
	}
	schema, err := structs.ParseSchema(t)
	if err != nil {
		return "", err
	}
	return fingerprintSchema(schema), nil
}

func fingerprintSchema(s *structs.LakeSchema) string {
	// Canonical form: table name + sorted (column, type, nullable,
	// pk, mergeKey) tuples joined by \n. Field index is NOT hashed
	// — a reordering of struct fields should not force a new
	// migration on its own.
	pkCols := map[string]bool{}
	for _, i := range s.PrimaryKeys {
		pkCols[s.Fields[i].Column] = true
	}
	mkCols := map[string]bool{}
	for _, i := range s.MergeKeys {
		mkCols[s.Fields[i].Column] = true
	}

	type row struct {
		column   string
		typeName string
		nullable bool
		pk       bool
		mergeKey bool
	}
	rows := make([]row, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.Ignored {
			continue
		}
		rows = append(rows, row{
			column:   f.Column,
			typeName: fingerprintTypeName(f.Type),
			nullable: f.IsNullable,
			pk:       pkCols[f.Column],
			mergeKey: mkCols[f.Column],
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].column < rows[j].column })

	b := &strings.Builder{}
	fmt.Fprintf(b, "table=%s\n", s.TableName)
	for _, r := range rows {
		fmt.Fprintf(b, "col=%s type=%s null=%t pk=%t mk=%t\n",
			r.column, r.typeName, r.nullable, r.pk, r.mergeKey)
	}

	h := sha256.Sum256([]byte(b.String()))
	return "sha256:" + hex.EncodeToString(h[:])
}

// fingerprintTypeName collapses a reflect.Type to the canonical
// token a catalog would observe. time.Time → "timestamp"; []byte →
// "binary"; other slices → "string" (JSON-in-STRING fallback);
// numeric kinds map to the Spark SQL primitive name so two Go
// int-flavors that render to the same SQL type produce the same
// fingerprint.
func fingerprintTypeName(t reflect.Type) string {
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil {
		return "string"
	}
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int64:
		return "bigint"
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return "int"
	case reflect.Float32:
		return "float"
	case reflect.Float64:
		return "double"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "binary"
		}
		return "string"
	case reflect.Struct:
		if t.PkgPath() == "time" && t.Name() == "Time" {
			return "timestamp"
		}
		return "string"
	default:
		return "string"
	}
}
