package migrations

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/datalake-go/lake-orm/structs"
)

// Fingerprint is a stable SHA-256 over the expected structs.LakeSchema
// for a tagged Go type. It's what every generated migration file
// carries in the State-JSON header so lakeorm can tell, on the next
// MigrateGenerate run, whether the current struct matches the prior
// state or has drifted — and only emit a new migration when it has.
//
// Idempotency is the whole point: re-running MigrateGenerate against
// an unchanged struct must be a no-op, and it is because the
// fingerprint comparison short-circuits before any .sql file gets
// written.
//
// Properties:
//
//   - deterministic across process invocations (sorted canonical form)
//   - insensitive to Go-field declaration order (a pure reorder does
//     not force a new migration)
//   - sensitive to column names, SQL-level types, nullability,
//     pk/mergeKey membership, and the resolved table name
func Fingerprint(v any) (string, error) {
	var t reflect.Type
	switch vv := v.(type) {
	case reflect.Type:
		t = vv
	case nil:
		return "", fmt.Errorf("migrations.Fingerprint: nil value")
	default:
		t = reflect.TypeOf(v)
	}
	schema, err := structs.ParseSchema(t)
	if err != nil {
		return "", err
	}
	return fingerprintSchema(schema), nil
}

// fingerprintSchema is Fingerprint's pure-function core: no
// reflect.TypeOf dispatch, just the canonical serialisation + hash.
func fingerprintSchema(s *structs.LakeSchema) string {
	// Canonical form: table name + sorted (column, type, nullable,
	// pk, mergeKey) tuples joined by \n. Field index is NOT hashed
	// so a reorder of struct fields does not force a new migration.
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
