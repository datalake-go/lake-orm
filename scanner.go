package lakeorm

import (
	"github.com/datalake-go/lake-orm/structs"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jmoiron/sqlx/reflectx"
)

// scannerType caches the reflect.Type for the sql.Scanner interface.
var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// Scanner provides reflection-based row scanning into Go structs via
// the lake-orm tag trio (`lake`, `lakeorm`, `spark`). It is the Driver-
// agnostic counterpart of what every Driver's underlying protocol
// produces (Spark Connect Rows, DuckDB vectors, future Arrow Flight
// batches) — each Driver converts its native rows into lakeorm.Row
// and the Scanner takes over.
//
// Two design decisions worth calling out:
//
//   - sqlx/reflectx for field resolution (embedded structs, dot-notation,
//     pointer-to-struct auto-init). Rolling our own would diverge from
//     every other sqlx-using Go service on edge cases.
//   - scannerTarget (defined below) for nullable custom types that
//     implement sql.Scanner — SortableID, Location, etc. The wrapper
//     tracks NULL separately from the underlying Scan so pointer
//     fields can be set to nil rather than zero-valued.
//
// sqlx/reflectx binds one tag key per mapper, so we carry three in
// priority order (lake > lakeorm > spark) and consult them in turn
// when resolving a column. Same precedence as tag.go's effectiveTag.
type Scanner struct {
	mappers []*reflectx.Mapper
}

// tagKeys lists the accepted tag names in priority order. Kept aligned
// with effectiveTag in tag.go; any change there needs a matching change
// here so scanning and schema parsing don't diverge.
var tagKeys = []string{"lake", "lakeorm", "spark"}

// NewScanner creates a Scanner backed by sqlx's reflectx field mapper.
// One mapper per accepted tag key — lookups walk them in priority order.
func NewScanner() *Scanner {
	mappers := make([]*reflectx.Mapper, len(tagKeys))
	for i, k := range tagKeys {
		mappers[i] = reflectx.NewMapperFunc(k, strings.ToLower)
	}
	return &Scanner{mappers: mappers}
}

// ScanRow scans a single Row produced by a Driver into dest (a *T).
// schema is used only for column-intent introspection; the actual
// mapping is by column name via the reflectx mapper.
func (s *Scanner) ScanRow(row Row, dest any, _ *structs.LakeSchema) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("lakeorm: scan dest must be a pointer to struct")
	}
	destElem := destValue.Elem()
	if destElem.Kind() != reflect.Struct {
		return fmt.Errorf("lakeorm: scan dest must be a pointer to struct")
	}

	columns := row.Columns()
	values := row.Values()
	if len(columns) != len(values) {
		return fmt.Errorf("lakeorm: column/value length mismatch (%d/%d)", len(columns), len(values))
	}

	mappings := s.buildColumnMappings(destElem.Type(), columns)

	for i, col := range columns {
		m := mappings[i]
		if m == nil {
			continue
		}
		field := reflectx.FieldByIndexes(destElem, m.index)
		if err := assignRowValue(field, values[i]); err != nil {
			return fmt.Errorf("lakeorm: assign column %s: %w", col, err)
		}
	}
	return nil
}

// ScanRows scans sql.Rows into a slice of struct pointers. Kept from
// the production port so existing database/sql-style callers have a
// path when they use Driver.Exec + database/sql behind the scenes.
// destSlicePtr must be *[]*T.
func (s *Scanner) ScanRows(rows *sql.Rows, destSlicePtr any) error {
	sliceValue := reflect.ValueOf(destSlicePtr)
	if sliceValue.Kind() != reflect.Ptr {
		return fmt.Errorf("destSlicePtr must be a pointer to slice")
	}
	sliceElem := sliceValue.Elem()
	if sliceElem.Kind() != reflect.Slice {
		return fmt.Errorf("destSlicePtr must be a pointer to slice")
	}
	elemType := sliceElem.Type().Elem()
	if elemType.Kind() != reflect.Ptr {
		return fmt.Errorf("slice element must be pointer type")
	}
	structType := elemType.Elem()
	if structType.Kind() != reflect.Struct {
		return fmt.Errorf("slice element must be pointer to struct")
	}

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns: %w", err)
	}
	mappings := s.buildColumnMappings(structType, columns)

	for rows.Next() {
		newStruct := reflect.New(structType)
		if err := s.scanIntoStruct(rows, columns, mappings, newStruct); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		sliceElem = reflect.Append(sliceElem, newStruct)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	sliceValue.Elem().Set(sliceElem)
	return nil
}

// columnMapping resolves a result-set column position to a struct
// field, pre-computed once per Scan call so the inner loop doesn't
// re-walk the sqlx TypeMap per row.
type columnMapping struct {
	index     []int
	fieldType reflect.Type
}

func (s *Scanner) buildColumnMappings(structType reflect.Type, columns []string) []*columnMapping {
	typeMaps := make([]*reflectx.StructMap, len(s.mappers))
	for i, m := range s.mappers {
		typeMaps[i] = m.TypeMap(structType)
	}
	mappings := make([]*columnMapping, len(columns))

	for i, col := range columns {
		colLower := strings.ToLower(col)
		// Walk mappers in priority order. First hit wins — matches
		// effectiveTag's precedence so a field tagged with `lake`
		// and `spark` is resolved through the `lake` name.
		for _, tm := range typeMaps {
			fi, ok := tm.Names[colLower]
			if !ok {
				continue
			}
			if isSkipped(fi.Field.Tag) {
				break
			}
			mappings[i] = &columnMapping{
				index:     fi.Index,
				fieldType: fi.Field.Type,
			}
			break
		}
	}

	return mappings
}

// isSkipped reports whether a field's tag carries a `-` sentinel under
// any accepted tag key. Checked across the trio so a single `-` on any
// alias consistently excludes the field.
func isSkipped(tag reflect.StructTag) bool {
	for _, k := range tagKeys {
		if tag.Get(k) == "-" {
			return true
		}
	}
	return false
}

// scanIntoStruct is the sql.Rows path used by Driver implementations
// that plumb through database/sql. The Driver-native path (Row → any)
// lives in ScanRow above; both share the column-mapping + assignment
// helpers.
func (s *Scanner) scanIntoStruct(rows *sql.Rows, columns []string, mappings []*columnMapping, dest reflect.Value) error {
	scanTargets := make([]any, len(columns))
	for i, m := range mappings {
		if m != nil {
			scanTargets[i] = createScanTarget(m.fieldType)
		} else {
			scanTargets[i] = new(any)
		}
	}

	if err := rows.Scan(scanTargets...); err != nil {
		return err
	}

	structVal := reflect.Indirect(dest)
	for i, m := range mappings {
		if m == nil {
			continue
		}
		field := reflectx.FieldByIndexes(structVal, m.index)
		if err := assignValue(field, scanTargets[i]); err != nil {
			return fmt.Errorf("assign column %s: %w", columns[i], err)
		}
	}

	return nil
}

// scannerTarget wraps an sql.Scanner so the scanner can detect types
// that handle their own scanning (e.g. SortableID) separately from the
// standard sql.Null* scan targets. Getting NULL-vs-zero semantics
// right on pointer-to-custom-scannable fields only surfaces in
// production — scannerTarget carries a valid bit independent of the
// underlying Scan so the caller can leave a pointer field nil
// instead of zero-valued.
type scannerTarget struct {
	dest   sql.Scanner
	valid  bool
	forPtr bool
}

// Scan implements sql.Scanner so database/sql calls this during rows.Scan.
func (s *scannerTarget) Scan(src any) error {
	if src == nil {
		s.valid = false
		if !s.forPtr {
			return s.dest.Scan(src)
		}
		// Pointer field: skip scanning — the caller sets the pointer to nil.
		return nil
	}
	s.valid = true
	return s.dest.Scan(src)
}

// createScanTarget returns an appropriate scan target for the given
// Go type. Resolution order:
//  1. sql.Scanner types → scannerTarget (SortableID, Location, etc.)
//  2. Primitive types → sql.Null* wrappers
//  3. Slices → NullString (arrays come back as JSON strings)
//  4. Structs → NullTime for time.Time, NullString for unknown (JSON)
func createScanTarget(fieldType reflect.Type) any {
	isPtr := fieldType.Kind() == reflect.Ptr
	actualType := fieldType
	if isPtr {
		actualType = fieldType.Elem()
	}

	ptrToActual := reflect.PointerTo(actualType)
	if ptrToActual.Implements(scannerType) {
		dest := reflect.New(actualType).Interface().(sql.Scanner)
		return &scannerTarget{dest: dest, forPtr: isPtr}
	}

	switch actualType.Kind() {
	case reflect.String:
		return new(sql.NullString)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return new(sql.NullInt64)
	case reflect.Float32, reflect.Float64:
		return new(sql.NullFloat64)
	case reflect.Bool:
		return new(sql.NullBool)
	case reflect.Slice:
		return new(sql.NullString)
	case reflect.Struct:
		if actualType.String() == "time.Time" {
			return new(sql.NullTime)
		}
		return new(sql.NullString)
	default:
		return new(sql.NullString)
	}
}

// assignValue dispatches a populated scan target (one of the
// sql.Null* types or a scannerTarget wrapper) onto its destination
// struct field. Pointer-vs-value handling lives here rather than in
// each assign* helper so the per-type helpers stay focused on
// conversion.
func assignValue(field reflect.Value, target any) error {
	fieldType := field.Type()
	isPtr := fieldType.Kind() == reflect.Ptr

	actualType := fieldType
	if isPtr {
		actualType = fieldType.Elem()
	}

	if st, ok := target.(*scannerTarget); ok {
		return assignScannerValue(field, st, actualType, isPtr)
	}

	switch v := target.(type) {
	case *sql.NullString:
		if !v.Valid {
			if isPtr {
				field.Set(reflect.Zero(fieldType))
			}
			return nil
		}
		return assignStringValue(field, v.String, actualType, isPtr)
	case *sql.NullInt64:
		if !v.Valid {
			if isPtr {
				field.Set(reflect.Zero(fieldType))
			}
			return nil
		}
		return assignIntValue(field, v.Int64, actualType, isPtr)
	case *sql.NullFloat64:
		if !v.Valid {
			if isPtr {
				field.Set(reflect.Zero(fieldType))
			}
			return nil
		}
		return assignFloatValue(field, v.Float64, actualType, isPtr)
	case *sql.NullBool:
		if !v.Valid {
			if isPtr {
				field.Set(reflect.Zero(fieldType))
			}
			return nil
		}
		if isPtr {
			p := reflect.New(actualType)
			p.Elem().SetBool(v.Bool)
			field.Set(p)
		} else {
			field.SetBool(v.Bool)
		}
		return nil
	case *sql.NullTime:
		if !v.Valid {
			if isPtr {
				field.Set(reflect.Zero(fieldType))
			}
			return nil
		}
		if isPtr {
			p := reflect.New(actualType)
			p.Elem().Set(reflect.ValueOf(v.Time))
			field.Set(p)
		} else {
			field.Set(reflect.ValueOf(v.Time))
		}
		return nil
	default:
		return fmt.Errorf("unsupported scan target type: %T", target)
	}
}

func assignScannerValue(field reflect.Value, st *scannerTarget, actualType reflect.Type, isPtr bool) error {
	if isPtr {
		if !st.valid {
			field.Set(reflect.Zero(field.Type()))
			return nil
		}
		ptr := reflect.New(actualType)
		ptr.Elem().Set(reflect.ValueOf(st.dest).Elem())
		field.Set(ptr)
		return nil
	}
	field.Set(reflect.ValueOf(st.dest).Elem())
	return nil
}

func assignStringValue(field reflect.Value, value string, actualType reflect.Type, isPtr bool) error {
	if actualType.Kind() == reflect.Slice {
		newSlice := reflect.New(actualType)
		if err := json.Unmarshal([]byte(value), newSlice.Interface()); err != nil {
			if value == "" || value == "null" || value == "[]" {
				field.Set(reflect.MakeSlice(actualType, 0, 0))
				return nil
			}
			return fmt.Errorf("parse JSON array: %w", err)
		}
		field.Set(newSlice.Elem())
		return nil
	}

	if isPtr {
		p := reflect.New(actualType)
		p.Elem().SetString(value)
		field.Set(p)
	} else {
		if field.Kind() != reflect.String {
			return fmt.Errorf("cannot set string value to field of type %v", field.Kind())
		}
		field.SetString(value)
	}
	return nil
}

func assignIntValue(field reflect.Value, value int64, actualType reflect.Type, isPtr bool) error {
	if isPtr {
		p := reflect.New(actualType)
		p.Elem().SetInt(value)
		field.Set(p)
	} else {
		field.SetInt(value)
	}
	return nil
}

func assignFloatValue(field reflect.Value, value float64, actualType reflect.Type, isPtr bool) error {
	if isPtr {
		p := reflect.New(actualType)
		p.Elem().SetFloat(value)
		field.Set(p)
	} else {
		field.SetFloat(value)
	}
	return nil
}

// assignRowValue converts a raw any produced by the Driver into the
// typed field. Drivers are expected to emit values already in their
// natural Go form (strings, int64, float64, bool, time.Time, []byte
// for binary, nil for NULL). The Spark Connect path runs Arrow
// decoding upstream so that contract holds; the one exception is
// arrow.Timestamp, which we translate to time.Time here.
func assignRowValue(field reflect.Value, v any) error {
	if v == nil {
		field.Set(reflect.Zero(field.Type()))
		return nil
	}
	fieldType := field.Type()
	isPtr := fieldType.Kind() == reflect.Ptr
	actualType := fieldType
	if isPtr {
		actualType = fieldType.Elem()
	}

	// arrow.Timestamp → time.Time: Spark Connect returns TIMESTAMP
	// columns as arrow.Timestamp (a named int64 carrying a unit via
	// the Arrow schema). We don't have the unit at this seam, but
	// the fast-path write side pins MICROS via the parquet
	// `timestamp(microsecond)` tag, and Iceberg's SQL TIMESTAMP type
	// is microseconds. Treat the int64 as microseconds.
	if ts, ok := v.(arrow.Timestamp); ok && actualType == reflect.TypeOf(time.Time{}) {
		t := ts.ToTime(arrow.Microsecond)
		if isPtr {
			ptr := reflect.New(actualType)
			ptr.Elem().Set(reflect.ValueOf(t))
			field.Set(ptr)
		} else {
			field.Set(reflect.ValueOf(t))
		}
		return nil
	}

	// sql.Scanner types go through the scannerTarget path so custom
	// types like SortableID keep their NULL semantics.
	ptrToActual := reflect.PointerTo(actualType)
	if ptrToActual.Implements(scannerType) {
		dest := reflect.New(actualType).Interface().(sql.Scanner)
		if err := dest.Scan(v); err != nil {
			return err
		}
		if isPtr {
			ptr := reflect.New(actualType)
			ptr.Elem().Set(reflect.ValueOf(dest).Elem())
			field.Set(ptr)
		} else {
			field.Set(reflect.ValueOf(dest).Elem())
		}
		return nil
	}

	rv := reflect.ValueOf(v)
	if rv.Type().AssignableTo(actualType) {
		if isPtr {
			ptr := reflect.New(actualType)
			ptr.Elem().Set(rv)
			field.Set(ptr)
		} else {
			field.Set(rv)
		}
		return nil
	}
	if rv.Type().ConvertibleTo(actualType) {
		if isPtr {
			ptr := reflect.New(actualType)
			ptr.Elem().Set(rv.Convert(actualType))
			field.Set(ptr)
		} else {
			field.Set(rv.Convert(actualType))
		}
		return nil
	}

	// String fallback — arrays come back as JSON strings over Spark Connect.
	if s, ok := v.(string); ok {
		return assignStringValue(field, s, actualType, isPtr)
	}

	return fmt.Errorf("cannot assign %T to %v", v, actualType)
}
