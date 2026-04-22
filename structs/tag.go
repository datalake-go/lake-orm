package structs

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"

	"github.com/datalake-go/lake-orm/types"
)

// AutoBehavior describes auto-populated column content. Client.Insert
// visits every field with AutoBehavior != AutoNone before validation
// and writes the canonical value for the behaviour.
type AutoBehavior int

const (
	AutoNone AutoBehavior = iota
	AutoCreateTime
	AutoUpdateTime
)

// PartitionStrategy is the tag-declared partition intent. Dialect
// implementations translate this into a physical strategy (Iceberg
// bucket, Delta ZORDER, etc.) via IndexStrategy().
type PartitionStrategy int

const (
	PartitionNone PartitionStrategy = iota
	PartitionRaw
	PartitionBucket
	PartitionTruncate
)

// IndexIntent captures the `indexed` / `sortable` / `mergeKey` tag intent.
// Dialects read this and return a concrete IndexStrategy.
type IndexIntent int

const (
	IntentNone IndexIntent = iota
	IntentIndexed
	IntentSortable
	IntentMergeKey
)

// LayoutIntent is the coarse layout hint currently exposed — may grow
// later to cover clustering, data-skipping ranges, etc.
type LayoutIntent int

const (
	LayoutNone LayoutIntent = iota
	LayoutSortable
)

// IndexStrategy / LayoutStrategy are opaque to the core — they are
// stringified descriptors that Dialect implementations emit for their
// own DDL generation. Kept as strings rather than typed-per-Dialect
// enums to avoid a compatibility matrix in the core package.
type (
	IndexStrategy  string
	LayoutStrategy string
)

// LakeSchema is the parsed form of a struct tagged with `spark:"..."`.
type LakeSchema struct {
	GoType      reflect.Type
	TableName   string
	Fields      []LakeField
	PrimaryKeys []int
	MergeKeys   []int
	Partitions  []LakePartitionSpec
}

// LakeField is the parsed metadata for one struct field.
type LakeField struct {
	Name         string // Go field name
	Column       string // DB column name
	Index        []int  // reflect.FieldByIndex path
	Type         reflect.Type
	IsNullable   bool
	IsPointer    bool
	AutoBehavior AutoBehavior
	IsJSON       bool
	Intent       IndexIntent
	Layout       LayoutIntent
	Ignored      bool
}

// LakePartitionSpec names the field index and strategy of a partition.
type LakePartitionSpec struct {
	FieldIndex int
	Strategy   PartitionStrategy
	Param      int // N for Bucket(N) / Truncate(N); zero otherwise
}

// ColumnNames returns the column names in declaration order, skipping
// ignored fields. Used by the query builder to emit projection-pushdown
// SELECTs instead of SELECT *.
func (s *LakeSchema) ColumnNames() []string {
	out := make([]string, 0, len(s.Fields))
	for i := range s.Fields {
		if s.Fields[i].Ignored {
			continue
		}
		out = append(out, s.Fields[i].Column)
	}
	return out
}

var (
	schemaCache   sync.Map // reflect.Type -> *LakeSchema
	tableOverride sync.Map // reflect.Type -> string
)

// Table overrides the derived table name for a Go type. Call once at
// init, before any Insert / Query / Migrate.
func Table(model any, name string) {
	t := reflect.TypeOf(model)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	tableOverride.Store(t, name)
	schemaCache.Delete(t) // force re-parse so the override sticks
}

// ParseSchema extracts LakeSchema from a Go type via reflection.
// Cached per-type; safe to call repeatedly.
func ParseSchema(t reflect.Type) (*LakeSchema, error) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("lakeorm: ParseSchema requires a struct type, got %v", t.Kind())
	}
	if cached, ok := schemaCache.Load(t); ok {
		return cached.(*LakeSchema), nil
	}

	schema := &LakeSchema{
		GoType:    t,
		TableName: defaultTableName(t),
	}
	if override, ok := tableOverride.Load(t); ok {
		schema.TableName = override.(string)
	}

	if err := walkFields(t, nil, schema); err != nil {
		return nil, err
	}

	schemaCache.Store(t, schema)
	return schema, nil
}

func walkFields(t reflect.Type, parentIndex []int, schema *LakeSchema) error {
	// seenColumns tracks column names already claimed by a field on
	// this schema. Two fields resolving to the same column would
	// produce a CREATE TABLE with duplicate column definitions —
	// Spark / Iceberg / Delta silently overwrite one in some code
	// paths, DuckDB outright rejects it. Catching at parse time
	// forces the caller to rename before any engine sees the schema.
	//
	// Initialised lazily on the first non-ignored field so the
	// recursive embedded-struct walk shares one map across the root
	// call.
	var seenColumns map[string]string
	return walkFieldsInto(t, parentIndex, schema, &seenColumns)
}

func walkFieldsInto(t reflect.Type, parentIndex []int, schema *LakeSchema, seenColumns *map[string]string) error {
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		idx := append(append([]int{}, parentIndex...), i)

		tag, err := effectiveTag(sf)
		if err != nil {
			return err
		}

		// Embedded structs without a tag are transparently walked.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct && tag == "" {
			if err := walkFieldsInto(sf.Type, idx, schema, seenColumns); err != nil {
				return err
			}
			continue
		}

		field, ok, err := parseField(sf, idx, tag)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if !field.Ignored {
			if *seenColumns == nil {
				*seenColumns = make(map[string]string)
			}
			if prev, taken := (*seenColumns)[field.Column]; taken {
				return fmt.Errorf(
					"%w: duplicate column %q on %s: already declared by field %s",
					ErrInvalidTag, field.Column, sf.Name, prev)
			}
			(*seenColumns)[field.Column] = sf.Name
		}

		fieldPos := len(schema.Fields)
		schema.Fields = append(schema.Fields, field)

		if tagHas(tag, "pk") {
			schema.PrimaryKeys = append(schema.PrimaryKeys, fieldPos)
		}
		if field.Intent == IntentMergeKey {
			schema.MergeKeys = append(schema.MergeKeys, fieldPos)
		}

		if spec, ok := parsePartition(tag, fieldPos); ok {
			schema.Partitions = append(schema.Partitions, spec)
		}
	}
	return nil
}

func parseField(sf reflect.StructField, idx []int, tag string) (LakeField, bool, error) {
	field := LakeField{
		Name:   sf.Name,
		Column: toSnake(sf.Name),
		Index:  idx,
		Type:   sf.Type,
	}

	if tag == "-" {
		field.Ignored = true
		return field, true, nil
	}
	if tag == "" {
		// Untagged exported fields are ignored — the lake tag is the contract.
		field.Ignored = true
		return field, true, nil
	}

	parts := splitTag(tag)
	if parts[0] != "" {
		field.Column = parts[0]
	}

	// _ingest_id is a system-managed column — every table lake-orm
	// creates carries it synthetically, stamped with a UUIDv7 per
	// Insert. Users declaring it would collide with the synthesised
	// column; refuse the declaration up front.
	if field.Column == types.SystemIngestIDColumn {
		return field, false, fmt.Errorf(
			"%w: field %s: %q is a system-managed column — remove it from your struct "+
				"(lake-orm generates one per Insert automatically; see INGEST_ID.md)",
			ErrInvalidTag, sf.Name, types.SystemIngestIDColumn)
	}

	for _, mod := range parts[1:] {
		kv := strings.SplitN(mod, "=", 2)
		key := strings.TrimSpace(kv[0])
		var val string
		if len(kv) == 2 {
			val = strings.TrimSpace(kv[1])
		}
		switch key {
		case "pk":
			// handled by walkFields
		case "mergeKey":
			field.Intent = IntentMergeKey
		case "indexed":
			if field.Intent == IntentNone {
				field.Intent = IntentIndexed
			}
		case "sortable":
			field.Layout = LayoutSortable
		case "nullable":
			field.IsNullable = true
		case "required":
			// Retired: lake-orm's validation now wraps
			// go-playground/validator. Use the standard
			// `validate:"required,..."` struct tag instead.
			return field, false, fmt.Errorf(
				"%w: required modifier on lake tag is no longer supported on %s — "+
					`use the standard go-playground struct tag: validate:"required"`,
				ErrInvalidTag, sf.Name)
		case "json":
			field.IsJSON = true
		case "auto":
			switch val {
			case "createTime":
				field.AutoBehavior = AutoCreateTime
			case "updateTime":
				field.AutoBehavior = AutoUpdateTime
			case "ingestID":
				// auto=ingestID was an earlier modifier that stamped
				// UUIDv7s onto a user-declared string field. The
				// column is now system-managed (types.SystemIngestIDColumn
				// is synthesised per-table, stamped at the driver
				// layer) and the tag is no longer valid. Remove the
				// field from the struct.
				return field, false, fmt.Errorf(
					"%w: auto=ingestID on %s is no longer supported — %q is a system-managed column, "+
						"remove the field from your struct (lake-orm adds it automatically; see INGEST_ID.md)",
					ErrInvalidTag, sf.Name, types.SystemIngestIDColumn)
			default:
				return field, false, fmt.Errorf("%w: auto=%q on %s", ErrInvalidTag, val, sf.Name)
			}
		case "validate":
			// Retired: lake-orm's validation now wraps
			// go-playground/validator. Rules live on the standard
			// `validate:"..."` struct tag instead of as lake-tag
			// modifiers. Example:
			//   Email string `lake:"email" validate:"required,email"`
			return field, false, fmt.Errorf(
				"%w: validate=%q modifier on lake tag is no longer supported on %s — "+
					`use the standard go-playground struct tag: validate:"%s"`,
				ErrInvalidTag, val, sf.Name, val)
		case "partition":
			// parsed by parsePartition; ignore here
		default:
			return field, false, fmt.Errorf("%w: unknown modifier %q on %s", ErrInvalidTag, key, sf.Name)
		}
	}

	if sf.Type.Kind() == reflect.Ptr {
		field.IsPointer = true
		field.IsNullable = true
	}

	return field, true, nil
}

func parsePartition(tag string, fieldPos int) (LakePartitionSpec, bool) {
	for _, mod := range splitTag(tag)[1:] {
		if !strings.HasPrefix(mod, "partition=") {
			continue
		}
		val := strings.TrimPrefix(mod, "partition=")
		switch {
		case val == "raw":
			return LakePartitionSpec{FieldIndex: fieldPos, Strategy: PartitionRaw}, true
		case strings.HasPrefix(val, "bucket:"):
			var n int
			fmt.Sscanf(val, "bucket:%d", &n)
			return LakePartitionSpec{FieldIndex: fieldPos, Strategy: PartitionBucket, Param: n}, true
		case strings.HasPrefix(val, "truncate:"):
			var n int
			fmt.Sscanf(val, "truncate:%d", &n)
			return LakePartitionSpec{FieldIndex: fieldPos, Strategy: PartitionTruncate, Param: n}, true
		}
	}
	return LakePartitionSpec{}, false
}

// effectiveTag reads any of the accepted tag keys off a struct
// field and returns the one that's set. Three accepted spellings:
//
//   - `lake:"..."`    — the canonical lake-orm ORM tag, analogous
//     to `gorm:"..."` in GORM. Recommended for new code.
//   - `lakeorm:"..."` — library-native alias, same semantics.
//   - `spark:"..."`   — driver-level tag; read by the spark-connect-
//     go fork's typed DataFrame helpers too. Kept as an accepted
//     alias so ORM-tagged structs can also be scanned directly by
//     driver code without duplicating the tag.
//
// All three are strictly equivalent; parse grammar, modifier set,
// and semantics are identical. A field that carries more than one
// with non-empty values errors at parse time so the alias can't
// drift into two sources of truth on a single declaration.
//
// Think of it the way Go structs already handle `gorm:"..."` and
// `json:"..."` side by side: the ORM respects its own tag, but
// downstream driver / serializer layers can read whichever they
// prefer. Here the three tags happen to be equivalent rather than
// semantically distinct — lake-orm ultimately scans all of them
// through the same parse path.
//
// Pre-rename `dorm:"..."` is deliberately NOT accepted — the
// library rename was a one-way move and keeping `dorm:` legal
// would rot into a fourth synonym.
func effectiveTag(sf reflect.StructField) (string, error) {
	lakeTag, hasLake := sf.Tag.Lookup("lake")
	lakeormTag, hasLakeorm := sf.Tag.Lookup("lakeorm")
	sparkTag, hasSpark := sf.Tag.Lookup("spark")

	// Collect non-empty declarations to detect conflict.
	var set []struct {
		name, val string
	}
	if hasLake && lakeTag != "" {
		set = append(set, struct{ name, val string }{"lake", lakeTag})
	}
	if hasLakeorm && lakeormTag != "" {
		set = append(set, struct{ name, val string }{"lakeorm", lakeormTag})
	}
	if hasSpark && sparkTag != "" {
		set = append(set, struct{ name, val string }{"spark", sparkTag})
	}
	if len(set) > 1 {
		return "", fmt.Errorf("%w: field %s carries %s:%q and %s:%q; pick one",
			ErrInvalidTag, sf.Name, set[0].name, set[0].val, set[1].name, set[1].val)
	}

	// Preference when exactly one is set: lake > lakeorm > spark.
	// For "-" ignore sentinel we still honour whichever was typed.
	if hasLake {
		return lakeTag, nil
	}
	if hasLakeorm {
		return lakeormTag, nil
	}
	return sparkTag, nil
}

func splitTag(tag string) []string {
	out := strings.Split(tag, ",")
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	return out
}

func tagHas(tag, mod string) bool {
	for _, p := range splitTag(tag)[1:] {
		if p == mod {
			return true
		}
	}
	return false
}

// defaultTableName derives a snake_case, lightly pluralized table name
// from a struct type. "User" → "users", "BoundingBox" → "bounding_boxes".
func defaultTableName(t reflect.Type) string {
	name := toSnake(t.Name())
	return pluralize(name)
}

func pluralize(s string) string {
	switch {
	case strings.HasSuffix(s, "s"), strings.HasSuffix(s, "x"), strings.HasSuffix(s, "z"),
		strings.HasSuffix(s, "ch"), strings.HasSuffix(s, "sh"):
		return s + "es"
	case strings.HasSuffix(s, "y"):
		return s[:len(s)-1] + "ies"
	default:
		return s + "s"
	}
}

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
