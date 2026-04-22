package structs

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/datalake-go/lake-orm/types"
)

// Tag grammar.
//
// A lake-orm struct tag looks like:
//
//	`lake:"<column>[,modifier[=value]]..."`
//
// Three tag keys parse identically — pick whichever reads best in
// your codebase: `lake:"..."` (canonical), `lakeorm:"..."` (library-
// native synonym), `spark:"..."` (driver-native synonym, so the
// Spark Connect fork's typed DataFrame helpers can scan the same
// struct without duplicating the declaration).
//
// A single field carrying non-empty values on two tag keys is a
// parse-time ErrInvalidTag; the alias cannot become a second source
// of truth on one declaration.
//
// Accepted modifiers:
//
//	pk                  primary key
//	mergeKey            upsert identity — Insert auto-routes to MERGE
//	indexed             dialect-specific index hint
//	sortable            dialect-specific sort-order hint
//	nullable            column is NULL-able (pointer types implicit)
//	json                scalar serialised as JSON in a STRING column
//	auto=createTime     stamped on every new row
//	auto=updateTime     stamped on every write
//	partition=raw       identity partitioning (e.g. event_date col)
//	partition=bucket:N  hash-bucket partitioning with N buckets
//	partition=truncate:N  prefix-truncate partitioning (N = length)
//
// Retired modifiers produce a parse-time error with a migration
// message pointing at the replacement:
//
//	required            use validate:"required" on the struct tag
//	validate=...        use validate:"..." on the struct tag
//	auto=ingestID       removed — _ingest_id is now system-managed
//
// `_ingest_id` as a user-declared column is also a parse-time
// error; the column is synthesised by every dialect and stamped by
// every driver at write time.

// AutoBehavior describes auto-populated column content. Client.Insert
// visits every field with AutoBehavior != AutoNone before validation
// and writes the canonical value for the behaviour.
type AutoBehavior int

const (
	AutoNone AutoBehavior = iota
	AutoCreateTime
	AutoUpdateTime
)

// ColumnNames returns the column names in declaration order, skipping
// ignored fields. Used by the query builder to emit projection-
// pushdown SELECTs instead of SELECT *.
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

// IndexIntent captures the `indexed` / `sortable` / `mergeKey` tag
// intent. Dialects read this and return a concrete IndexStrategy.
type IndexIntent int

const (
	IntentNone IndexIntent = iota
	IntentIndexed
	IntentSortable
	IntentMergeKey
)

// IndexStrategy is an opaque descriptor a Dialect emits for its own
// DDL generation — the string form is Dialect-specific (e.g. Iceberg
// "bucket:64", Delta "zorder"). Kept as a string alias so the core
// stays dialect-agnostic.
type IndexStrategy string

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

// LakeSchema is the parsed form of a tagged struct — the runtime
// view every dialect, driver, and migration author reads to make
// decisions about DDL, partitioning, merge routing, and projection.
type LakeSchema struct {
	GoType      reflect.Type
	TableName   string
	Fields      []LakeField
	PrimaryKeys []int
	MergeKeys   []int
	Partitions  []LakePartitionSpec
}

// LayoutIntent is the coarse layout hint currently exposed — may
// grow later to cover clustering, data-skipping ranges, etc.
type LayoutIntent int

const (
	LayoutNone LayoutIntent = iota
	LayoutSortable
)

// LayoutStrategy is an opaque descriptor a Dialect emits for its
// own layout directives (e.g. Iceberg "sort_order"). Same role as
// IndexStrategy for a different axis of physical layout.
type LayoutStrategy string

// ParseSchema extracts a LakeSchema from a Go type via reflection.
// Cached per reflect.Type; safe to call repeatedly from hot paths.
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

// schemaCache caches ParseSchema's output keyed by reflect.Type.
// Shared with table.go's Table() override, which invalidates the
// cache on rename.
var schemaCache sync.Map // reflect.Type -> *LakeSchema

// effectiveTag reads any of the accepted tag keys off a struct
// field and returns the one that's set. A field carrying more than
// one with non-empty values errors so the alias can't drift into
// two sources of truth on a single declaration.
func effectiveTag(sf reflect.StructField) (string, error) {
	lakeTag, hasLake := sf.Tag.Lookup("lake")
	lakeormTag, hasLakeorm := sf.Tag.Lookup("lakeorm")
	sparkTag, hasSpark := sf.Tag.Lookup("spark")

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
	if hasLake {
		return lakeTag, nil
	}
	if hasLakeorm {
		return lakeormTag, nil
	}
	return sparkTag, nil
}

// parseField turns one reflect.StructField into a LakeField. Tag
// modifiers are checked here; retired modifiers return a parse-time
// error with a migration message.
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
		field.Ignored = true
		return field, true, nil
	}

	parts := splitTag(tag)
	if parts[0] != "" {
		field.Column = parts[0]
	}

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
				return field, false, fmt.Errorf(
					"%w: auto=ingestID on %s is no longer supported — %q is a system-managed column, "+
						"remove the field from your struct (lake-orm adds it automatically; see INGEST_ID.md)",
					ErrInvalidTag, sf.Name, types.SystemIngestIDColumn)
			default:
				return field, false, fmt.Errorf("%w: auto=%q on %s", ErrInvalidTag, val, sf.Name)
			}
		case "validate":
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

// parsePartition extracts a partition spec from a tag, if any. The
// partition=... modifier is the only one with a sub-grammar; the
// rest of parseField short-circuits when it sees `partition=` and
// defers to this function.
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

// splitTag splits a tag string on commas and trims surrounding
// whitespace on each part.
func splitTag(tag string) []string {
	out := strings.Split(tag, ",")
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	return out
}

// tagHas reports whether a tag carries a bare modifier (no "=value").
func tagHas(tag, mod string) bool {
	for _, p := range splitTag(tag)[1:] {
		if p == mod {
			return true
		}
	}
	return false
}

// walkFields is the entry to the recursive field walker. Lazy-
// initialises the per-schema seenColumns map on first hit so the
// embedded-struct recursion shares one map across the root call.
func walkFields(t reflect.Type, parentIndex []int, schema *LakeSchema) error {
	var seenColumns map[string]string
	return walkFieldsInto(t, parentIndex, schema, &seenColumns)
}

// walkFieldsInto walks a struct's fields, parsing tags and
// collecting fields / primary keys / merge keys / partitions into
// the schema. Embedded structs are transparently descended when
// they carry no tag.
//
// Two fields resolving to the same column name on the same schema
// are a parse-time ErrInvalidTag — Spark / Iceberg / Delta each
// handle duplicate columns differently (silent overwrite in some
// paths, outright rejection in others), so catching it at parse
// time forces the caller to rename before any engine sees the
// schema.
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
