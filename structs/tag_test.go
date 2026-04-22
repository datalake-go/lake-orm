package structs

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type tagTestUser struct {
	ID        string     `spark:"id,pk"`
	Email     string     `spark:"email,mergeKey"`
	Country   string     `spark:"country,indexed"`
	CreatedAt time.Time  `spark:"created_at,auto=createTime"`
	UpdatedAt time.Time  `spark:"updated_at,auto=updateTime"`
	DeletedAt *time.Time `spark:"deleted_at,nullable"`
	Internal  int        `spark:"-"`
	Untagged  string
}

func TestParseSchema_UserModel(t *testing.T) {
	schema, err := ParseSchema(reflect.TypeOf(tagTestUser{}))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}

	if schema.TableName != "tag_test_users" {
		t.Errorf("TableName = %q, want tag_test_users", schema.TableName)
	}
	if len(schema.PrimaryKeys) != 1 {
		t.Errorf("PrimaryKeys = %v, want 1 entry", schema.PrimaryKeys)
	}
	if len(schema.MergeKeys) != 1 {
		t.Errorf("MergeKeys = %v, want 1 entry", schema.MergeKeys)
	}

	byCol := map[string]LakeField{}
	for _, f := range schema.Fields {
		byCol[f.Column] = f
	}

	if f, ok := byCol["email"]; !ok {
		t.Errorf("email column missing")
	} else if f.Intent != IntentMergeKey {
		t.Errorf("email Intent = %v, want IntentMergeKey", f.Intent)
	}

	if f, ok := byCol["country"]; !ok {
		t.Errorf("country column missing")
	} else if f.Intent != IntentIndexed {
		t.Errorf("country Intent = %v, want IntentIndexed", f.Intent)
	}

	if f, ok := byCol["deleted_at"]; !ok {
		t.Errorf("deleted_at column missing")
	} else if !f.IsPointer || !f.IsNullable {
		t.Errorf("deleted_at should be pointer + nullable, got %+v", f)
	}

	if f, ok := byCol["created_at"]; !ok {
		t.Errorf("created_at column missing")
	} else if f.AutoBehavior != AutoCreateTime {
		t.Errorf("created_at AutoBehavior = %v, want AutoCreateTime", f.AutoBehavior)
	}

	// Ignored and untagged fields both show up as Ignored=true.
	if f, ok := byCol["internal"]; ok && !f.Ignored {
		t.Errorf("Internal (dorm:\"-\") must be ignored")
	}
	untaggedSeen := false
	for _, f := range schema.Fields {
		if f.Name == "Untagged" {
			untaggedSeen = true
			if !f.Ignored {
				t.Errorf("Untagged field must default to ignored")
			}
		}
	}
	if !untaggedSeen {
		t.Errorf("Untagged field missing from parsed schema")
	}
}

func TestColumnNames_SkipsIgnored(t *testing.T) {
	schema, err := ParseSchema(reflect.TypeOf(tagTestUser{}))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	cols := schema.ColumnNames()
	for _, c := range cols {
		if c == "internal" || c == "untagged" {
			t.Errorf("ColumnNames included ignored/untagged column: %q", c)
		}
	}
}

func TestToSnakePluralize(t *testing.T) {
	cases := []struct{ in, want string }{
		{"User", "users"},
		{"BoundingBox", "bounding_boxes"},
		{"City", "cities"},
		{"Bus", "buses"},
	}
	for _, c := range cases {
		got := pluralize(toSnake(c.in))
		if got != c.want {
			t.Errorf("pluralize(toSnake(%q)) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestParseSchema_UnknownModifierErrors(t *testing.T) {
	type bad struct {
		X string `spark:"x,nonsense"`
	}
	_, err := ParseSchema(reflect.TypeOf(bad{}))
	if err == nil {
		t.Fatal("ParseSchema should reject unknown modifier")
	}
}

// --- multi-tag alias ----------------------------------------------
//
// `lake:"..."`, `lakeorm:"..."` and `spark:"..."` all parse
// equivalently so callers can pick whichever reads better in their
// codebase. The three coexist like `gorm:"..."` and `json:"..."` —
// `lake` is the canonical lake-orm tag, `spark` is what the
// spark-connect-go fork reads natively, and `lakeorm` is a written-
// out synonym. `dorm:"..."` is NOT accepted; the library rename
// retired that spelling.

type lakeormTagOnly struct {
	ID    string `lakeorm:"id,pk"`
	Email string `lakeorm:"email"`
}

func TestParseSchema_AcceptsLakeormTagAsAlias(t *testing.T) {
	schema, err := ParseSchema(reflect.TypeOf(lakeormTagOnly{}))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if len(schema.Fields) != 2 {
		t.Fatalf("Fields = %d, want 2", len(schema.Fields))
	}
	if schema.Fields[0].Column != "id" || schema.Fields[1].Column != "email" {
		t.Errorf("columns = %+v, want [id email]", schema.Fields)
	}
	if len(schema.PrimaryKeys) != 1 || schema.PrimaryKeys[0] != 0 {
		t.Errorf("PrimaryKeys = %v, want [0]", schema.PrimaryKeys)
	}
}

type conflictingTags struct {
	ID string `spark:"spark_id,pk" lakeorm:"lake_id,pk"`
}

func TestParseSchema_RejectsBothSparkAndLakeormOnSameField(t *testing.T) {
	_, err := ParseSchema(reflect.TypeOf(conflictingTags{}))
	if err == nil {
		t.Fatal("ParseSchema should reject both spark: and lakeorm: tags on one field")
	}
	if !errors.Is(err, ErrInvalidTag) {
		t.Errorf("err = %v, want wraps ErrInvalidTag", err)
	}
}

type lakeTagOnly struct {
	ID      string `lake:"id,pk"`
	Email   string `lake:"email"`
	Country string `lake:"country,indexed"`
}

func TestParseSchema_AcceptsLakeTag(t *testing.T) {
	schema, err := ParseSchema(reflect.TypeOf(lakeTagOnly{}))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if len(schema.Fields) != 3 {
		t.Fatalf("Fields = %d, want 3", len(schema.Fields))
	}
	if len(schema.PrimaryKeys) != 1 || schema.PrimaryKeys[0] != 0 {
		t.Errorf("PrimaryKeys = %v, want [0]", schema.PrimaryKeys)
	}
	if schema.Fields[2].Intent != IntentIndexed {
		t.Errorf("country Intent = %v, want IntentIndexed", schema.Fields[2].Intent)
	}
}

type legacyRequiredOnLakeTag struct {
	Email string `lake:"email,required"`
}

func TestParseSchema_RejectsLegacyRequiredModifier(t *testing.T) {
	_, err := ParseSchema(reflect.TypeOf(legacyRequiredOnLakeTag{}))
	if err == nil {
		t.Fatal("ParseSchema should reject the legacy `required` modifier on the lake tag")
	}
	if !errors.Is(err, ErrInvalidTag) {
		t.Errorf("err = %v, want wraps ErrInvalidTag", err)
	}
}

type legacyValidateOnLakeTag struct {
	Email string `lake:"email,validate=email"`
}

func TestParseSchema_RejectsLegacyValidateModifier(t *testing.T) {
	_, err := ParseSchema(reflect.TypeOf(legacyValidateOnLakeTag{}))
	if err == nil {
		t.Fatal("ParseSchema should reject the legacy `validate=` modifier on the lake tag")
	}
	if !errors.Is(err, ErrInvalidTag) {
		t.Errorf("err = %v, want wraps ErrInvalidTag", err)
	}
}

type lakeAndSparkConflict struct {
	ID string `lake:"lake_id,pk" spark:"spark_id,pk"`
}

func TestParseSchema_RejectsLakeAndSparkOnSameField(t *testing.T) {
	_, err := ParseSchema(reflect.TypeOf(lakeAndSparkConflict{}))
	if err == nil {
		t.Fatal("ParseSchema should reject both lake: and spark: tags on one field")
	}
	if !errors.Is(err, ErrInvalidTag) {
		t.Errorf("err = %v, want wraps ErrInvalidTag", err)
	}
}

type lakeAndLakeormConflict struct {
	ID string `lake:"a,pk" lakeorm:"b,pk"`
}

func TestParseSchema_RejectsLakeAndLakeormOnSameField(t *testing.T) {
	_, err := ParseSchema(reflect.TypeOf(lakeAndLakeormConflict{}))
	if err == nil {
		t.Fatal("ParseSchema should reject both lake: and lakeorm: tags on one field")
	}
	if !errors.Is(err, ErrInvalidTag) {
		t.Errorf("err = %v, want wraps ErrInvalidTag", err)
	}
}

type mixedTagsPerField struct {
	ID      string `lake:"id,pk"`
	Email   string `lakeorm:"email"`
	Country string `spark:"country"`
}

// Different tag keys on different fields of the same struct are fine —
// conflict detection is per-field, not per-struct. Verified explicitly
// because it's the migration path for older codebases: add a new field
// with `lake:"..."` without rewriting the rest.
func TestParseSchema_MixedTagKeysAcrossFields(t *testing.T) {
	schema, err := ParseSchema(reflect.TypeOf(mixedTagsPerField{}))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if len(schema.Fields) != 3 {
		t.Fatalf("Fields = %d, want 3", len(schema.Fields))
	}
	wantCols := []string{"id", "email", "country"}
	for i, want := range wantCols {
		if schema.Fields[i].Column != want {
			t.Errorf("Fields[%d].Column = %q, want %q", i, schema.Fields[i].Column, want)
		}
	}
}

type dormTagIgnored struct {
	// `dorm:"..."` is NOT a recognised alias — the field has no spark
	// or lakeorm tag, so it falls through to the untagged branch
	// (Ignored = true). Pin the behaviour so someone who adds `dorm`
	// support in the future sees this test fail and revisits the
	// rename decision.
	ID string `dorm:"id,pk"`
}

func TestParseSchema_IgnoresLegacyDormTag(t *testing.T) {
	schema, err := ParseSchema(reflect.TypeOf(dormTagIgnored{}))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if len(schema.Fields) != 1 {
		t.Fatalf("Fields = %d, want 1", len(schema.Fields))
	}
	if !schema.Fields[0].Ignored {
		t.Error("ID field carrying only dorm:\"...\" should be Ignored (legacy alias intentionally unsupported)")
	}
}

// --- duplicate-column detection ------------------------------------

type duplicateColumnStruct struct {
	A string `lake:"email"`
	B string `lake:"email"`
}

func TestParseSchema_RejectsDuplicateColumnNames(t *testing.T) {
	_, err := ParseSchema(reflect.TypeOf(duplicateColumnStruct{}))
	if err == nil {
		t.Fatal("ParseSchema should reject two fields mapping to the same column")
	}
	if !errors.Is(err, ErrInvalidTag) {
		t.Errorf("err = %v, want wraps ErrInvalidTag", err)
	}
	if !strings.Contains(err.Error(), "duplicate column") {
		t.Errorf("error should mention duplicate column, got: %v", err)
	}
}

type duplicateAcrossTagAliases struct {
	A string `lake:"status"`
	B string `lakeorm:"status"`
}

func TestParseSchema_RejectsDuplicateColumnAcrossTagAliases(t *testing.T) {
	_, err := ParseSchema(reflect.TypeOf(duplicateAcrossTagAliases{}))
	if err == nil {
		t.Fatal("ParseSchema should reject column collision regardless of tag key")
	}
	if !errors.Is(err, ErrInvalidTag) {
		t.Errorf("err = %v, want wraps ErrInvalidTag", err)
	}
}
