package sqlbuild

import (
	"reflect"
	"testing"
)

func TestSelect_MinimalShape(t *testing.T) {
	got, args := Select{Table: "users"}.Build()
	if got != "SELECT * FROM users" {
		t.Errorf("got %q, want SELECT * FROM users", got)
	}
	if len(args) != 0 {
		t.Errorf("expected no args, got %v", args)
	}
}

func TestSelect_ColumnsOverrideStar(t *testing.T) {
	got, _ := Select{Columns: []string{"id", "email"}, Table: "users"}.Build()
	if got != "SELECT id, email FROM users" {
		t.Errorf("got %q", got)
	}
}

func TestSelect_FullShape(t *testing.T) {
	got, args := Select{
		Columns: []string{"id", "email"},
		Table:   "lakeorm.default.users",
		Where:   "country = ?",
		Args:    []any{"UK"},
		OrderBy: []OrderSpec{
			{Column: "created_at", Desc: true},
			{Column: "id"},
		},
		Limit:  100,
		Offset: 50,
	}.Build()

	want := "SELECT id, email FROM lakeorm.default.users WHERE country = ? ORDER BY created_at DESC, id LIMIT 100 OFFSET 50"
	if got != want {
		t.Errorf("\n got: %s\nwant: %s", got, want)
	}
	if !reflect.DeepEqual(args, []any{"UK"}) {
		t.Errorf("args = %v, want [UK]", args)
	}
}

func TestSelect_SkipsZeroLimitOffset(t *testing.T) {
	// Zero limit/offset stay out of the rendered SQL — we don't
	// want "LIMIT 0" on the wire when the caller just didn't set one.
	got, _ := Select{Table: "t", Limit: 0, Offset: 0}.Build()
	if got != "SELECT * FROM t" {
		t.Errorf("unexpected clauses in %q", got)
	}
}
