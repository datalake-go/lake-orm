package migrate

import (
	"reflect"
	"testing"
)

func schemaOf(name string, fields ...Field) *Schema {
	return &Schema{TableName: name, Fields: fields}
}

func col(name string, goType reflect.Type, nullable bool) Field {
	return Field{Column: name, GoType: goType, Nullable: nullable}
}

var (
	stringTy = reflect.TypeOf("")
	int8Ty   = reflect.TypeOf(int8(0))
	int32Ty  = reflect.TypeOf(int32(0))
	int64Ty  = reflect.TypeOf(int64(0))
)

func TestDiff_NilCurrent_ProducesCreateTable(t *testing.T) {
	target := schemaOf("users",
		col("id", stringTy, false),
		col("email", stringTy, false),
	)
	changes := Diff(nil, target, "iceberg")
	if len(changes) != 1 {
		t.Fatalf("len(changes) = %d, want 1; got %+v", len(changes), changes)
	}
	if changes[0].Op != OpCreateTable || changes[0].Table != target.TableName {
		t.Errorf("create-table change wrong shape: %+v", changes[0])
	}
}

func TestDiff_AddColumn(t *testing.T) {
	current := schemaOf("users",
		col("id", stringTy, false),
		col("email", stringTy, false),
	)
	target := schemaOf("users",
		col("id", stringTy, false),
		col("email", stringTy, false),
		col("tier", stringTy, true),
	)
	changes := Diff(current, target, "iceberg")
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %+v", len(changes), changes)
	}
	c := changes[0]
	if c.Op != OpAddColumn || c.Column != "tier" || !c.Nullable {
		t.Errorf("bad add-column change: %+v", c)
	}
}

func TestDiff_DropColumn(t *testing.T) {
	current := schemaOf("users",
		col("id", stringTy, false),
		col("email", stringTy, false),
	)
	target := schemaOf("users",
		col("id", stringTy, false),
	)
	changes := Diff(current, target, "iceberg")
	if len(changes) != 1 || changes[0].Op != OpDropColumn || changes[0].Column != "email" {
		t.Fatalf("expected 1 drop on 'email', got %+v", changes)
	}
}

func TestDiff_WidenType(t *testing.T) {
	current := schemaOf("t",
		col("id", stringTy, false),
		col("n", int8Ty, false),
	)
	target := schemaOf("t",
		col("id", stringTy, false),
		col("n", int64Ty, false),
	)
	changes := Diff(current, target, "iceberg")
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %+v", changes)
	}
	if changes[0].Op != OpWidenType {
		t.Errorf("expected OpWidenType, got %+v", changes[0])
	}
}

func TestDiff_NarrowType(t *testing.T) {
	current := schemaOf("t",
		col("id", stringTy, false),
		col("n", int64Ty, false),
	)
	target := schemaOf("t",
		col("id", stringTy, false),
		col("n", int32Ty, false),
	)
	changes := Diff(current, target, "iceberg")
	if len(changes) != 1 || changes[0].Op != OpNarrowType {
		t.Fatalf("expected OpNarrowType, got %+v", changes)
	}
}

func TestDiff_NoChanges_ReturnsEmpty(t *testing.T) {
	s := schemaOf("t",
		col("id", stringTy, false),
		col("email", stringTy, false),
	)
	if changes := Diff(s, s, "iceberg"); len(changes) != 0 {
		t.Errorf("expected empty diff, got %+v", changes)
	}
}
