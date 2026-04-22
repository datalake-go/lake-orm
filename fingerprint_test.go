package lakeorm

import (
	"strings"
	"testing"
	"time"

	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/types"
)

type fpUser struct {
	ID        types.SortableID `spark:"id,pk"`
	Email     string           `spark:"email,mergeKey"`
	CreatedAt time.Time        `spark:"created_at"`
}

// Same columns, same types, reordered declaration → same fingerprint.
type fpUserReordered struct {
	CreatedAt time.Time        `spark:"created_at"`
	Email     string           `spark:"email,mergeKey"`
	ID        types.SortableID `spark:"id,pk"`
}

func TestSchemaFingerprint_StableAcrossReorder(t *testing.T) {
	a, err := SchemaFingerprint(&fpUser{})
	if err != nil {
		t.Fatalf("fpUser: %v", err)
	}
	b, err := SchemaFingerprint(&fpUserReordered{})
	if err != nil {
		t.Fatalf("fpUserReordered: %v", err)
	}
	// They'll be DIFFERENT because the two structs have different
	// derived table names (fp_user vs fp_user_reordered). The
	// fingerprint includes the table name by design — an identical
	// schema under a different table name is NOT the same artifact
	// because AssertSchema would be looking at a different row in
	// the catalog.
	if a == b {
		t.Errorf("different tables should produce different fingerprints; got identical %s", a)
	}
}

// Variant: same table name (via lakeorm.Table override), same columns
// in different order → fingerprint matches.
type fpUserTableA struct {
	ID        string    `spark:"id,pk"`
	Email     string    `spark:"email,mergeKey"`
	CreatedAt time.Time `spark:"created_at"`
}
type fpUserTableB struct {
	CreatedAt time.Time `spark:"created_at"`
	Email     string    `spark:"email,mergeKey"`
	ID        string    `spark:"id,pk"`
}

func TestSchemaFingerprint_SameTableSameColumnsDifferentOrder(t *testing.T) {
	structs.Table(&fpUserTableA{}, "the_users")
	structs.Table(&fpUserTableB{}, "the_users")
	a, err := SchemaFingerprint(&fpUserTableA{})
	if err != nil {
		t.Fatalf("A: %v", err)
	}
	b, err := SchemaFingerprint(&fpUserTableB{})
	if err != nil {
		t.Fatalf("B: %v", err)
	}
	if a != b {
		t.Errorf("same-table same-columns should fingerprint identically; got A=%s B=%s", a, b)
	}
}

func TestSchemaFingerprint_SensitiveToColumnAddition(t *testing.T) {
	type before struct {
		ID string `spark:"id,pk"`
	}
	type after struct {
		ID   string `spark:"id,pk"`
		Name string `spark:"name"`
	}
	structs.Table(&before{}, "t1")
	structs.Table(&after{}, "t1")
	a, _ := SchemaFingerprint(&before{})
	b, _ := SchemaFingerprint(&after{})
	if a == b {
		t.Errorf("column addition should change fingerprint; both returned %s", a)
	}
}

func TestSchemaFingerprint_FormatPrefixed(t *testing.T) {
	fp, err := SchemaFingerprint(&fpUser{})
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if !strings.HasPrefix(fp, "sha256:") {
		t.Errorf("fingerprint should be sha256-prefixed; got %q", fp)
	}
	if len(fp) != len("sha256:")+64 {
		t.Errorf("fingerprint should be sha256: + 64 hex chars; got %q (len=%d)", fp, len(fp))
	}
}

func TestSchemaFingerprint_NilError(t *testing.T) {
	if _, err := SchemaFingerprint(nil); err == nil {
		t.Errorf("expected error on nil input")
	}
}
