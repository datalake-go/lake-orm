package migrations

import (
	"strings"
	"testing"
	"time"

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

func TestFingerprint_StableAcrossReorder(t *testing.T) {
	a, err := Fingerprint(&fpUser{})
	if err != nil {
		t.Fatalf("fpUser: %v", err)
	}
	b, err := Fingerprint(&fpUserReordered{})
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

// Variant: same table name (via TableNamer override), same columns
// in different order → fingerprint matches.
type fpUserTableA struct {
	ID        string    `spark:"id,pk"`
	Email     string    `spark:"email,mergeKey"`
	CreatedAt time.Time `spark:"created_at"`
}

func (fpUserTableA) TableName() string { return "the_users" }

type fpUserTableB struct {
	CreatedAt time.Time `spark:"created_at"`
	Email     string    `spark:"email,mergeKey"`
	ID        string    `spark:"id,pk"`
}

func (fpUserTableB) TableName() string { return "the_users" }

func TestFingerprint_SameTableSameColumnsDifferentOrder(t *testing.T) {
	a, err := Fingerprint(&fpUserTableA{})
	if err != nil {
		t.Fatalf("A: %v", err)
	}
	b, err := Fingerprint(&fpUserTableB{})
	if err != nil {
		t.Fatalf("B: %v", err)
	}
	if a != b {
		t.Errorf("same-table same-columns should fingerprint identically; got A=%s B=%s", a, b)
	}
}

type fpAdditionBefore struct {
	ID string `spark:"id,pk"`
}

func (fpAdditionBefore) TableName() string { return "t1" }

type fpAdditionAfter struct {
	ID   string `spark:"id,pk"`
	Name string `spark:"name"`
}

func (fpAdditionAfter) TableName() string { return "t1" }

func TestFingerprint_SensitiveToColumnAddition(t *testing.T) {
	a, _ := Fingerprint(&fpAdditionBefore{})
	b, _ := Fingerprint(&fpAdditionAfter{})
	if a == b {
		t.Errorf("column addition should change fingerprint; both returned %s", a)
	}
}

func TestFingerprint_FormatPrefixed(t *testing.T) {
	fp, err := Fingerprint(&fpUser{})
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

func TestFingerprint_NilError(t *testing.T) {
	if _, err := Fingerprint(nil); err == nil {
		t.Errorf("expected error on nil input")
	}
}
