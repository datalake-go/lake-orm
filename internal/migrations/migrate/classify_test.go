package migrate

import (
	"strings"
	"testing"
)

func TestClassify_TableDriven(t *testing.T) {
	cases := []struct {
		name            string
		change          Change
		wantDestructive bool
		reasonContains  string
	}{
		{"add nullable no default", Change{Op: OpAddColumn, Nullable: true}, false, ""},
		{"add nullable with default", Change{Op: OpAddColumn, Nullable: true, HasDefault: true, Default: "'tier-x'"}, true, "default value is a semantic choice"},
		{"add not null", Change{Op: OpAddColumn, Nullable: false}, true, "full-table scan"},
		{"drop column", Change{Op: OpDropColumn, Column: "tier"}, true, "data loss"},
		{"rename column", Change{Op: OpRenameColumn, OldColumn: "email_address", Column: "email"}, true, "downstream readers may break"},
		{"widen type", Change{Op: OpWidenType, OldType: "INT", Type: "BIGINT"}, false, ""},
		{"narrow type", Change{Op: OpNarrowType, OldType: "BIGINT", Type: "INT"}, true, "overflow risk"},
		{"set not null", Change{Op: OpSetNotNull}, true, "scan-and-validate"},
		{"set nullable", Change{Op: OpSetNullable}, false, ""},
		{"create table", Change{Op: OpCreateTable}, false, ""},
		{"unknown op", Change{Op: OpUnknown}, true, "not recognised"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Classify(c.change, "iceberg")
			if got.Destructive != c.wantDestructive {
				t.Errorf("Destructive = %v, want %v (reason: %s)", got.Destructive, c.wantDestructive, got.Reason)
			}
			if c.reasonContains != "" && !strings.Contains(got.Reason, c.reasonContains) {
				t.Errorf("Reason = %q, want to contain %q", got.Reason, c.reasonContains)
			}
		})
	}
}
