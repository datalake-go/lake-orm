// Package migrations is lakeorm's authoring side of schema
// evolution. Given a tagged Go struct and the State-JSON header
// of the most-recent migration file for the same table, it
// computes the diff, classifies each change as destructive or
// safe per the dialect rule table (see MIGRATIONS.md), and emits
// one goose-format .sql file per changed table with
// `-- DESTRUCTIVE: <reason>` comments on anything a reviewer
// should notice in the PR diff.
//
// Execution is NOT this package's concern — the .sql files run
// through lake-goose against the Spark Connect database/sql
// driver. This package writes files; lake-goose runs them.
//
// The State-JSON header carries the target table state after
// every Up statement in that file has applied — Django's
// MigrationLoader pattern. Regeneration replays the most-recent
// file's State-JSON, compares it to the current Go struct via
// a Fingerprint, and only emits a new migration when the
// fingerprint changed. That is what makes MigrateGenerate
// idempotent.
package migrations

import "reflect"

// Op names the kind of schema change. Every Change carries one Op
// and the operands that make sense for it; unused fields on Change
// are ignored by Classify and Generate for ops that don't need them.
type Op int

const (
	OpUnknown Op = iota
	// OpCreateTable is emitted when the current schema is nil — the
	// whole struct is a new table. v0 passes this through to the
	// Dialect's CreateTableDDL; Generate skips these (bootstrap is
	// handled by db.Migrate) unless explicitly requested.
	OpCreateTable
	// OpAddColumn adds Column with Type and Nullable. HasDefault /
	// Default carry non-NULL default metadata when present.
	OpAddColumn
	// OpDropColumn drops Column. Data loss; always unsafe.
	OpDropColumn
	// OpRenameColumn renames OldColumn → Column. Metadata-only in
	// Iceberg but unsafe semantically (downstream readers break).
	OpRenameColumn
	// OpWidenType changes Column's type from OldType to Type where
	// Type is a widening (int→long, float→double). Safe in both
	// formats.
	OpWidenType
	// OpNarrowType changes Column's type from OldType to Type where
	// Type is a narrowing (long→int, double→float). Overflow risk;
	// always unsafe.
	OpNarrowType
	// OpSetNotNull flips Column from nullable to NOT NULL. Requires
	// a scan-and-validate; always unsafe.
	OpSetNotNull
	// OpSetNullable flips Column from NOT NULL to nullable. Metadata-
	// only; safe.
	OpSetNullable
)

// Schema is the migrate-local view of a table's persisted shape,
// independent of the richer structs.LakeSchema. Contains only what Diff
// and Generate actually need.
type Schema struct {
	TableName string
	Fields    []Field
}

// Field is the migrate-local view of one column. Callers populate
// GoType for widen/narrow classification; Nullable / PK / MergeKey
// drive the corresponding rule-table entries.
type Field struct {
	Column   string
	GoType   reflect.Type
	Nullable bool
	PK       bool
	MergeKey bool
	Ignored  bool
}

// Change is one semantic modification to a table's schema. Diff
// produces a slice of these; Classify labels each; Generate renders
// each into SQL.
type Change struct {
	Op         Op
	Table      string  // fully-qualified table name
	Column     string  // the column being added / dropped / modified
	OldColumn  string  // for OpRenameColumn, the pre-rename name
	Type       string  // target SQL type for Add / Widen / Narrow
	OldType    string  // prior SQL type for Widen / Narrow
	Nullable   bool    // target nullability for Add / SetNullable
	HasDefault bool    // true when Add carries a non-NULL default
	Default    string  // literal SQL for the default value, when HasDefault
	Schema     *Schema // for OpCreateTable, the full target schema
}

// Verdict is Classify's output: whether the change is destructive
// plus a human-readable reason the generator uses as an informational
// comment in the migration file. There is no machine-enforced
// acknowledgement gate — the file on disk is the contract the
// reviewer signed off on, not a structured ack.
type Verdict struct {
	// Destructive is true for operations that can lose data, break
	// downstream readers, or require a full table scan to apply
	// safely. Reviewers see a `-- DESTRUCTIVE: <Reason>` comment in
	// the generated file and decide whether to keep the change.
	Destructive bool

	// Reason is a short rationale for why the change is destructive.
	// Empty for non-destructive changes.
	Reason string
}

// Classify returns the dialect-aware verdict for a change. The rule
// table applies across Iceberg and Delta identically at v0; dialect
// name is accepted for forward compatibility when specific formats
// need a narrower rule (e.g. Delta's column-mapping mode loosens
// rename/drop constraints).
//
// Unknown ops classify as destructive-with-reason so the reviewer
// sees an unambiguous signal rather than silent pass-through.
func ClassifyOp(c Change, _ string) Verdict {
	switch c.Op {
	case OpAddColumn:
		if !c.Nullable {
			return Verdict{Destructive: true, Reason: "NOT NULL add without default requires a full-table scan"}
		}
		if c.HasDefault {
			return Verdict{Destructive: true, Reason: "default value is a semantic choice, not a schema shape — review the default"}
		}
		return Verdict{}
	case OpDropColumn:
		return Verdict{Destructive: true, Reason: "data loss; downstream readers may break"}
	case OpRenameColumn:
		return Verdict{Destructive: true, Reason: "downstream readers may break on the renamed column"}
	case OpWidenType:
		return Verdict{}
	case OpNarrowType:
		return Verdict{Destructive: true, Reason: "overflow risk; requires a full-table scan to validate"}
	case OpSetNotNull:
		return Verdict{Destructive: true, Reason: "requires scan-and-validate; rows with existing NULLs will fail"}
	case OpSetNullable:
		return Verdict{}
	case OpCreateTable:
		return Verdict{}
	default:
		return Verdict{Destructive: true, Reason: "operation not recognised by classifier"}
	}
}
