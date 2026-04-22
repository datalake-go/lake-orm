package goose

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// GooseMigration carries the header comment fields for a generated
// migration file.
type GooseMigration struct {
	Source      string    // e.g. "models.User" or "cmd/api/models.go"
	Fingerprint string    // output of lakeorm.SchemaFingerprint for the target struct
	DialectName string    // "iceberg" | "delta"
	GeneratedAt time.Time // embedded in the header; zero → time.Now()

	// TargetState is the full table schema AFTER every Up statement
	// in this file is applied. Generate serialises it as a single-
	// line State-JSON comment in the header so subsequent replays
	// can reconstruct the prior state without a DESCRIBE TABLE —
	// the Django MigrationLoader pattern adapted to our goose-style
	// output. Optional; empty TargetState omits the State-JSON line.
	TargetState *Schema
}

// Generate emits a goose-format .sql file for the given changes
// into w. Format compatibility:
//
//   - `-- +goose Up` / `-- +goose Down` blocks (lake-goose reads
//     these; the format is directly compatible with pressly/goose).
//   - Each destructive statement is preceded by a
//     `-- DESTRUCTIVE: <reason>` comment. There is no machine-
//     enforced ack — reviewers see the comment in the PR diff and
//     decide whether to keep the statement. The file on disk is the
//     contract.
//   - The Down block is informational — lakehouse engines don't
//     execute Down for schema migrations; forward-migrate to roll
//     back.
//
// OpCreateTable changes are skipped; bootstrap creates go through
// db.Migrate's CREATE TABLE IF NOT EXISTS path, not through
// generated ALTER files.
func GenerateGooseMigration(w io.Writer, changes []Change, meta GooseMigration) error {
	if meta.GeneratedAt.IsZero() {
		meta.GeneratedAt = time.Now().UTC()
	}

	fmt.Fprintf(w, "-- lakeorm: generated %s", meta.GeneratedAt.UTC().Format(time.RFC3339))
	if meta.Source != "" {
		fmt.Fprintf(w, " from %s", meta.Source)
	}
	fmt.Fprintln(w)
	if meta.Fingerprint != "" {
		fmt.Fprintf(w, "-- Struct fingerprint: %s\n", meta.Fingerprint)
	}
	if meta.DialectName != "" {
		fmt.Fprintf(w, "-- Dialect: %s\n", meta.DialectName)
	}
	if meta.TargetState != nil {
		if line, err := EncodeState(meta.TargetState); err == nil && line != "" {
			fmt.Fprintln(w, line)
		}
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- +goose Up")
	fmt.Fprintln(w, "--")
	fmt.Fprintln(w, "-- NOTE ON SCHEMA EVOLUTION:")
	fmt.Fprintln(w, "-- lake-orm does not encourage schema evolution. The library")
	fmt.Fprintln(w, "-- emits these ALTERs so downstream tooling can apply them,")
	fmt.Fprintln(w, "-- but the idiomatic flow is to define a stable schema up")
	fmt.Fprintln(w, "-- front and clean data upstream before it lands. Schema")
	fmt.Fprintln(w, "-- evolution in a lakehouse is a tax on every downstream")
	fmt.Fprintln(w, "-- consumer and a common source of silent-wrong data —")
	fmt.Fprintln(w, "-- reviewers should treat DESTRUCTIVE lines here as a prompt")
	fmt.Fprintln(w, "-- to rethink the model, not as a routine operation. See the")
	fmt.Fprintln(w, "-- Philosophy section of the lake-orm README.")
	fmt.Fprintln(w)

	for i, c := range changes {
		if c.Op == OpCreateTable {
			continue
		}
		v := Classify(c, meta.DialectName)
		if v.Destructive {
			fmt.Fprintf(w, "-- DESTRUCTIVE: %s\n", v.Reason)
		}
		fmt.Fprintln(w, renderStatement(c))
		if i != len(changes)-1 {
			fmt.Fprintln(w)
		}
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "-- +goose Down")
	fmt.Fprintln(w, "-- Lakehouse engines don't execute Down blocks for")
	fmt.Fprintln(w, "-- schema migrations; forward-migrate to roll back.")
	fmt.Fprintln(w, "-- This block is informational.")

	return nil
}

// renderStatement emits the ALTER TABLE statement for a single
// Change. The dialect-specific polish (Iceberg's TIMESTAMPTZ, Delta's
// GENERATED ALWAYS) doesn't land here in v0 — both engines parse the
// same Spark SQL at the primitive level. Extending this is
// dialect-specific follow-up work.
func renderStatement(c Change) string {
	switch c.Op {
	case OpAddColumn:
		b := &strings.Builder{}
		fmt.Fprintf(b, "ALTER TABLE %s ADD COLUMN %s %s", c.Table, c.Column, c.Type)
		if !c.Nullable {
			b.WriteString(" NOT NULL")
		}
		if c.HasDefault {
			fmt.Fprintf(b, " DEFAULT %s", c.Default)
		}
		b.WriteByte(';')
		return b.String()
	case OpDropColumn:
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", c.Table, c.Column)
	case OpRenameColumn:
		return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", c.Table, c.OldColumn, c.Column)
	case OpWidenType, OpNarrowType:
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;", c.Table, c.Column, c.Type)
	case OpSetNotNull:
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", c.Table, c.Column)
	case OpSetNullable:
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", c.Table, c.Column)
	default:
		return fmt.Sprintf("-- unrecognised change: op=%d column=%s", c.Op, c.Column)
	}
}
