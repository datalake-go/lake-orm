package goose

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestGenerate_NonDestructiveAddEmitsNoDestructiveComment(t *testing.T) {
	changes := []Change{
		{Op: OpAddColumn, Table: "users", Column: "tier", Type: "STRING", Nullable: true},
	}
	var buf bytes.Buffer
	err := GenerateGooseMigration(&buf, changes, GooseMigration{
		Source:      "models.User",
		Fingerprint: "sha256:abc",
		DialectName: "iceberg",
		GeneratedAt: time.Date(2026, 4, 19, 14, 30, 22, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		"-- lakeorm: generated 2026-04-19T14:30:22Z from models.User",
		"-- Struct fingerprint: sha256:abc",
		"-- Dialect: iceberg",
		"-- +goose Up",
		"ALTER TABLE users ADD COLUMN tier STRING;",
		"-- +goose Down",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("generated file missing %q; full output:\n%s", want, out)
		}
	}
	if strings.Contains(out, "DESTRUCTIVE:") {
		t.Errorf("non-destructive generation should not emit DESTRUCTIVE comments; got:\n%s", out)
	}
}

func TestGenerate_DestructiveRenameEmitsComment(t *testing.T) {
	changes := []Change{
		{Op: OpRenameColumn, Table: "users", OldColumn: "email_address", Column: "email"},
	}
	var buf bytes.Buffer
	if err := GenerateGooseMigration(&buf, changes, GooseMigration{DialectName: "iceberg"}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		"-- DESTRUCTIVE:",
		"downstream readers may break",
		"ALTER TABLE users RENAME COLUMN email_address TO email;",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("generated file missing %q; full output:\n%s", want, out)
		}
	}
}

func TestGenerate_MixedDiffNonDestructiveAndDestructive(t *testing.T) {
	// One file, both statements, both visible in review.
	changes := []Change{
		{Op: OpAddColumn, Table: "users", Column: "tier", Type: "STRING", Nullable: true},
		{Op: OpRenameColumn, Table: "users", OldColumn: "email_address", Column: "email"},
	}
	var buf bytes.Buffer
	if err := GenerateGooseMigration(&buf, changes, GooseMigration{DialectName: "iceberg"}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, "ADD COLUMN tier STRING") {
		t.Errorf("missing non-destructive add: %s", out)
	}
	if !strings.Contains(out, "RENAME COLUMN email_address TO email") {
		t.Errorf("missing destructive rename: %s", out)
	}
	if !strings.Contains(out, "-- DESTRUCTIVE:") {
		t.Errorf("mixed-diff should still flag the destructive statement: %s", out)
	}
}

func TestGenerate_SkipsCreateTable(t *testing.T) {
	// Bootstrap creates flow through db.Migrate, not the generator.
	changes := []Change{{Op: OpCreateTable, Table: "users"}}
	var buf bytes.Buffer
	if err := GenerateGooseMigration(&buf, changes, GooseMigration{DialectName: "iceberg"}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if strings.Contains(buf.String(), "CREATE TABLE") {
		t.Errorf("Generate should skip OpCreateTable; got:\n%s", buf.String())
	}
}
