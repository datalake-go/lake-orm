package lakeorm

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/datalake-go/lake-orm/internal/migrations/goose"
)

// MigrateGenerate writes one goose-format .sql file per struct with
// pending changes into dir, plus an atlas.sum manifest at the dir
// root. Does not execute the migrations — that's lake-goose's job
// against the Spark Connect database/sql driver.
//
// Destructive operations (DROP COLUMN, RENAME COLUMN, type narrowing,
// NOT-NULL tightening) land in the file with a `-- DESTRUCTIVE:
// <reason>` informational comment. Reviewers see the comments in the
// PR diff and decide; there is no machine-enforced acknowledgement
// gate — the file on disk is the contract the reviewer signed off on.
//
// atlas.sum (Atlas-compatible format) is overwritten on every call:
// line 1 is `h1:<sha256 of remaining lines>`, each subsequent line is
// `<filename> h1:<sha256 of file contents>`. Downstream tooling that
// understands atlas.sum can detect post-generation edits.
func (c *client) MigrateGenerate(ctx context.Context, dir string, structs ...any) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lakeorm.MigrateGenerate: mkdir %s: %w", dir, err)
	}

	diffs, err := c.planLocalDiffs(ctx, dir, structs)
	if err != nil {
		return nil, err
	}

	var written []string
	now := time.Now().UTC()
	for _, d := range diffs {
		if len(d.changes) == 0 {
			continue
		}
		fingerprint, _ := SchemaFingerprint(d.schema.GoType)
		slug := slugifyTable(d.schema.TableName)
		filename := filepath.Join(dir,
			fmt.Sprintf("%s_%s.sql", now.Format("20060102150405"), slug),
		)
		f, err := os.Create(filename)
		if err != nil {
			return written, fmt.Errorf("lakeorm.MigrateGenerate: create %s: %w", filename, err)
		}
		meta := goose.GooseMigration{
			Source:      d.schema.GoType.String(),
			Fingerprint: fingerprint,
			DialectName: c.dialect.Name(),
			GeneratedAt: now,
			TargetState: d.target,
		}
		if err := goose.GenerateGooseMigration(f, d.changes, meta); err != nil {
			_ = f.Close()
			return written, fmt.Errorf("lakeorm.MigrateGenerate: render %s: %w", filename, err)
		}
		if err := f.Close(); err != nil {
			return written, fmt.Errorf("lakeorm.MigrateGenerate: close %s: %w", filename, err)
		}
		written = append(written, filename)
	}

	if err := writeAtlasSum(dir); err != nil {
		return written, fmt.Errorf("lakeorm.MigrateGenerate: atlas.sum: %w", err)
	}

	return written, nil
}

// localDiff pairs a target schema with the changes lakeorm would
// generate a file for. target is the migrate-flavoured view of the
// schema — the same one we serialise into each file's State-JSON
// header so subsequent runs can replay it.
type localDiff struct {
	schema  *LakeSchema
	target  *goose.Schema
	changes []goose.Change
}

// planLocalDiffs resolves every struct to a LakeSchema, reconstructs
// the prior target state from the most recent migration file that
// declared the same table (Django's MigrationLoader replay pattern),
// and diffs against the new struct. Fresh tables fall through to a
// nil prior state and emit a bootstrap migration.
//
// v1 can layer a driver-level DESCRIBE TABLE on top and
// cross-validate file-state vs catalog-state; the replay-from-files
// step stays even then, because "prior state" is what a future
// `lakeorm migrate --check` compares against to detect model drift.
func (c *client) planLocalDiffs(_ context.Context, dir string, structs []any) ([]localDiff, error) {
	out := make([]localDiff, 0, len(structs))
	for _, s := range structs {
		schema, err := ParseSchema(reflect.TypeOf(s))
		if err != nil {
			return nil, fmt.Errorf("lakeorm.migrate: parse %T: %w", s, err)
		}
		target := lakeSchemaToMigrateSchema(schema)

		var prior *goose.Schema
		if dir != "" {
			prior, err = goose.ReplayLatestState(dir, target.TableName)
			if err != nil {
				return nil, fmt.Errorf("lakeorm.migrate: replay prior state for %s: %w", target.TableName, err)
			}
		}

		changes := goose.Diff(prior, target, c.dialect.Name())
		out = append(out, localDiff{schema: schema, target: target, changes: changes})
	}
	return out, nil
}

// lakeSchemaToMigrateSchema adapts the reflection-heavy LakeSchema
// (pk/mergeKey indices, partitions, validators, auto-behaviours) to
// the narrower goose.Schema the generator actually needs. Lives on
// the lakeorm side so the migrate subpackage stays lakeorm-import-
// free.
//
// The system-managed SystemIngestIDColumn is appended last as a
// synthetic field. Tables created pre-Phase-1 of the system-column
// architecture (see issue #63) didn't carry it; the next
// MigrateGenerate run detects the gap and emits
// ALTER TABLE ... ADD COLUMN _ingest_id STRING. Nullable for backfill
// safety — old rows stay NULL, new rows get stamped at write time.
func lakeSchemaToMigrateSchema(s *LakeSchema) *goose.Schema {
	if s == nil {
		return nil
	}
	pk := map[int]bool{}
	for _, i := range s.PrimaryKeys {
		pk[i] = true
	}
	mk := map[int]bool{}
	for _, i := range s.MergeKeys {
		mk[i] = true
	}
	fields := make([]goose.Field, 0, len(s.Fields)+1)
	for i, f := range s.Fields {
		fields = append(fields, goose.Field{
			Column:   f.Column,
			GoType:   f.Type,
			Nullable: f.IsNullable,
			PK:       pk[i],
			MergeKey: mk[i],
			Ignored:  f.Ignored,
		})
	}
	fields = append(fields, goose.Field{
		Column:   SystemIngestIDColumn,
		GoType:   reflect.TypeOf(""),
		Nullable: true,
	})
	return &goose.Schema{
		TableName: s.TableName,
		Fields:    fields,
	}
}

// slugifyTable normalises a table name for use in a migration
// filename: lowercase, non-alnum → underscore, collapsed.
func slugifyTable(name string) string {
	var b strings.Builder
	lastUnderscore := true
	for _, r := range strings.ToLower(name) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

// writeAtlasSum scans dir for .sql files, computes each file's
// sha256, and writes an atlas.sum manifest. Atlas's format:
//
//	h1:<base64(sha256(lines 2..N))>
//	<filename> h1:<base64(sha256(file-contents))>
//	...
//
// The first line's hash covers every subsequent line so a tamper
// anywhere — renaming a file, editing a file, reordering lines —
// invalidates the top-line hash. Downstream tooling that already
// reads atlas.sum (goose-ent, atlas itself, custom CI jobs) picks
// up the check without additional wiring.
func writeAtlasSum(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	type fileHash struct {
		name string
		hash string
	}
	var hashes []fileHash
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		full := filepath.Join(dir, e.Name())
		f, err := os.Open(full)
		if err != nil {
			return err
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			_ = f.Close()
			return err
		}
		_ = f.Close()
		hashes = append(hashes, fileHash{
			name: e.Name(),
			hash: "h1:" + base64.StdEncoding.EncodeToString(h.Sum(nil)),
		})
	}
	// Sort lexicographically so atlas.sum is stable across runs.
	sort.Slice(hashes, func(i, j int) bool { return hashes[i].name < hashes[j].name })

	// Body = filename + space + hash, one per line.
	var body strings.Builder
	for _, h := range hashes {
		fmt.Fprintf(&body, "%s %s\n", h.name, h.hash)
	}
	dirHash := sha256.Sum256([]byte(body.String()))

	var out strings.Builder
	fmt.Fprintf(&out, "h1:%s\n", base64.StdEncoding.EncodeToString(dirHash[:]))
	out.WriteString(body.String())

	return os.WriteFile(filepath.Join(dir, "atlas.sum"), []byte(out.String()), 0o644)
}
