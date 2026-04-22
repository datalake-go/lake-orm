package lakeorm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/datalake-go/lake-orm/internal/migrations"
	"github.com/datalake-go/lake-orm/structs"
)

// MigrateGenerate writes one goose-format .sql file per struct with
// pending changes into dir, plus a lakeorm.sum manifest at the dir
// root. Does not execute the migrations — that's lake-goose's job
// against the Spark Connect database/sql driver.
//
// Destructive operations (DROP COLUMN, RENAME COLUMN, type narrowing,
// NOT-NULL tightening) land in the file with a `-- DESTRUCTIVE:
// <reason>` informational comment. Reviewers see the comments in the
// PR diff and decide; there is no machine-enforced acknowledgement
// gate — the file on disk is the contract the reviewer signed off on.
//
// lakeorm.sum is overwritten on every call: line 1 is
// `h1:<sha256 of remaining lines>`, each subsequent line is
// `<filename> h1:<sha256 of file contents>`. CI / reviewers that
// verify the manifest detect post-generation edits with a single
// shell one-liner.
func (c *client) MigrateGenerate(ctx context.Context, dir string, models ...any) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lakeorm.MigrateGenerate: mkdir %s: %w", dir, err)
	}

	diffs, err := c.planLocalDiffs(ctx, dir, models)
	if err != nil {
		return nil, err
	}

	var written []string
	now := time.Now().UTC()
	for _, d := range diffs {
		if len(d.changes) == 0 {
			continue
		}
		fingerprint, _ := migrations.Fingerprint(d.schema.GoType)
		slug := migrations.SlugifyTable(d.schema.TableName)
		filename := filepath.Join(dir,
			fmt.Sprintf("%s_%s.sql", now.Format("20060102150405"), slug),
		)
		f, err := os.Create(filename)
		if err != nil {
			return written, fmt.Errorf("lakeorm.MigrateGenerate: create %s: %w", filename, err)
		}
		meta := migrations.GooseMigration{
			Source:      d.schema.GoType.String(),
			Fingerprint: fingerprint,
			DialectName: c.dialect.Name(),
			GeneratedAt: now,
			TargetState: d.target,
		}
		if err := migrations.GenerateGooseMigration(f, d.changes, meta); err != nil {
			_ = f.Close()
			return written, fmt.Errorf("lakeorm.MigrateGenerate: render %s: %w", filename, err)
		}
		if err := f.Close(); err != nil {
			return written, fmt.Errorf("lakeorm.MigrateGenerate: close %s: %w", filename, err)
		}
		written = append(written, filename)
	}

	if err := migrations.WriteManifest(dir); err != nil {
		return written, fmt.Errorf("lakeorm.MigrateGenerate: lakeorm.sum: %w", err)
	}

	return written, nil
}

// localDiff pairs a target schema with the changes lakeorm would
// generate a file for. target is the migration-flavoured view of the
// schema — the same one serialised into each file's State-JSON
// header so subsequent runs can replay it.
type localDiff struct {
	schema  *structs.LakeSchema
	target  *migrations.Schema
	changes []migrations.Change
}

// planLocalDiffs resolves every model to a LakeSchema, reconstructs
// the prior target state from the most recent migration file that
// declared the same table (Django's MigrationLoader replay pattern),
// and diffs against the new struct. Fresh tables fall through to a
// nil prior state and emit a bootstrap migration.
func (c *client) planLocalDiffs(_ context.Context, dir string, models []any) ([]localDiff, error) {
	out := make([]localDiff, 0, len(models))
	for _, s := range models {
		schema, err := structs.ParseSchema(reflect.TypeOf(s))
		if err != nil {
			return nil, fmt.Errorf("lakeorm.migrate: parse %T: %w", s, err)
		}
		target := migrations.FromLakeSchema(schema)

		var prior *migrations.Schema
		if dir != "" {
			prior, err = migrations.ReplayLatestState(dir, target.TableName)
			if err != nil {
				return nil, fmt.Errorf("lakeorm.migrate: replay prior state for %s: %w", target.TableName, err)
			}
		}

		changes := migrations.ComputeDiff(prior, target, c.dialect.Name())
		out = append(out, localDiff{schema: schema, target: target, changes: changes})
	}
	return out, nil
}
