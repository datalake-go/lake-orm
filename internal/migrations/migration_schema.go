package migrations

import (
	"reflect"

	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/types"
)

// structs.LakeSchema → migrations.Schema adapter.
//
// The two schema representations exist for separate reasons:
// LakeSchema is the reflection-heavy runtime view every dialect
// and driver reads, carrying reflect.Type / field index paths /
// partition specs / auto-behaviours. Schema (this package) is the
// narrower view the migration generator needs — column name,
// type, nullability, pk/mergeKey membership — plus the system-
// managed _ingest_id column appended as a synthetic field.
//
// Why the synthetic ingest_id field is appended here rather than
// on the LakeSchema side: tables created before the system-column
// work landed didn't carry _ingest_id. On the next MigrateGenerate
// run, replaying the prior State-JSON produces a Schema that omits
// the column; adapting the current LakeSchema through this function
// always includes it; ComputeDiff then emits
// ALTER TABLE ... ADD COLUMN _ingest_id STRING for the catch-up
// migration automatically. The migration path is data-driven
// rather than hand-written per-project.

// FromLakeSchema projects a structs.LakeSchema into the narrower
// migrations.Schema the Diff / Generate / State machinery consumes.
// The system-managed _ingest_id column is appended as a synthetic
// field; see the migration_schema.go concept comment for why.
func FromLakeSchema(s *structs.LakeSchema) *Schema {
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
	fields := make([]Field, 0, len(s.Fields)+1)
	for i, f := range s.Fields {
		fields = append(fields, Field{
			Column:   f.Column,
			GoType:   f.Type,
			Nullable: f.IsNullable,
			PK:       pk[i],
			MergeKey: mk[i],
			Ignored:  f.Ignored,
		})
	}
	fields = append(fields, Field{
		Column:   types.SystemIngestIDColumn,
		GoType:   reflect.TypeOf(""),
		Nullable: true,
	})
	return &Schema{
		TableName: s.TableName,
		Fields:    fields,
	}
}
