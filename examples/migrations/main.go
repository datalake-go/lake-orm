// Example: Django-style offline migration authoring.
//
//	go run ./examples/migrations
//
// lake-orm splits schema change into two distinct phases on purpose:
//
//  1. AUTHORING — `MigrateGenerate` inspects your current structs,
//     replays the most-recent migration file's State-JSON header to
//     reconstruct the prior schema state, diffs, and writes one
//     goose-format `.sql` file per changed table. Destructive
//     operations (DROP COLUMN, type narrowing, NOT-NULL tightening)
//     land with a `-- DESTRUCTIVE: <reason>` comment for the
//     reviewer to eyeball in the PR.
//
//  2. APPLICATION — lake-goose (separate binary) runs those `.sql`
//     files against the Spark Connect database/sql driver in order,
//     with the atlas.sum manifest verifying no post-generation
//     edits slipped in.
//
// Treating authoring and application separately is the Django /
// Alembic model and the one that scales across teams: the migration
// file is the contract PR reviewers sign off on, not the live diff.
//
// This example shows the authoring side. It does NOT need a Spark
// Connect cluster to run — MigrateGenerate operates purely on struct
// reflection and local file state.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backend"
	"github.com/datalake-go/lake-orm/dialect/iceberg"
	"github.com/datalake-go/lake-orm/driver/spark"
	"github.com/datalake-go/lake-orm/types"
)

// V1 schema — the struct as it exists at the first deployment.
type UserV1 struct {
	ID        types.SortableID `lake:"id,pk"`
	Email     string           `lake:"email,required"`
	CreatedAt time.Time        `lake:"created_at,auto=createTime"`
}

// V2 schema — later deployment adds Country (additive, safe) and
// renames Email → PrimaryEmail (DESTRUCTIVE; reviewer must confirm).
//
// In a real repo you'd replace UserV1 with UserV2 in place and run
// MigrateGenerate; here both live side-by-side so the example is
// self-contained.
type UserV2 struct {
	ID           types.SortableID `lake:"id,pk"`
	PrimaryEmail string           `lake:"primary_email,required"`
	Country      string           `lake:"country,required"`
	CreatedAt    time.Time        `lake:"created_at,auto=createTime"`
}

func main() {
	ctx := context.Background()

	outDir, err := os.MkdirTemp("", "lake-orm-migrations-*")
	if err != nil {
		log.Fatalf("mkdtemp: %v", err)
	}
	defer os.RemoveAll(outDir)

	// The driver/backend are required by Open but MigrateGenerate is
	// purely offline — no Spark calls happen in this example.
	// spark.Remote accepts a URL that is never dialled until the
	// first Driver.Execute, so the memory-backed backend + unreachable
	// host is a perfectly valid authoring-only setup.
	store := backend.Memory("migrations-example")
	db, err := lakeorm.Open(
		spark.Remote("sc://unused:15002"),
		iceberg.Dialect(),
		store,
	)
	if err != nil {
		log.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// First generation — bootstrap migration for UserV1.
	files, err := db.MigrateGenerate(ctx, outDir, &UserV1{})
	if err != nil {
		log.Fatalf("MigrateGenerate UserV1: %v", err)
	}
	fmt.Println("V1 migration files:")
	for _, f := range files {
		fmt.Println("  " + filepath.Base(f))
	}

	// Second generation — diff UserV2 against the V1 state recorded
	// in the previous file's State-JSON header. Emits an ALTER TABLE
	// migration with DESTRUCTIVE comments on the rename.
	files, err = db.MigrateGenerate(ctx, outDir, &UserV2{})
	if err != nil {
		log.Fatalf("MigrateGenerate UserV2: %v", err)
	}
	fmt.Println("V2 migration files:")
	for _, f := range files {
		fmt.Println("  " + filepath.Base(f))
	}

	// Show atlas.sum — lines covering every file + the manifest
	// hash. Downstream tooling can detect post-generation edits
	// by re-hashing and comparing.
	sum, err := os.ReadFile(filepath.Join(outDir, "atlas.sum"))
	if err != nil {
		log.Fatalf("read atlas.sum: %v", err)
	}
	fmt.Println("\natlas.sum:\n" + string(sum))
}
