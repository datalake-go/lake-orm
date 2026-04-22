// Example: `_ingest_id` as a system-managed column for batch
// reconciliation.
//
// Run against the lake-k8s docker-compose stack:
//
//	make docker-up
//	go run ./examples/ingest-id
//
// lake-orm generates a UUIDv7 per Client.Insert call. It uses that
// ingest_id for three things:
//
//  1. Staging prefix key — <warehouse>/<ingest_id>/part-*.parquet.
//  2. Janitor cleanup — CleanupStaging parses the embedded ms
//     timestamp to detect orphaned prefixes older than a TTL.
//  3. Per-row batch identity — every row lake-orm writes carries
//     the ingest_id in its `_ingest_id` column. This unlocks
//     reconciliation queries ("show me what batch 14:23 landed"),
//     and is the filter predicate on the MERGE source when the
//     struct declares a mergeKey (Phase 2 of the system-column
//     work; tracked in issue #63).
//
// Note on declaring the column. `_ingest_id` is **system-managed**
// — user structs MUST NOT declare it. The column synthesises itself
// into every CREATE TABLE DDL lake-orm emits, and the driver layer
// populates it on write. Declaring a field that resolves to
// `_ingest_id` via any tag alias (lake / lakeorm / spark) errors
// at schema-parse time. Likewise the older `auto=ingestID` tag
// modifier is no longer accepted; anyone with legacy structs
// carrying it will see a migration error pointing at this design.
//
// To *read* the ingest_id back (e.g. for a reconciliation query),
// declare a separate projection struct containing a string field
// tagged `lake:"_ingest_id"`. The write-side struct stays clean.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	lakeorm "github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backends"
	"github.com/datalake-go/lake-orm/dialects/iceberg"
	"github.com/datalake-go/lake-orm/drivers/spark"
	"github.com/datalake-go/lake-orm/types"
)

// Event — write-side struct. No _ingest_id field; the column is
// system-managed. Notice there's no auto= tag for ingest_id either.
type Event struct {
	ID        types.SortableID `lake:"id,pk"`
	UserID    string           `lake:"user_id,required"`
	EventType string           `lake:"event_type,required"`
	CreatedAt time.Time        `lake:"created_at,auto=createTime"`
}

// EventWithIngestID — read-side projection struct. Adds the
// system column explicitly for reconciliation queries. CQRS shape:
// the read-side is a different struct from the write-side.
type EventWithIngestID struct {
	ID        types.SortableID `lake:"id"`
	UserID    string           `lake:"user_id"`
	EventType string           `lake:"event_type"`
	CreatedAt time.Time        `lake:"created_at"`
	IngestID  string           `lake:"_ingest_id"`
}

func main() {
	ctx := context.Background()

	store, err := backends.S3(envOr(
		"LAKEORM_S3_DSN",
		"s3://lakeorm-local/lake?endpoint=http://localhost:8333&path_style=true&access_key=lakeorm&secret_key=lakeorm",
	))
	if err != nil {
		log.Fatalf("backends.S3: %v", err)
	}
	db, err := lakeorm.Open(
		spark.Remote(envOr("LAKEORM_SPARK_URI", "sc://localhost:15002")),
		iceberg.Dialect(),
		store,
	)
	if err != nil {
		log.Fatalf("lakeorm.Open: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(ctx, &Event{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	// Two separate batches — each Insert call generates its own
	// ingest_id. Rows in the same batch share it; rows from
	// different batches don't.
	now := time.Now().Truncate(time.Microsecond)
	batchA := []*Event{
		{ID: types.NewSortableID(), UserID: "u1", EventType: "page_view", CreatedAt: now},
		{ID: types.NewSortableID(), UserID: "u1", EventType: "click", CreatedAt: now},
	}
	if err := db.Insert(ctx, batchA, lakeorm.ViaObjectStorage()); err != nil {
		log.Fatalf("insert batch A: %v", err)
	}
	batchB := []*Event{
		{ID: types.NewSortableID(), UserID: "u2", EventType: "page_view", CreatedAt: now},
	}
	if err := db.Insert(ctx, batchB, lakeorm.ViaObjectStorage()); err != nil {
		log.Fatalf("insert batch B: %v", err)
	}

	// Read back grouped by ingest_id. The read-side projection
	// struct explicitly includes _ingest_id — the write-side Event
	// struct doesn't, so `Query[Event]` would silently drop the
	// column.
	rows, err := lakeorm.Query[EventWithIngestID](ctx, db,
		`SELECT * FROM events ORDER BY _ingest_id, created_at`)
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	var last string
	for _, r := range rows {
		if r.IngestID != last {
			fmt.Printf("\nbatch _ingest_id=%s:\n", r.IngestID)
			last = r.IngestID
		}
		fmt.Printf("  %s  user=%s  %s\n", r.ID, r.UserID, r.EventType)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
