// Example: writing against a Delta Lake table (instead of Iceberg).
//
// Run against the lake-k8s docker-compose stack with a Delta-enabled
// Spark Connect server:
//
//	make docker-up            # in the repo root
//	go run ./examples/delta
//
// This example is identical to examples/basic except for one line —
// the Dialect chosen at `lakeorm.Open`. That single swap changes
// the DDL grammar, the MERGE shape, and the partitioning translation
// the library emits under the hood; the struct tags, the write
// helpers, and the typed read helpers stay the same.
//
// Pluggable dialects are a deliberate feature: lake-orm refuses to
// pick a format for you. If your warehouse is Iceberg, use
// iceberg.Dialect(). If it's Delta, use delta.Dialect(). Everything
// else is a re-export.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backend"
	"github.com/datalake-go/lake-orm/dialect/delta"
	"github.com/datalake-go/lake-orm/driver/spark"
	"github.com/datalake-go/lake-orm/types"
)

// Event — a typical append-heavy Delta workload shape. Partitioning
// by event_date uses raw partitioning (the column value IS the
// partition key); for Iceberg-style bucketing / truncation see the
// `partition=bucket:N` / `partition=truncate:N` modifiers.
type Event struct {
	ID        types.SortableID `lake:"id,pk"`
	UserID    string           `lake:"user_id,required"`
	EventType string           `lake:"event_type,required,indexed"`
	EventDate string           `lake:"event_date,required,partition=raw"`
	Payload   string           `lake:"payload,json"`
	CreatedAt time.Time        `lake:"created_at,auto=createTime"`
}

func main() {
	ctx := context.Background()

	sparkURI := envOr("DORM_SPARK_URI", "sc://localhost:15002")
	s3DSN := envOr(
		"DORM_S3_DSN",
		"s3://dorm-local/lake?endpoint=http://localhost:8333&path_style=true&access_key=dorm&secret_key=dorm",
	)

	store, err := backend.S3(s3DSN)
	if err != nil {
		log.Fatalf("backend.S3: %v", err)
	}

	// One line swap versus examples/basic: delta.Dialect() instead
	// of iceberg.Dialect(). lake-orm now emits Delta DDL, Delta
	// MERGE, and Delta-compatible partition expressions.
	//
	// Optional tuning — Delta's deletion vectors and per-column
	// index strategies are exposed as dialect options for teams that
	// want to pin them explicitly rather than relying on cluster
	// defaults.
	db, err := lakeorm.Open(
		spark.Remote(sparkURI),
		delta.Dialect(
			delta.WithDeletionVectors(true),
			delta.WithIndexStrategy("event_type", delta.ZOrder),
		),
		store,
	)
	if err != nil {
		log.Fatalf("lakeorm.Open: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(ctx, &Event{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	now := time.Now().Truncate(time.Microsecond)
	events := []*Event{
		{
			ID:        types.NewSortableID(),
			UserID:    "u1",
			EventType: "page_view",
			EventDate: now.Format("2006-01-02"),
			Payload:   `{"path":"/home"}`,
			CreatedAt: now,
		},
		{
			ID:        types.NewSortableID(),
			UserID:    "u1",
			EventType: "click",
			EventDate: now.Format("2006-01-02"),
			Payload:   `{"target":"cta-signup"}`,
			CreatedAt: now,
		},
	}

	if err := lakeorm.Validate(events); err != nil {
		log.Fatalf("validate: %v", err)
	}
	if err := db.Insert(ctx, events, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert: %v", err)
	}

	// Read back. Typed via lake:"..." tags — the scanner treats
	// `lake`, `lakeorm`, `spark` equivalently.
	rows, err := lakeorm.Query[Event](ctx, db,
		`SELECT * FROM events WHERE event_date = ? AND event_type = ?`,
		now.Format("2006-01-02"), "page_view")
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	for _, r := range rows {
		fmt.Printf("%s  %s  %s\n", r.ID, r.EventType, r.Payload)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
