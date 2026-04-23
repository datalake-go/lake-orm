// Example: arbitrary native Spark query → typed Go struct.
//
// Run against the lake-k8s docker-compose stack:
//
//	make docker-up
//	go run ./examples/arbitrary_spark_query
//
// What this demonstrates — the CQRS read pattern lake-orm advocates
// taken to its conclusion:
//
//   - Writes still bind to the persisted Event type via Insert. One
//     lake-tagged struct per persisted table, exactly as before.
//   - Reads can take any shape the underlying engine produces. This
//     example drops to the raw Spark Connect DataFrame API, chains
//     a GroupBy + Agg + OrderBy in native Spark (not SQL), and then
//     hands the result DataFrame to lakeorm.Query[T] via
//     drv.FromDataFrame. The projection struct EventSummary is the
//     contract for the result shape — spark:"..." tags bind the
//     aggregated columns to Go fields.
//
// The same pattern covers anything sparksql.DataFrame can express:
// window functions, approximate percentiles, joins against
// DataFrames built from in-memory data, user-defined functions,
// and so on. lake-orm doesn't own the query grammar; it owns the
// typed decode path at the materialisation edge.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/datalake-go/spark-connect-go/spark/sql/functions"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backends"
	"github.com/datalake-go/lake-orm/dialects/iceberg"
	"github.com/datalake-go/lake-orm/drivers/spark"
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/types"
)

// Event — write-side persisted entity.
type Event struct {
	ID        types.SortableID `spark:"id,pk"`
	UserID    string           `spark:"user_id,required"`
	EventType string           `spark:"event_type,required"`
	CreatedAt time.Time        `spark:"created_at,auto=createTime"`
}

// EventSummary — read-side projection. Has no persisted analogue;
// exists only to shape the rows coming back from the aggregate below.
// The column names on the DataFrame side are what the spark:"..."
// tags bind against.
type EventSummary struct {
	EventType string `spark:"event_type"`
	N         int64  `spark:"n"`
}

func main() {
	ctx := context.Background()

	sparkURI := envOr("LAKEORM_SPARK_URI", "sc://localhost:15002")
	s3DSN := envOr(
		"LAKEORM_S3_DSN",
		"s3://lakeorm-local/lake?endpoint=http://localhost:8333&path_style=true&access_key=lakeorm&secret_key=lakeorm",
	)

	store, err := backends.S3(s3DSN)
	if err != nil {
		log.Fatalf("backends.S3: %v", err)
	}
	drv := spark.Remote(sparkURI)
	db, err := lakeorm.Open(drv, iceberg.Dialect(), store)
	if err != nil {
		log.Fatalf("lakeorm.Open: %v", err)
	}
	defer db.Close()

	// --- Write path ------------------------------------------------
	//
	// Persisted-entity writes go through Insert unchanged. lake-orm
	// owns the write grammar (Validate → Plan → Finalize); user code
	// only names the struct.
	if err := db.Migrate(ctx, &Event{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	now := time.Now().Truncate(time.Microsecond)
	events := []*Event{
		{ID: types.NewSortableID(), UserID: "u1", EventType: "page_view", CreatedAt: now},
		{ID: types.NewSortableID(), UserID: "u1", EventType: "click", CreatedAt: now},
		{ID: types.NewSortableID(), UserID: "u2", EventType: "page_view", CreatedAt: now},
		{ID: types.NewSortableID(), UserID: "u2", EventType: "page_view", CreatedAt: now},
		{ID: types.NewSortableID(), UserID: "u3", EventType: "click", CreatedAt: now},
	}
	if err := structs.Validate(events); err != nil {
		log.Fatalf("validate: %v", err)
	}
	if err := db.Insert(ctx, events, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert: %v", err)
	}

	// --- Read path -- arbitrary native Spark query -> typed Go -----
	//
	// Borrow a session from the driver's pool. The returned release
	// hook goes back through defer so the session returns to the pool
	// when this function exits, even if the DataFrame chain errors.
	sess, release, err := drv.Session(ctx)
	if err != nil {
		log.Fatalf("Session: %v", err)
	}
	defer release()

	// Build a native Spark DataFrame — no SQL string. The Spark side
	// lazily composes the operations; nothing executes until the
	// Convertible impl pulls rows at decode time.
	base, err := sess.Table("events")
	if err != nil {
		log.Fatalf("Table: %v", err)
	}
	grouped := base.GroupBy(functions.Col("event_type"))
	agg, err := grouped.Agg(ctx, functions.Count(functions.IntLit(1)).Alias("n"))
	if err != nil {
		log.Fatalf("Agg: %v", err)
	}

	// Hand the DataFrame to drv.FromDataFrame. The Convertible impl
	// on *spark.Driver iterates StreamRows under the hood and scans
	// each row into EventSummary via the spark:"..." tags. No
	// DataFrame/RowStream types leak into user code — the projection
	// struct is the contract, end to end.
	summary, err := lakeorm.Query[EventSummary](ctx, db, drv.FromDataFrame(agg))
	if err != nil {
		log.Fatalf("Query: %v", err)
	}

	fmt.Println("event_type  n")
	for _, s := range summary {
		fmt.Printf("%-10s  %d\n", s.EventType, s.N)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
