// Example: the hero path for lakeorm.
//
// Run against the lake-k8s docker-compose stack:
//
//	make docker-up    # in the repo root
//	go run ./examples/basic
//
// Demonstrates:
//   - Struct tags as the schema contract
//   - lakeorm.Validate before any I/O (HTTP-handler pattern)
//   - Insert via the object-storage fast path (partition writer + Spark ingest)
//   - Streaming read with constant memory
//   - DataFrame escape hatch for native Spark operations
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backends"
	"github.com/datalake-go/lake-orm/dialects/iceberg"
	"github.com/datalake-go/lake-orm/drivers/spark"
	"github.com/datalake-go/lake-orm/types"
)

// User is a typical tagged model. The struct IS the schema contract —
// one `spark:"..."` tag per column. The library derives the parquet
// schema from these tags automatically (time.Time → TIMESTAMP(MICROS),
// pointer fields → optional, etc.), so no `parquet:"..."` duplication.
type User struct {
	ID        types.SortableID `spark:"id,pk"`
	Email     string           `spark:"email,mergeKey,validate=email,required"`
	Country   string           `spark:"country,required"`
	CreatedAt time.Time        `spark:"created_at,auto=createTime"`
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

	// The Iceberg catalog itself is configured on the Spark Connect
	// server (docker-compose / Helm chart) — Hadoop-catalog rooted
	// at the s3a:// warehouse. The Dialect just names it so queries
	// get qualified as <catalog>.<db>.<table>.
	db, err := lakeorm.Open(
		spark.Remote(sparkURI),
		iceberg.Dialect(),
		store,
	)
	if err != nil {
		log.Fatalf("lakeorm.Open: %v", err)
	}
	defer db.Close()

	// Migrate — idempotent CREATE TABLE IF NOT EXISTS derived from
	// the struct tags. Runs against the remote engine.
	if err := db.Migrate(ctx, &User{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	// Validate BEFORE any I/O. HTTP handlers should call this first
	// so bad data gets a synchronous 400 instead of an async pipeline
	// failure at 7pm.
	alice := &User{
		ID:        types.NewSortableID(),
		Email:     "alice@example.com",
		Country:   "UK",
		CreatedAt: time.Now().Truncate(time.Microsecond),
	}
	if err := lakeorm.Validate(alice); err != nil {
		log.Fatalf("validate: %v", err)
	}

	// Insert via the object-storage fast path — writes a parquet part
	// through backends.S3, then issues one INSERT ... SELECT FROM
	// parquet.`<staging>/*.parquet` on the Spark server. This is the
	// path lakeorm's throughput claim actually rides. The direct
	// gRPC path is a v0 stub (`lakeorm.ViaGRPC()` returns ErrNotImplemented).
	if err := db.Insert(ctx, []*User{alice}, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert: %v", err)
	}

	// Stream back with constant memory — one Arrow record batch per
	// iteration, range-able with Go 1.23's iter.Seq2. SQL lives in one
	// place (auditable, reviewable); the spark:"..." tags on User bind
	// result columns to fields.
	for user, err := range lakeorm.QueryStream[User](
		ctx, db, `SELECT * FROM users WHERE country = ?`, "UK",
	) {
		if err != nil {
			log.Printf("stream: %v", err)
			break
		}
		fmt.Printf("read: %s %s\n", user.ID, user.Email)
	}

	// Escape hatch — drop to raw DataFrame for Spark ops outside the
	// typed CRUD surface.
	df, err := db.DataFrame(ctx, `SELECT * FROM users WHERE country = ?`, "UK")
	if err != nil {
		log.Printf("dataframe: %v", err)
		return
	}
	n, _ := df.Count(ctx)
	fmt.Printf("count: %d\n", n)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
