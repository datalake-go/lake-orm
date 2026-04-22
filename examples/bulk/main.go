// Example: the fast-path write — parquet through object storage
// with a Spark `INSERT ... SELECT FROM parquet.<staging>` landing.
//
//	make docker-up
//	go run ./examples/bulk
//
// Two write paths live inside lake-orm; this example is the one that
// makes the throughput claim on the landing page actually hold up:
//
//   - `lakeorm.ViaObjectStorage()` — THIS path. The Go process writes
//     a parquet part to the backend (S3 / GCS / file) through the
//     partition writer, then issues ONE Spark statement of the form
//     `INSERT INTO target SELECT * FROM parquet.<staging>`. Payload
//     never traverses Go — the library orchestrates but doesn't
//     relay the bytes. Pick this for anything beyond a few hundred
//     rows.
//
//   - `lakeorm.ViaGRPC()` — gRPC-direct Arrow IPC. A v0 stub today
//     (returns ErrNotImplemented); the target for small-batch
//     low-latency writes once the Spark Connect Arrow IPC upload
//     path stabilises.
//
// When to pick which is mostly about row count. gRPC-direct will be
// fine for sub-kb messages when it ships; ViaObjectStorage is
// unambiguously the right path for bulk ingest today.
//
// The example inserts 10,000 rows in one call. On the local
// docker-compose stack this lands in a few hundred ms because the
// bytes skip the Go process entirely — the parquet part goes
// straight from the partition writer into SeaweedFS, then Spark
// reads from there.
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

// Metric — shape picked to exercise a mix of types (timestamp,
// int64, float64, string). The parquet writer covers these natively.
type Metric struct {
	ID        types.SortableID `lake:"id,pk"`
	IngestID  string           `lake:"ingest_id,auto=ingestID"`
	Source    string           `lake:"source,required,indexed"`
	Value     float64          `lake:"value,required"`
	SampledAt time.Time        `lake:"sampled_at,required"`
	CreatedAt time.Time        `lake:"created_at,auto=createTime"`
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

	if err := db.Migrate(ctx, &Metric{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	const N = 10_000
	rows := make([]*Metric, N)
	now := time.Now().Truncate(time.Microsecond)
	for i := range rows {
		rows[i] = &Metric{
			ID:        types.NewSortableID(),
			Source:    fmt.Sprintf("host-%03d", i%64),
			Value:     float64(i) * 0.5,
			SampledAt: now.Add(-time.Duration(i) * time.Second),
			CreatedAt: now,
		}
	}
	if err := lakeorm.Validate(rows); err != nil {
		log.Fatalf("validate: %v", err)
	}

	start := time.Now()
	if err := db.Insert(ctx, rows, lakeorm.ViaObjectStorage()); err != nil {
		log.Fatalf("insert: %v", err)
	}
	fmt.Printf("inserted %d rows via object storage in %s\n", N, time.Since(start))

	df, err := db.DataFrame(ctx, `SELECT COUNT(*) AS n FROM metrics`)
	if err != nil {
		log.Fatalf("DataFrame: %v", err)
	}
	n, err := df.Count(ctx)
	if err != nil {
		log.Fatalf("Count: %v", err)
	}
	fmt.Printf("metrics table now contains %d rows\n", n)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
