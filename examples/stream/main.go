// Example: streaming a large result back with constant memory.
//
// Run against the lake-k8s docker-compose stack:
//
//	make docker-up    # in the repo root
//	go run ./examples/stream
//
// What this demonstrates:
//
//   - lakeorm.Query[T].Stream returns iter.Seq2[*T, error], backed by
//     the Driver's streaming primitive — one Arrow record batch per
//     iteration, natural backpressure, constant memory regardless of
//     result size. This is SPARK-52780 (streaming reads) exposed as
//     a typed Go iterator.
//   - The loop terminates early on the first error; the iterator is
//     closed when the range exits.
//   - Stream is the right shape for straight SELECTs. For joins and
//     aggregates, see examples/joins — the result-shape struct
//     pattern and the `spark:"..."` tag scanner apply there too.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backend"
	"github.com/datalake-go/lake-orm/dialect/iceberg"
	"github.com/datalake-go/lake-orm/driver/spark"
	"github.com/datalake-go/lake-orm/types"
)

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

	store, err := backend.S3(s3DSN)
	if err != nil {
		log.Fatalf("backend.S3: %v", err)
	}
	db, err := lakeorm.Open(spark.Remote(sparkURI), iceberg.Dialect(), store)
	if err != nil {
		log.Fatalf("lakeorm.Open: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(ctx, &User{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	// Seed a handful of rows so there's something to stream back.
	now := time.Now().Truncate(time.Microsecond)
	users := []*User{
		{ID: types.NewSortableID(), Email: "alice@example.com", Country: "UK", CreatedAt: now},
		{ID: types.NewSortableID(), Email: "bob@example.com", Country: "UK", CreatedAt: now},
		{ID: types.NewSortableID(), Email: "carol@example.com", Country: "US", CreatedAt: now},
	}
	if err := lakeorm.Validate(users); err != nil {
		log.Fatalf("validate: %v", err)
	}
	if err := db.Insert(ctx, users, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert: %v", err)
	}

	// Streaming range — constant memory regardless of how many rows
	// match. Break early and the iterator cleans up the underlying
	// gRPC stream. SQL (including ORDER BY) is explicit; there's no
	// chainable builder layer.
	var count int
	for user, err := range lakeorm.QueryStream[User](
		ctx, db,
		`SELECT * FROM users WHERE country = ? ORDER BY created_at DESC`,
		"UK",
	) {
		if err != nil {
			log.Fatalf("stream: %v", err)
		}
		count++
		fmt.Printf("row %d: %s %s\n", count, user.ID, user.Email)
	}
	fmt.Printf("streamed %d rows\n", count)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
