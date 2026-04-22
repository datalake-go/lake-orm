// Example: the typed helper API — Query[T] / QueryStream[T] /
// QueryFirst[T] and their DataFrame-shaped siblings CollectAs[T] /
// StreamAs[T] / FirstAs[T].
//
//	make docker-up
//	go run ./examples/typed-helpers
//
// lake-orm keeps the core DataFrame surface untyped on purpose —
// joins and aggregates produce row shapes that no single "write-side
// struct" can represent. The typed helpers are thin generic wrappers
// over that untyped core, applied at the materialisation edge where
// you declare the shape you want.
//
//   - Query[T]       — materialises the full result as []T.
//   - QueryStream[T] — iter.Seq2[T, error], one row at a time,
//     constant memory.
//   - QueryFirst[T]  — returns *T, nil if empty, error otherwise.
//   - CollectAs[T]   — the same for a DataFrame produced by
//     db.DataFrame(ctx, sql).
//   - StreamAs[T]    — streaming form of CollectAs.
//   - FirstAs[T]     — first row only.
//
// CollectAs / StreamAs / FirstAs exist for the CQRS join case: when
// you've dropped to a raw DataFrame to write the SQL for a join,
// you still want the result typed against a result-shape struct.
package main

import (
	"context"
	"errors"
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

// Write-side struct — one per persisted table.
type User struct {
	ID        types.SortableID `lake:"id,pk"`
	Email     string           `lake:"email,required"`
	Country   string           `lake:"country,required"`
	CreatedAt time.Time        `lake:"created_at,auto=createTime"`
}

// Read-side struct — one per projection. Has no persisted
// equivalent; only exists to shape the rows coming back from a
// particular query.
type CountryCount struct {
	Country string `lake:"country"`
	N       int64  `lake:"n"`
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

	if err := db.Migrate(ctx, &User{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	now := time.Now().Truncate(time.Microsecond)
	if err := db.Insert(ctx, []*User{
		{ID: types.NewSortableID(), Email: "a@example.com", Country: "UK", CreatedAt: now},
		{ID: types.NewSortableID(), Email: "b@example.com", Country: "UK", CreatedAt: now},
		{ID: types.NewSortableID(), Email: "c@example.com", Country: "US", CreatedAt: now},
	}, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert: %v", err)
	}

	// 1. Query[T] — full materialisation. Pick this when the result
	//    set is small (bounded by LIMIT or a narrow predicate) and
	//    the caller wants random access.
	users, err := lakeorm.Query[User](ctx, db,
		`SELECT * FROM users WHERE country = ? ORDER BY email`, "UK")
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	fmt.Printf("Query: got %d UK users\n", len(users))

	// 2. QueryStream[T] — iter.Seq2, constant memory. Pick this
	//    when the predicate might match many rows, or when the
	//    caller processes-and-discards each row.
	var n int
	for u, err := range lakeorm.QueryStream[User](ctx, db,
		`SELECT * FROM users ORDER BY created_at`) {
		if err != nil {
			log.Fatalf("stream: %v", err)
		}
		n++
		fmt.Printf("  stream row %d: %s\n", n, u.Email)
	}

	// 3. QueryFirst[T] — single-row convenience. Returns a typed
	//    ErrNoRows wrapper on empty result so the caller can
	//    errors.Is it instead of checking len == 0.
	firstUS, err := lakeorm.QueryFirst[User](ctx, db,
		`SELECT * FROM users WHERE country = ? ORDER BY created_at`, "US")
	switch {
	case errors.Is(err, lakeorm.ErrNoRows):
		fmt.Println("QueryFirst: no US users")
	case err != nil:
		log.Fatalf("QueryFirst: %v", err)
	default:
		fmt.Printf("QueryFirst: first US user is %s\n", firstUS.Email)
	}

	// 4. CollectAs[T] — the DataFrame flavour. Use when the SQL
	//    involves joins / aggregates and the rows don't line up
	//    with a persisted struct. The projection struct
	//    CountryCount is the contract.
	df, err := db.DataFrame(ctx,
		`SELECT country, COUNT(*) AS n FROM users GROUP BY country ORDER BY n DESC`)
	if err != nil {
		log.Fatalf("DataFrame: %v", err)
	}
	counts, err := lakeorm.CollectAs[CountryCount](ctx, df)
	if err != nil {
		log.Fatalf("CollectAs: %v", err)
	}
	for _, c := range counts {
		fmt.Printf("  country=%s n=%d\n", c.Country, c.N)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
