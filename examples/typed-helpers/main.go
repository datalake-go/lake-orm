// Example: the typed read API — Query[T] / QueryStream[T] /
// QueryFirst[T] — built on the drivers.Convertible capability and
// per-driver From* conversion helpers.
//
//	make docker-up
//	go run ./examples/typed-helpers
//
// lake-orm separates writes from reads (CQRS). Writes bind to
// persisted types via Insert; reads run through a driver-native
// Source closure that the root Query[T] helpers decode into any
// result-shape struct — persisted or projection. Build the Source
// with the driver's own conversion helper (drv.FromSQL, drv.FromTable,
// drv.FromDataFrame, ...).
//
//   - Query[T]       — materialises the full result as []T.
//   - QueryStream[T] — iter.Seq2[T, error], one row at a time,
//     constant memory.
//   - QueryFirst[T]  — returns *T, nil if empty, error otherwise.
//
// The same three helpers cover the join / aggregate case: declare
// a result-shape struct (like CountryCount below) and feed it to
// Query[T]. No separate DataFrame surface; the projection struct
// is the contract.
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
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/types"
	lkerrors "github.com/datalake-go/lake-orm/errors"
)

// Write-side struct — one per persisted table.
type User struct {
	ID        types.SortableID `spark:"id,pk"`
	Email     string           `spark:"email,required"`
	Country   string           `spark:"country,required"`
	CreatedAt time.Time        `spark:"created_at,auto=createTime"`
}

// Read-side struct — one per projection. Has no persisted
// equivalent; only exists to shape the rows coming back from a
// particular query.
type CountryCount struct {
	Country string `spark:"country"`
	N       int64  `spark:"n"`
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
	drv := spark.Remote(envOr("LAKEORM_SPARK_URI", "sc://localhost:15002"))
	db, err := lakeorm.Open(drv, iceberg.Dialect(), store)
	if err != nil {
		log.Fatalf("lakeorm.Open: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(ctx, &User{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	now := time.Now().Truncate(time.Microsecond)
	seed := []*User{
		{ID: types.NewSortableID(), Email: "a@example.com", Country: "UK", CreatedAt: now},
		{ID: types.NewSortableID(), Email: "b@example.com", Country: "UK", CreatedAt: now},
		{ID: types.NewSortableID(), Email: "c@example.com", Country: "US", CreatedAt: now},
	}
	if err := structs.Validate(seed); err != nil {
		log.Fatalf("validate: %v", err)
	}
	if err := db.Insert(ctx, seed, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert: %v", err)
	}

	// 1. Query[T] — full materialisation. Pick this when the result
	//    set is small (bounded by LIMIT or a narrow predicate) and
	//    the caller wants random access.
	users, err := lakeorm.Query[User](ctx, db,
		drv.FromSQL(`SELECT * FROM users WHERE country = ? ORDER BY email`, "UK"))
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	fmt.Printf("Query: got %d UK users\n", len(users))

	// 2. QueryStream[T] — iter.Seq2, constant memory. Pick this
	//    when the predicate might match many rows, or when the
	//    caller processes-and-discards each row.
	var n int
	for u, err := range lakeorm.QueryStream[User](ctx, db,
		drv.FromSQL(`SELECT * FROM users ORDER BY created_at`)) {
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
		drv.FromSQL(`SELECT * FROM users WHERE country = ? ORDER BY created_at`, "US"))
	switch {
	case errors.Is(err, lkerrors.ErrNoRows):
		fmt.Println("QueryFirst: no US users")
	case err != nil:
		log.Fatalf("QueryFirst: %v", err)
	default:
		fmt.Printf("QueryFirst: first US user is %s\n", firstUS.Email)
	}

	// 4. Join / aggregate into a result-shape struct. Same Query[T]
	//    shape, different type — CountryCount is a projection that
	//    no persisted table has. The projection struct is the contract.
	counts, err := lakeorm.Query[CountryCount](ctx, db,
		drv.FromSQL(`SELECT country, COUNT(*) AS n FROM users GROUP BY country ORDER BY n DESC`))
	if err != nil {
		log.Fatalf("Query CountryCount: %v", err)
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
