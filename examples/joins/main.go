// Example: reading joins and aggregates with CQRS-style output types.
//
// Run against the lake-k8s docker-compose stack:
//
//	make docker-up    # in the repo root
//	go run ./examples/joins
//
// What this demonstrates — the read-side pattern lakeorm
// advocates:
//
//   - Writes go through lake-tagged entities (User, Order below).
//     The struct is the source of truth for the persisted schema.
//   - Reads that involve joins or aggregates go through Query[T]
//     with a driver-native Source (drv.FromSQL here); the result
//     shape is declared on a purpose-built output struct carrying
//     `spark:"..."` tags.
//   - The output type is the *contract*, not a pre-declared wrapper.
//     One write-side struct per table, one read-side struct per
//     projection you care about. Unified ORM mappings collapse under
//     joins — we don't try to make them work.
//
// This separation (CQRS-style) is intentional.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/backends"
	"github.com/datalake-go/lake-orm/dialects/iceberg"
	"github.com/datalake-go/lake-orm/drivers/spark"
	"github.com/datalake-go/lake-orm/types"
)

// Write-side entity. Tag convention is spark:"<column>[,modifier...]".
type User struct {
	ID      types.SortableID `spark:"id,pk"`
	Email   string           `spark:"email,mergeKey,validate=email,required"`
	Country string           `spark:"country,required"`
}

// Write-side entity. One row per order; amount in the smallest
// currency unit so the numeric type is integer-precise.
type Order struct {
	ID          types.SortableID `spark:"id,pk"`
	UserID      string           `spark:"user_id,required"`
	AmountPence int64            `spark:"amount_pence,required"`
	PlacedAt    time.Time        `spark:"placed_at,auto=createTime"`
}

// Read-side output contract. `spark:"..."` tags bind result columns
// to fields — the fork's typed DataFrame scanner does the rest. This
// struct has no persisted analogue; it exists only as the shape we
// want rows to come back in.
type UserOrderTotal struct {
	UserID     string `spark:"user_id"`
	Email      string `spark:"email"`
	OrderCount int64  `spark:"order_count"`
	TotalPence int64  `spark:"total_pence"`
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

	// --- Write path -----------------------------------------------

	if err := db.Migrate(ctx, &User{}, &Order{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	alice := &User{ID: types.NewSortableID(), Email: "alice@example.com", Country: "UK"}
	bob := &User{ID: types.NewSortableID(), Email: "bob@example.com", Country: "US"}
	users := []*User{alice, bob}
	if err := structs.Validate(users); err != nil {
		log.Fatalf("validate users: %v", err)
	}
	if err := db.Insert(ctx, users, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert users: %v", err)
	}

	orders := []*Order{
		{ID: types.NewSortableID(), UserID: string(alice.ID), AmountPence: 2500, PlacedAt: time.Now().Truncate(time.Microsecond)},
		{ID: types.NewSortableID(), UserID: string(alice.ID), AmountPence: 1750, PlacedAt: time.Now().Truncate(time.Microsecond)},
		{ID: types.NewSortableID(), UserID: string(bob.ID), AmountPence: 9999, PlacedAt: time.Now().Truncate(time.Microsecond)},
	}
	if err := structs.Validate(orders); err != nil {
		log.Fatalf("validate orders: %v", err)
	}
	if err := db.Insert(ctx, orders, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert orders: %v", err)
	}

	// --- Read path -- join, aggregate, scan into output struct ----
	//
	// The SQL lives in one place (auditable, reviewable). The output
	// shape lives on the struct (compiler-checked, refactor-safe).
	// Neither tries to do the other's job.
	const sql = `
		SELECT
			u.id           AS user_id,
			u.email        AS email,
			COUNT(o.id)    AS order_count,
			SUM(o.amount_pence) AS total_pence
		FROM users u
		LEFT JOIN orders o ON o.user_id = u.id
		GROUP BY u.id, u.email
		ORDER BY total_pence DESC`

	// One-line typed scan via lakeorm.Query[T] + drv.FromSQL. The
	// spark:"..." tags on UserOrderTotal bind result columns to
	// fields; lake-orm's reflection scanner does the rest.
	results, err := lakeorm.Query[UserOrderTotal](ctx, db, drv.FromSQL(sql))
	if err != nil {
		log.Fatalf("query: %v", err)
	}

	for _, r := range results {
		fmt.Printf("%s  orders=%d  total=%d\n", r.Email, r.OrderCount, r.TotalPence)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
