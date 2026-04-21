// Example: the pure-Go embedded path. No Docker, no JVM, no network.
//
//	go run ./examples/duckdb
//
// Uses lake-orm's DuckDB driver against an in-memory DuckDB handle
// and lake-orm's in-memory backend. The same ORM surface — `lake:`
// tags, Migrate, Insert, Query[T], Stream, DataFrame escape hatch —
// works identically against this driver and the Spark-family
// drivers. Use this for unit / integration tests, local iteration,
// and single-process analytics workloads.
//
// CGO note: go-duckdb links against the DuckDB C++ library via CGO.
// Builds with CGO_ENABLED=0 will fail. Pre-compiled DuckDB binaries
// cover linux-amd64 / linux-arm64 / darwin-amd64 / darwin-arm64 /
// windows-amd64; exotic targets may need a custom build of DuckDB.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	// Register the "duckdb" database/sql driver.
	_ "github.com/marcboeker/go-duckdb/v2"

	lakeorm "github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backend"
	duckdbdialect "github.com/datalake-go/lake-orm/dialect/duckdb"
	duckdbdriver "github.com/datalake-go/lake-orm/driver/duckdb"
	"github.com/datalake-go/lake-orm/types"
)

// User is a typical tagged model. Same struct shape as the other
// examples — the driver choice doesn't change how models are
// declared.
type User struct {
	ID        types.SortableID `lake:"id,pk"`
	Email     string           `lake:"email,mergeKey,validate=email,required"`
	Country   string           `lake:"country,required"`
	CreatedAt time.Time        `lake:"created_at,auto=createTime"`
}

func main() {
	ctx := context.Background()

	// An empty DSN opens an in-memory DuckDB. Pass a filename for a
	// persistent on-disk database ("analytics.ddb" etc.).
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("sql.Open(duckdb): %v", err)
	}
	defer db.Close()

	// In-memory object-storage backend. Keeps the whole runtime on
	// one process with no filesystem writes.
	store := backend.Memory("duckdb-example")

	client, err := lakeorm.Open(
		duckdbdriver.Driver(db),
		duckdbdialect.Dialect(),
		store,
	)
	if err != nil {
		log.Fatalf("lakeorm.Open: %v", err)
	}
	defer client.Close()

	if err := client.Migrate(ctx, &User{}); err != nil {
		log.Fatalf("Migrate: %v", err)
	}

	now := time.Now().Truncate(time.Microsecond)
	users := []*User{
		{ID: types.NewSortableID(), Email: "alice@example.com", Country: "UK", CreatedAt: now},
		{ID: types.NewSortableID(), Email: "bob@example.com", Country: "UK", CreatedAt: now},
		{ID: types.NewSortableID(), Email: "carol@example.com", Country: "US", CreatedAt: now},
	}
	if err := lakeorm.Validate(users); err != nil {
		log.Fatalf("Validate: %v", err)
	}
	if err := client.Insert(ctx, users); err != nil {
		log.Fatalf("Insert: %v", err)
	}

	// Typed read via Query[T]. The generic materialisation path goes
	// through lake-orm's reflection scanner when the driver isn't
	// Spark — same API, different transport.
	ukUsers, err := lakeorm.Query[User](ctx, client,
		`SELECT id, email, country, created_at FROM users WHERE country = ? ORDER BY email`, "UK")
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	for _, u := range ukUsers {
		fmt.Printf("%s  %s  %s\n", u.ID, u.Email, u.Country)
	}

	// DataFrame escape hatch for joins / aggregates.
	df, err := client.DataFrame(ctx,
		`SELECT country, COUNT(*) AS n FROM users GROUP BY country ORDER BY n DESC`)
	if err != nil {
		log.Fatalf("DataFrame: %v", err)
	}
	n, _ := df.Count(ctx)
	fmt.Printf("%d distinct countries\n", n)
}
