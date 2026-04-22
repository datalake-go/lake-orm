// Example: lake-orm against a Databricks cluster via Databricks
// Connect (Spark Connect over a Databricks workspace).
//
// Three Databricks-adjacent drivers exist; this one is the one you
// want when your workload runs on an interactive cluster and you
// need the full Spark Connect DataFrame API:
//
//   - driver/spark              — generic Spark Connect (self-hosted,
//     EMR, Glue, lake-k8s).
//   - driver/databricksconnect  — THIS driver. Databricks Connect
//     (Spark Connect over Databricks,
//     OAuth M2M / PAT, cluster-ID header,
//     automatic token refresh).
//   - driver/databricks         — native SQL warehouse via BYO
//     `databricks-sql-go` *sql.DB. See
//     examples/databricks for that path.
//
// Required env (OAuth M2M — service-principal credentials):
//
//	DATABRICKS_WORKSPACE_URL   e.g. "https://acme.cloud.databricks.com"
//	DATABRICKS_CLIENT_ID       OAuth M2M client ID
//	DATABRICKS_CLIENT_SECRET   OAuth M2M client secret
//	DATABRICKS_CLUSTER_ID      e.g. "0123-456789-abcdef01"
//	WAREHOUSE_URI              the S3 / ABFSS warehouse root
//
// If you're still on PAT auth during migration, swap OAuthM2M for
// PAT below; rest of the code is identical.
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
	"github.com/datalake-go/lake-orm/dialects/delta"
	"github.com/datalake-go/lake-orm/drivers/databricksconnect"
	"github.com/datalake-go/lake-orm/types"
)

// User — canonical `lake:"..."` tags. Same struct would work against
// the generic spark driver or the native databricks driver; only the
// driver-construction line changes.
type User struct {
	ID        types.SortableID `lake:"id,pk"`
	Email     string           `lake:"email,mergeKey,validate=email,required"`
	Country   string           `lake:"country,required"`
	CreatedAt time.Time        `lake:"created_at,auto=createTime"`
}

func main() {
	ctx := context.Background()

	auth := databricksconnect.OAuthM2M{
		WorkspaceURL: mustEnv("DATABRICKS_WORKSPACE_URL"),
		ClientID:     mustEnv("DATABRICKS_CLIENT_ID"),
		ClientSecret: mustEnv("DATABRICKS_CLIENT_SECRET"),
		ClusterID:    mustEnv("DATABRICKS_CLUSTER_ID"),
		// 5-minute refresh buffer is the default; left explicit here
		// so the knob is discoverable.
		TokenRefreshBuffer: 5 * time.Minute,
	}

	drv := databricksconnect.Driver(auth)

	store, err := backends.S3(mustEnv("WAREHOUSE_URI"))
	if err != nil {
		log.Fatalf("backends.S3: %v", err)
	}

	// Delta is the native Databricks table format. Swap to
	// iceberg.Dialect() if your cluster is configured against an
	// Iceberg catalog (Uniform / REST catalog).
	db, err := lakeorm.Open(drv, delta.Dialect(), store)
	if err != nil {
		log.Fatalf("lakeorm.Open: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(ctx, &User{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	u := &User{
		ID:        types.NewSortableID(),
		Email:     "alice@example.com",
		Country:   "UK",
		CreatedAt: time.Now().Truncate(time.Microsecond),
	}
	if err := structs.Validate(u); err != nil {
		log.Fatalf("validate: %v", err)
	}
	if err := db.Insert(ctx, []*User{u}, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert: %v", err)
	}

	rows, err := lakeorm.Query[User](ctx, db,
		`SELECT * FROM users WHERE country = ?`, "UK")
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	for _, r := range rows {
		fmt.Printf("%s  %s  %s\n", r.ID, r.Email, r.Country)
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing env: %s", key)
	}
	return v
}
