// Example: lake-orm against a Databricks SQL warehouse using the
// native `databricks-sql-go` driver. Bring-your-own-connection —
// lake-orm doesn't own OAuth, warehouse selection, or connection
// pooling; you construct the *sql.DB exactly as your environment
// requires and hand it to driver/databricks.
//
// Required env:
//
//	DATABRICKS_WORKSPACE_HOST   e.g. "acme.cloud.databricks.com"
//	DATABRICKS_WAREHOUSE_ID     e.g. "0123456789abcdef"
//	DATABRICKS_CLIENT_ID        OAuth M2M client ID (service principal)
//	DATABRICKS_CLIENT_SECRET    OAuth M2M client secret
//	DATABRICKS_CATALOG          e.g. "main"
//	DATABRICKS_SCHEMA           e.g. "analytics"
//
// Authentication, session config, and connection-pool sizing are
// Databricks-specific knobs that every deployment tunes
// differently. The reference implementation in
// svc-data-platform-api/internal/datalake documents the set that
// works at production scale; this example is the minimum viable
// wiring.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	dbsqlclient "github.com/databricks/databricks-sql-go"
	dbsqlm2m "github.com/databricks/databricks-sql-go/auth/oauth/m2m"

	lakeorm "github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backends"
	"github.com/datalake-go/lake-orm/dialects/delta"
	"github.com/datalake-go/lake-orm/drivers/databricks"
)

// User — write-side entity. The same spark:"..." tag drives the
// scanner on the Databricks driver as it does on the Spark Connect
// driver. ORM code stays identical across the two.
type User struct {
	ID        string    `spark:"id,pk"`
	Email     string    `spark:"email,required"`
	Country   string    `spark:"country,required"`
	CreatedAt time.Time `spark:"created_at,auto=createTime"`
}

func main() {
	ctx := context.Background()

	host := mustEnv("DATABRICKS_WORKSPACE_HOST")
	warehouseID := mustEnv("DATABRICKS_WAREHOUSE_ID")
	clientID := mustEnv("DATABRICKS_CLIENT_ID")
	clientSecret := mustEnv("DATABRICKS_CLIENT_SECRET")
	catalog := mustEnv("DATABRICKS_CATALOG")
	schema := mustEnv("DATABRICKS_SCHEMA")

	// Build *sql.DB via databricks-sql-go. lake-orm does NOT
	// configure Databricks — this is the part every team tunes
	// differently (CloudFetch, session params, pool lifecycle).
	connector, err := dbsqlclient.NewConnector(
		dbsqlclient.WithServerHostname(host),
		dbsqlclient.WithHTTPPath("/sql/1.0/warehouses/"+warehouseID),
		dbsqlclient.WithAuthenticator(
			dbsqlm2m.NewAuthenticator(clientID, clientSecret, host),
		),
		dbsqlclient.WithInitialNamespace(catalog, schema),
		dbsqlclient.WithCloudFetch(true),
		dbsqlclient.WithMaxRows(500_000),
		dbsqlclient.WithTimeout(30*time.Minute),
		dbsqlclient.WithSessionParams(map[string]string{
			"ansi_mode": "false",
			"timezone":  "UTC",
		}),
	)
	if err != nil {
		log.Fatalf("databricks connector: %v", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(32)
	db.SetConnMaxLifetime(5 * time.Minute)
	defer db.Close()

	// Hand the *sql.DB to the lake-orm Databricks driver.
	drv := databricks.New(db)

	// Delta is the native Databricks table format.
	store, err := backends.S3(os.Getenv("WAREHOUSE_URI"))
	if err != nil {
		log.Fatalf("backends.S3: %v", err)
	}
	client, err := lakeorm.Open(drv, delta.Dialect(), store)
	if err != nil {
		log.Fatalf("lakeorm.Open: %v", err)
	}
	defer client.Close()

	// Same ORM surface as the Spark Connect drivers. Write-side
	// struct bound by spark:"..." tags, read-side through Query[T].
	users, err := lakeorm.Query[User](ctx, client,
		drv.FromSQL(`SELECT * FROM users WHERE country = ? LIMIT 50`, "UK"))
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	for _, u := range users {
		fmt.Printf("%s  %s  %s\n", u.ID, u.Email, u.Country)
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing env: %s", key)
	}
	return v
}
