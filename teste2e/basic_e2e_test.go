//go:build e2e

package teste2e

import (
	"context"
	"testing"
	"time"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/backend"
	"github.com/datalake-go/lake-orm/dialect/iceberg"
	"github.com/datalake-go/lake-orm/driver/spark"
	"github.com/datalake-go/lake-orm/testutils"
	"github.com/datalake-go/lake-orm/types"
)

// User is the canonical hero-path model. Shared across the e2e tests
// so everyone asserts against the same schema.
type User struct {
	ID        types.SortableID `spark:"id,pk"            validate:"required"`
	Email     string           `spark:"email,mergeKey"   validate:"required,email"`
	Country   string           `spark:"country"          validate:"required"`
	CreatedAt time.Time        `spark:"created_at,auto=createTime"`
}

// TestE2E_Migrate_InsertSmall_Stream is the minimum-viable e2e test:
// stand up the full stack, migrate the User table, insert a small
// batch through the gRPC path, stream it back.
//
// Skipped (not failed) when the Spark Connect testcontainer image
// isn't available — most CI setups won't have a pre-baked
// dorm-spark-connect image, and the alternative (stock apache/spark
// resolving JARs on first run) is too slow for routine CI. Run
// against a long-lived docker-compose stack by setting
// DORM_SPARK_URI and DORM_S3_DSN.
func TestE2E_Migrate_InsertSmall_Stream(t *testing.T) {
	db := openClientForE2E(t)
	if db == nil {
		return // openClientForE2E has already called t.Skip
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.Migrate(ctx, &User{}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	f := testutils.NewFactory(t)
	users := []*User{
		{ID: types.NewSortableID(), Email: "alice@example.com", Country: "UK", CreatedAt: f.Now()},
		{ID: types.NewSortableID(), Email: "bob@example.com", Country: "US", CreatedAt: f.Now()},
	}
	if err := lakeorm.Validate(users); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := db.Insert(ctx, users, lakeorm.ViaGRPC()); err != nil {
		t.Fatalf("Insert (gRPC): %v", err)
	}

	seen := map[string]bool{}
	for u, err := range lakeorm.QueryStream[User](
		ctx, db, `SELECT * FROM users WHERE country = ?`, "UK",
	) {
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		seen[u.Email] = true
	}
	if !seen["alice@example.com"] {
		t.Errorf("alice not streamed back; seen: %v", seen)
	}
}

// openClientForE2E resolves the (Spark, S3) endpoints from
// DORM_SPARK_URI / DORM_S3_DSN env vars (set by the developer-run
// docker-compose stack) or skips if unset. Testcontainer startup for
// Spark Connect is gated behind a future dorm-spark-connect image
// because stock apache/spark startup is too slow for CI.
func openClientForE2E(t *testing.T) lakeorm.Client {
	t.Helper()

	sparkURI := envOr(t, "DORM_SPARK_URI", "")
	s3DSN := envOr(t, "DORM_S3_DSN", "")

	if sparkURI == "" || s3DSN == "" {
		t.Skip("teste2e requires DORM_SPARK_URI and DORM_S3_DSN — run `make docker-up` and export the printed endpoints, or wait for the dorm-spark-connect testcontainer image to land")
	}

	store, err := backend.S3(s3DSN)
	if err != nil {
		t.Fatalf("backend.S3(%q): %v", s3DSN, err)
	}
	db, err := lakeorm.Open(
		spark.Remote(sparkURI),
		iceberg.Dialect(),
		store,
	)
	if err != nil {
		t.Fatalf("lakeorm.Open: %v", err)
	}
	return db
}
