// Example: the canonical `lake:"..."` tag.
//
// Run against the lake-k8s docker-compose stack:
//
//	make docker-up    # in the repo root
//	go run ./examples/lake-tag
//
// Three tag names are accepted and parse equivalently: `lake:"..."`,
// `lakeorm:"..."`, `spark:"..."`. Think of them the way a Go struct
// already carries `gorm:"..."` and `json:"..."` side by side — the
// ORM looks at one, the serializer / driver at another, and they
// don't have to agree on name. In lake-orm's case the three are
// synonyms; pick the one that reads best in the surrounding code.
//
//   - `lake:"..."`    — canonical. Recommended for new projects.
//   - `lakeorm:"..."` — written-out synonym. Good when a team has
//     other single-word tag keys (`db`, `json`) and wants the
//     library name to be unambiguous at a glance.
//   - `spark:"..."`   — historical driver-level tag. Kept because
//     the spark-connect-go fork also reads it; leaving it in place
//     means the same struct can be handed directly to a typed
//     `sparkconnect.Collect[T]` without a second set of tags.
//
// Mixing tag names ACROSS fields on the same struct is allowed.
// Mixing them on ONE field is a parse error (ErrInvalidTag) —
// catches the "which is the source of truth?" ambiguity up front.
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

// User uses the canonical `lake:"..."` tag. This is the recommended
// form for new code.
type User struct {
	ID        types.SortableID `lake:"id,pk"`
	Email     string           `lake:"email,mergeKey,validate=email,required"`
	Country   string           `lake:"country,required"`
	CreatedAt time.Time        `lake:"created_at,auto=createTime"`
}

// LegacyOrder carries `spark:"..."` because it was declared before
// the `lake:` canonical name was introduced. It still parses — no
// migration required.
type LegacyOrder struct {
	ID          types.SortableID `spark:"id,pk"`
	UserID      string           `spark:"user_id,required"`
	AmountPence int64            `spark:"amount_pence,required"`
	PlacedAt    time.Time        `spark:"placed_at,auto=createTime"`
}

// Mixed is intentionally mixed: one field per tag name. Illustrates
// that the per-field rule is the constraint — not the per-struct
// rule. Useful during incremental migration.
type Mixed struct {
	ID    string `lake:"id,pk"`
	Email string `lakeorm:"email,required"`
	Note  string `spark:"note"`
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

	// All three struct shapes migrate + insert + read through the
	// same surface. The tag variation is invisible to the client.
	if err := db.Migrate(ctx, &User{}, &LegacyOrder{}, &Mixed{}); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	u := &User{
		ID:        types.NewSortableID(),
		Email:     "alice@example.com",
		Country:   "UK",
		CreatedAt: time.Now().Truncate(time.Microsecond),
	}
	if err := lakeorm.Validate(u); err != nil {
		log.Fatalf("validate: %v", err)
	}
	if err := db.Insert(ctx, []*User{u}, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert user: %v", err)
	}

	// Read back through the typed helper. Whether the struct is
	// tagged `lake:`, `lakeorm:`, or `spark:`, the scanner resolves
	// columns through the same precedence chain.
	rows, err := lakeorm.Query[User](ctx, db, `SELECT * FROM users`)
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	for _, r := range rows {
		fmt.Printf("%s  %s  %s\n", r.ID, r.Email, r.Country)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
