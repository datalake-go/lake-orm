// Example: validation at the application boundary via
// go-playground/validator.
//
// Run against the lake-k8s docker-compose stack:
//
//	make docker-up
//	go run ./examples/validation
//
// lake-orm delegates validation to [go-playground/validator/v10] —
// the de facto Go library. Rules live on the standard
// `validate:"..."` struct tag; lake-orm calls `validator.Struct()`
// internally when `structs.Validate(records)` runs at the
// application boundary.
//
// This example shows:
//
//   - Built-in rules (required, email, oneof).
//   - A custom rule registered once at init via
//     `structs.Validator().RegisterValidation(name, fn)`.
//   - Unwrapping `error` → `validator.ValidationErrors` for
//     per-field inspection in an HTTP-handler-shaped response.
//
// [go-playground/validator/v10]: https://github.com/go-playground/validator
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"

	lakeorm "github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/backends"
	"github.com/datalake-go/lake-orm/dialects/iceberg"
	"github.com/datalake-go/lake-orm/drivers/spark"
	"github.com/datalake-go/lake-orm/types"
)

type User struct {
	ID       types.SortableID `lake:"id,pk"                   validate:"required"`
	Email    string           `lake:"email,mergeKey"          validate:"required,email"`
	Country  string           `lake:"country"                 validate:"required,oneof=UK US DE FR"`
	Postcode string           `lake:"postcode"                validate:"required,uk_postcode"`
	JoinedAt time.Time        `lake:"joined_at,auto=createTime"`
}

// ukPostcodeRE — loose regex. Real production code would use the
// office-of-national-statistics validation service; this is enough
// to demonstrate the custom-validator hook.
var ukPostcodeRE = regexp.MustCompile(`(?i)^[A-Z]{1,2}[0-9R][0-9A-Z]? ?[0-9][A-Z]{2}$`)

func init() {
	err := structs.Validator().RegisterValidation("uk_postcode", func(fl validator.FieldLevel) bool {
		return ukPostcodeRE.MatchString(strings.TrimSpace(fl.Field().String()))
	})
	if err != nil {
		log.Fatalf("register uk_postcode: %v", err)
	}
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

	// Happy path — passes.
	good := &User{
		ID:       types.NewSortableID(),
		Email:    "alice@example.com",
		Country:  "UK",
		Postcode: "SW1A 1AA",
		JoinedAt: time.Now().Truncate(time.Microsecond),
	}
	if err := structs.Validate(good); err != nil {
		log.Fatalf("unexpected validation failure: %v", err)
	}
	fmt.Println("validated: alice")

	// Deliberately broken — three failing rules. The returned
	// error unwraps to validator.ValidationErrors so callers can
	// build per-field HTTP-400 responses.
	bad := &User{
		ID:       types.NewSortableID(),
		Email:    "not-an-email",
		Country:  "ZZ",  // not in the oneof list
		Postcode: "ZZZ", // not a valid postcode
		JoinedAt: time.Now().Truncate(time.Microsecond),
	}
	err = structs.Validate(bad)
	var vErrs validator.ValidationErrors
	if errors.As(err, &vErrs) {
		for _, fe := range vErrs {
			// Rich FieldError surface: Field, Namespace, Tag,
			// Param, Value, ActualTag, Kind, Type — see the
			// go-playground/validator docs for the full set.
			fmt.Printf("field=%s rule=%s param=%q value=%v\n",
				fe.Field(), fe.Tag(), fe.Param(), fe.Value())
		}
	}
	// Example handler returns 400 here — don't proceed to Insert.

	// Insert only the validated record.
	if err := db.Insert(ctx, []*User{good}, lakeorm.ViaObjectStorage()); err != nil {
		log.Printf("insert: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
