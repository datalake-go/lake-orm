//go:build e2e

package teste2e

import (
	"context"
	"testing"
	"time"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/testutils"
	"github.com/datalake-go/lake-orm/types"
)

// Order is the adjacent write-side entity for the join test.
type Order struct {
	ID          types.SortableID `spark:"id,pk"                  validate:"required"`
	UserID      string           `spark:"user_id"                validate:"required"`
	AmountPence int64            `spark:"amount_pence"           validate:"required"`
	PlacedAt    time.Time        `spark:"placed_at,auto=createTime"`
}

// UserOrderTotal is the result-shape struct for the join below. It
// has no persisted analogue — it's the read-side output contract.
// The spark:"..." tags bind projected columns to fields.
type UserOrderTotal struct {
	UserID     string `spark:"user_id"`
	Email      string `spark:"email"`
	OrderCount int64  `spark:"order_count"`
	TotalPence int64  `spark:"total_pence"`
}

// TestE2E_Join_TypedScan covers the CQRS-style read path end-to-end:
// write via dorm-tagged entities, read via SQL join + unwrap the
// underlying sparksql.DataFrame + sparksql.Collect[T] into a
// spark-tagged result-shape struct.
func TestE2E_Join_TypedScan(t *testing.T) {
	db := openClientForE2E(t)
	if db == nil {
		return
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.Migrate(ctx, &User{}, &Order{}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	f := testutils.NewFactory(t)
	alice := &User{ID: types.NewSortableID(), Email: "alice@example.com", Country: "UK", CreatedAt: f.Now()}
	bob := &User{ID: types.NewSortableID(), Email: "bob@example.com", Country: "US", CreatedAt: f.Now()}
	users := []*User{alice, bob}
	if err := lakeorm.Validate(users); err != nil {
		t.Fatalf("Validate users: %v", err)
	}
	if err := db.Insert(ctx, users, lakeorm.ViaObjectStorage()); err != nil {
		t.Fatalf("Insert users: %v", err)
	}

	orders := []*Order{
		{ID: types.NewSortableID(), UserID: string(alice.ID), AmountPence: 2500, PlacedAt: f.Now()},
		{ID: types.NewSortableID(), UserID: string(alice.ID), AmountPence: 1750, PlacedAt: f.Now()},
		{ID: types.NewSortableID(), UserID: string(bob.ID), AmountPence: 9999, PlacedAt: f.Now()},
	}
	if err := lakeorm.Validate(orders); err != nil {
		t.Fatalf("Validate orders: %v", err)
	}
	if err := db.Insert(ctx, orders, lakeorm.ViaObjectStorage()); err != nil {
		t.Fatalf("Insert orders: %v", err)
	}

	const sql = `
		SELECT
			u.id                AS user_id,
			u.email             AS email,
			COUNT(o.id)         AS order_count,
			SUM(o.amount_pence) AS total_pence
		FROM users u
		LEFT JOIN orders o ON o.user_id = u.id
		WHERE u.id IN (?, ?)
		GROUP BY u.id, u.email`

	df, err := db.DataFrame(ctx, sql, string(alice.ID), string(bob.ID))
	if err != nil {
		t.Fatalf("DataFrame: %v", err)
	}

	results, err := lakeorm.CollectAs[UserOrderTotal](ctx, df)
	if err != nil {
		t.Fatalf("CollectAs: %v", err)
	}

	byEmail := map[string]UserOrderTotal{}
	for _, r := range results {
		byEmail[r.Email] = r
	}
	if got := byEmail["alice@example.com"]; got.OrderCount != 2 || got.TotalPence != 4250 {
		t.Errorf("alice totals: got %+v, want order_count=2 total_pence=4250", got)
	}
	if got := byEmail["bob@example.com"]; got.OrderCount != 1 || got.TotalPence != 9999 {
		t.Errorf("bob totals: got %+v, want order_count=1 total_pence=9999", got)
	}
}

// TestE2E_Stream_TypedQuery pins the straight streaming read against
// the same stack — the constant-memory iter.Seq2 path users reach
// for when the result doesn't need a join.
func TestE2E_Stream_TypedQuery(t *testing.T) {
	db := openClientForE2E(t)
	if db == nil {
		return
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.Migrate(ctx, &User{}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	f := testutils.NewFactory(t)
	users := []*User{
		{ID: types.NewSortableID(), Email: "stream-alice@example.com", Country: "UK", CreatedAt: f.Now()},
		{ID: types.NewSortableID(), Email: "stream-bob@example.com", Country: "UK", CreatedAt: f.Now()},
	}
	if err := lakeorm.Validate(users); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := db.Insert(ctx, users, lakeorm.ViaObjectStorage()); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var seen int
	for u, err := range lakeorm.QueryStream[User](
		ctx, db,
		`SELECT * FROM users WHERE email LIKE ?`, "stream-%@example.com",
	) {
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		if u.Email == "" {
			t.Errorf("empty email on streamed row: %+v", u)
		}
		seen++
	}
	if seen < 2 {
		t.Errorf("streamed %d rows, want >= 2", seen)
	}
}
