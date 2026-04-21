package testutils

import (
	"sort"
	"testing"
	"time"

	"github.com/datalake-go/lake-orm/types"
)

// Factory holds per-test seed state: a unique ingest ID shared across
// related fixtures, a monotonically increasing row sequence number,
// and the *testing.T handle for fail-fast error reporting.
//
// One Factory per test. Generate related records through the same
// IngestID so tests can filter by "what this test produced" without
// cross-contamination from other parallel tests.
type Factory struct {
	IngestID types.SortableID
	t        *testing.T
	seq      int
}

// NewFactory creates a Factory bound to t. Each factory gets a fresh
// ingest ID so parallel tests don't collide.
func NewFactory(t *testing.T) *Factory {
	t.Helper()
	return &Factory{
		IngestID: types.NewSortableID(),
		t:        t,
	}
}

// NextSeq returns the next sequence number. Used by record
// generators that need a per-test-unique counter.
func (f *Factory) NextSeq() int {
	f.seq++
	return f.seq
}

// Now returns a microsecond-truncated timestamp. Spark / Iceberg /
// Delta store TIMESTAMP as microseconds (6-digit fractional seconds),
// so Go's nanosecond-precision time.Now round-trips with silent
// truncation — equality checks against the original Go value fail
// unless we truncate upfront.
func (f *Factory) Now() time.Time { return time.Now().Truncate(time.Microsecond) }

// SortedIDs returns n KSUIDs in guaranteed ascending lexicographic
// order. KSUIDs generated within the same second have random
// payload, so back-to-back NewSortableID calls are NOT ordered.
// Tests that need deterministic ordering (verifying "latest
// version" queries, pagination) should use this.
func SortedIDs(n int) []types.SortableID {
	ids := make([]types.SortableID, n)
	for i := range ids {
		ids[i] = types.NewSortableID()
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})
	return ids
}
