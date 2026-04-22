package lakeorm

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CleanupReport summarises CleanupStaging's pass. Deleted carries
// prefix URIs that were removed; Failed carries prefixes that
// matched the TTL cutoff but whose delete call errored — the
// operator can retry selectively.
type CleanupReport struct {
	Deleted []string
	Failed  []string
	Scanned int
}

// StagingPrefix is one entry a StagingLister backend yields. URI is
// the absolute backend URI; IngestID is the final path segment,
// which CleanupStaging parses as a UUIDv7.
type StagingPrefix struct {
	URI      string
	IngestID string
}

// NewStagingPrefix builds a StagingPrefix from its absolute backend
// URI and the trailing ingest_id path segment.
func NewStagingPrefix(uri, ingestID string) StagingPrefix {
	return StagingPrefix{URI: uri, IngestID: ingestID}
}

// StagingLister is the optional backend extension Client.CleanupStaging
// relies on. Backends that can enumerate their _staging/ namespace
// implement this; backends that can't leave it unimplemented and
// CleanupStaging returns ErrNotImplemented.
//
// Kept as a separate interface (not folded into Backend) so the
// Backend contract stays narrow: the fast-path write path only needs
// StagingPrefix / StagingLocation / CleanupStaging by-URI, and that's
// the minimum any backend must implement.
type StagingLister interface {
	ListStagingPrefixes(ctx context.Context) ([]StagingPrefix, error)
}

// CleanupStaging walks every prefix under the Backend's _staging/
// namespace, parses each prefix name as a UUIDv7, and deletes
// prefixes whose embedded timestamp is older than olderThan.
//
// Safe to run concurrently with ongoing Inserts: UUIDv7's time-
// sortability guarantees a prefix older than the TTL is not mid-
// flight (unless clock skew exceeds the TTL, which is pathological).
// Typical olderThan values: 24h for dev, 72h for production.
//
// Per INGEST_ID.md, non-UUIDv7 prefixes are skipped (not deleted) —
// the _staging/ namespace may legitimately contain other operator-
// managed content.
//
// Returns ErrNotImplemented wrapped if the configured Backend does
// not satisfy the StagingLister optional interface.
func (c *client) CleanupStaging(ctx context.Context, olderThan time.Duration) (*CleanupReport, error) {
	lister, ok := c.backend.(StagingLister)
	if !ok {
		return nil, fmt.Errorf("lakeorm.CleanupStaging: backend does not implement StagingLister: %w",
			ErrNotImplemented)
	}
	prefixes, err := lister.ListStagingPrefixes(ctx)
	if err != nil {
		return nil, fmt.Errorf("lakeorm.CleanupStaging: list staging: %w", err)
	}
	cutoff := time.Now().Add(-olderThan)
	report := &CleanupReport{Scanned: len(prefixes)}
	for _, p := range prefixes {
		id, perr := uuid.Parse(p.IngestID)
		if perr != nil || id.Version() != 7 {
			continue
		}
		if extractUUIDv7Timestamp(id).After(cutoff) {
			continue
		}
		if err := c.backend.CleanupStaging(ctx, p.URI); err != nil {
			report.Failed = append(report.Failed, p.URI)
			continue
		}
		report.Deleted = append(report.Deleted, p.URI)
	}
	return report, nil
}

// extractUUIDv7Timestamp reads the 48-bit unix_ts_ms header from a
// UUIDv7 per RFC 9562 and returns the embedded moment.
func extractUUIDv7Timestamp(id uuid.UUID) time.Time {
	// UUIDv7 layout: 48 bits unix_ts_ms, 4 bits version, 12 bits rand_a,
	// 2 bits variant, 62 bits rand_b. ms is the first 6 bytes,
	// big-endian.
	bytes := id[:6]
	// Big-endian 48-bit read, packed into an int64.
	ms := int64(binary.BigEndian.Uint32(bytes[:4]))<<16 |
		int64(binary.BigEndian.Uint16(bytes[4:6]))
	return time.UnixMilli(ms)
}
