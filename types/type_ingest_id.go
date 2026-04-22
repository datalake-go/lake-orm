package types

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// IngestID is the per-operation correlation token every Client.Insert
// generates. The underlying representation is a UUIDv7 — the leading
// 48 bits are a Unix-millisecond timestamp, which makes staging
// prefixes time-sortable in object-storage listings and lets the
// janitor (Client.CleanupStaging) identify orphans by parsing the
// timestamp out of a prefix name. No separate state file required.
//
// Every write plan carries the IngestID; the driver stamps it onto
// every row it writes (direct-ingest stamps per prepared INSERT,
// parquet-ingest stamps on the parquet stage's synthesised
// _ingest_id field). The MERGE source is filtered by the same
// IngestID so retry-on-OCC-conflict is idempotent.
//
// The string form is the canonical UUIDv7 hyphenated representation
// (e.g. "0193abc0-1234-7def-8901-234567890abc"). Drivers read the
// string form; types.IngestID is the typed wrapper callers see.
type IngestID string

// NewIngestID generates a fresh UUIDv7 IngestID. Returns an error
// only if the underlying crypto/rand draw fails, which in practice
// means the process is unable to read entropy — an unrecoverable
// state callers can surface to their own error path.
func NewIngestID() (IngestID, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("types.NewIngestID: %w", err)
	}
	return IngestID(u.String()), nil
}

// String returns the canonical hyphenated UUIDv7 representation
// and satisfies fmt.Stringer for log formatting.
func (id IngestID) String() string { return string(id) }

// Timestamp decodes the 48-bit unix_ts_ms header from the UUIDv7
// and returns the embedded moment. Used by CleanupStaging to
// identify prefixes older than a TTL without a state file.
//
// Returns the zero time if the underlying string is not a valid
// UUIDv7 — callers that care about the distinction check Valid()
// first.
func (id IngestID) Timestamp() time.Time {
	u, err := uuid.Parse(string(id))
	if err != nil || u.Version() != 7 {
		return time.Time{}
	}
	// UUIDv7 layout: 48 bits unix_ts_ms, 4 bits version, 12 bits
	// rand_a, 2 bits variant, 62 bits rand_b. ms is the first 6
	// bytes, big-endian.
	bytes := u[:6]
	ms := int64(binary.BigEndian.Uint32(bytes[:4]))<<16 |
		int64(binary.BigEndian.Uint16(bytes[4:6]))
	return time.UnixMilli(ms)
}

// Valid reports whether the string form parses as a UUIDv7. A
// zero-value IngestID returns false; any garbage string returns
// false; any non-v7 UUID returns false.
func (id IngestID) Valid() bool {
	u, err := uuid.Parse(string(id))
	return err == nil && u.Version() == 7
}
