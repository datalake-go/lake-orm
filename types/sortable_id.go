// Package types defines the small value types lakeorm exposes on its
// public API: SortableID (KSUID-backed), ObjectURI/Location (typed storage
// addresses), and SparkTableName. Kept in its own package so downstream
// code can import types without pulling in the driver/format/backend stack.
package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/segmentio/ksuid"
)

// SortableID is a KSUID stored as its canonical base62 string. KSUIDs sort
// lexicographically in creation order, which is what makes them suitable
// for primary keys, idempotency tokens, and ingest IDs on object-storage
// prefixes (so lexicographic S3 listings match temporal order).
type SortableID string

// NewSortableID returns a freshly generated KSUID.
func NewSortableID() SortableID {
	return SortableID(ksuid.New().String())
}

// ParseSortableID validates and returns a SortableID from a string.
func ParseSortableID(s string) (SortableID, error) {
	id, err := ksuid.Parse(s)
	if err != nil {
		return "", fmt.Errorf("invalid sortable ID: %w", err)
	}
	return SortableID(id.String()), nil
}

func (id SortableID) String() string { return string(id) }

// IsNil reports whether the ID is the zero value (empty or ksuid.Nil).
func (id SortableID) IsNil() bool {
	s := id.String()
	return s == "" || s == ksuid.Nil.String()
}

// Compare returns -1/0/1 by lexicographic string order (KSUIDs are time-sortable).
func (id SortableID) Compare(other *SortableID) int {
	if other == nil {
		if id.IsNil() {
			return 0
		}
		return 1
	}
	return strings.Compare(id.String(), other.String())
}

// KSUID returns the underlying ksuid.KSUID (or ksuid.Nil on error / empty).
func (id SortableID) KSUID() ksuid.KSUID {
	if id.IsNil() {
		return ksuid.Nil
	}
	k, err := ksuid.Parse(id.String())
	if err != nil {
		return ksuid.Nil
	}
	return k
}

// Bytes returns the 20 raw KSUID bytes, or nil if the ID is zero.
func (id SortableID) Bytes() []byte {
	k := id.KSUID()
	if k.IsNil() {
		return nil
	}
	out := make([]byte, len(k))
	copy(out, k[:])
	return out
}

func (id SortableID) MarshalText() ([]byte, error) {
	if id.IsNil() {
		return []byte{}, nil
	}
	return []byte(id.String()), nil
}

func (id *SortableID) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		*id = ""
		return nil
	}
	parsed, err := ParseSortableID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

func (id SortableID) MarshalJSON() ([]byte, error) {
	if id.IsNil() {
		return []byte("null"), nil
	}
	return json.Marshal(id.String())
}

func (id *SortableID) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*id = ""
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		*id = ""
		return nil
	}
	parsed, err := ParseSortableID(s)
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// Value implements driver.Valuer for database/sql.
func (id SortableID) Value() (driver.Value, error) {
	if id.IsNil() {
		return nil, nil
	}
	return id.String(), nil
}

// Scan implements sql.Scanner for database/sql. Needed for the reflection
// scanner in scanner.go to pick SortableID up via its scannerTarget path
// (nullable SortableID columns become *SortableID fields transparently).
func (id *SortableID) Scan(src any) error {
	if src == nil {
		*id = ""
		return nil
	}
	switch v := src.(type) {
	case string:
		parsed, err := ParseSortableID(v)
		if err != nil {
			return err
		}
		*id = parsed
		return nil
	case []byte:
		return id.Scan(string(v))
	default:
		return fmt.Errorf("unsupported Scan type for SortableID: %T", src)
	}
}

// SortableIDsToStrings converts a slice of SortableID to a slice of strings.
func SortableIDsToStrings(ids []SortableID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}
	return out
}
