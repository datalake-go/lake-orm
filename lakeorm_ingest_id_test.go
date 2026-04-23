package lakeorm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/datalake-go/lake-orm/backends"
	lkerrors "github.com/datalake-go/lake-orm/errors"
	"github.com/datalake-go/lake-orm/structs"
)

// --- _ingest_id is a system column; user-declared use rejected -----

type userDeclaredIngestID struct {
	ID       string `lake:"id,pk"`
	IngestID string `lake:"_ingest_id"`
}

func TestParseSchema_RejectsUserDeclaredIngestIDColumn(t *testing.T) {
	_, err := structs.ParseSchema(reflect.TypeOf(userDeclaredIngestID{}))
	if err == nil {
		t.Fatal("structs.ParseSchema should reject a user field mapped to _ingest_id")
	}
	if !errors.Is(err, structs.ErrInvalidTag) {
		t.Errorf("err = %v, want wraps structs.ErrInvalidTag", err)
	}
	if !strings.Contains(err.Error(), "system-managed") {
		t.Errorf("error should explain the system-managed column rule, got: %v", err)
	}
}

// --- auto=ingestID tag modifier is no longer supported -------------

type legacyAutoIngestID struct {
	ID       string `lake:"id,pk"`
	IngestID string `lake:"some_col,auto=ingestID"`
}

func TestParseSchema_RejectsLegacyAutoIngestIDModifier(t *testing.T) {
	_, err := structs.ParseSchema(reflect.TypeOf(legacyAutoIngestID{}))
	if err == nil {
		t.Fatal("structs.ParseSchema should reject auto=ingestID modifier")
	}
	if !errors.Is(err, structs.ErrInvalidTag) {
		t.Errorf("err = %v, want wraps structs.ErrInvalidTag", err)
	}
	if !strings.Contains(err.Error(), "no longer supported") {
		t.Errorf("error should carry the migration message, got: %v", err)
	}
}

// --- UUIDv7 timestamp extraction -----------------------------------

func TestExtractUUIDv7Timestamp_RoundTrips(t *testing.T) {
	before := time.Now()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7: %v", err)
	}
	after := time.Now()
	got := extractUUIDv7Timestamp(id)
	// UUIDv7 has ms precision; allow a small wall-clock window either
	// side of the generation call.
	if got.Before(before.Add(-time.Millisecond)) || got.After(after.Add(time.Millisecond)) {
		t.Errorf("extracted ts %v not within generation window [%v, %v]", got, before, after)
	}
}

// --- CleanupStaging plumbing ---------------------------------------

// fakeBackend is a minimal Backend that only implements what
// CleanupStaging touches: Name, CleanupStaging-by-URI, and the
// optional StagingLister extension.
type fakeStagingBackend struct {
	backends.Backend
	prefixes      []StagingPrefix
	listErr       error
	deletedURIs   []string
	failDeleteURI string
}

func (f *fakeStagingBackend) Name() string { return "fake" }
func (f *fakeStagingBackend) CleanupStaging(_ context.Context, uri string) error {
	if uri == f.failDeleteURI {
		return errors.New("permission denied")
	}
	f.deletedURIs = append(f.deletedURIs, uri)
	return nil
}

func (f *fakeStagingBackend) ListStagingPrefixes(_ context.Context) ([]StagingPrefix, error) {
	return f.prefixes, f.listErr
}

func TestCleanupStaging_DeletesOnlyExpiredUUIDv7Prefixes(t *testing.T) {
	// Fresh UUIDv7 → skip. Past-TTL UUIDv7 → delete. Garbage name →
	// skip (not ours). Delete failure → bubble into Failed.
	oldID, _ := uuid.NewV7()
	// Override the ms so the prefix is 10 days old.
	oldMs := time.Now().Add(-10 * 24 * time.Hour).UnixMilli()
	oldID[0] = byte(oldMs >> 40)
	oldID[1] = byte(oldMs >> 32)
	oldID[2] = byte(oldMs >> 24)
	oldID[3] = byte(oldMs >> 16)
	oldID[4] = byte(oldMs >> 8)
	oldID[5] = byte(oldMs)

	freshID, _ := uuid.NewV7()

	prefixes := []StagingPrefix{
		{URI: "s3://b/lake/_staging/" + oldID.String(), IngestID: oldID.String()},
		{URI: "s3://b/lake/_staging/" + freshID.String(), IngestID: freshID.String()},
		{URI: "s3://b/lake/_staging/not-a-uuid", IngestID: "not-a-uuid"},
	}

	c := &client{backend: &fakeStagingBackend{prefixes: prefixes}}
	rep, err := c.CleanupStaging(context.Background(), 24*time.Hour)
	if err != nil {
		t.Fatalf("CleanupStaging: %v", err)
	}
	if rep.Scanned != 3 {
		t.Errorf("Scanned = %d, want 3", rep.Scanned)
	}
	if len(rep.Deleted) != 1 || !strings.Contains(rep.Deleted[0], oldID.String()) {
		t.Errorf("Deleted = %v, want the oldID prefix only", rep.Deleted)
	}
	if len(rep.Failed) != 0 {
		t.Errorf("Failed = %v, want empty", rep.Failed)
	}
}

func TestCleanupStaging_BackendWithoutStagingListerReturnsNotImplemented(t *testing.T) {
	// A plain Backend that doesn't implement StagingLister.
	type plainBackend struct{ backends.Backend }
	c := &client{backend: &plainBackend{}}
	_, err := c.CleanupStaging(context.Background(), 24*time.Hour)
	if !errors.Is(err, lkerrors.ErrNotImplemented) {
		t.Errorf("err = %v, want lkerrors.ErrNotImplemented", err)
	}
}

func TestCleanupStaging_SurfacesListError(t *testing.T) {
	want := errors.New("list failed")
	c := &client{backend: &fakeStagingBackend{listErr: want}}
	_, err := c.CleanupStaging(context.Background(), 24*time.Hour)
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want wrapping %v", err, want)
	}
}

func TestCleanupStaging_RecordsDeleteFailures(t *testing.T) {
	// Force one expired prefix's delete to fail; it should land in
	// Failed (not Deleted). Remaining ones still process.
	oldID1, _ := uuid.NewV7()
	oldMs := time.Now().Add(-10 * 24 * time.Hour).UnixMilli()
	for _, id := range []*uuid.UUID{&oldID1} {
		id[0] = byte(oldMs >> 40)
		id[1] = byte(oldMs >> 32)
		id[2] = byte(oldMs >> 24)
		id[3] = byte(oldMs >> 16)
		id[4] = byte(oldMs >> 8)
		id[5] = byte(oldMs)
	}
	badURI := "s3://b/lake/_staging/" + oldID1.String()
	c := &client{backend: &fakeStagingBackend{
		prefixes:      []StagingPrefix{{URI: badURI, IngestID: oldID1.String()}},
		failDeleteURI: badURI,
	}}
	rep, err := c.CleanupStaging(context.Background(), 24*time.Hour)
	if err != nil {
		t.Fatalf("CleanupStaging: %v", err)
	}
	if len(rep.Failed) != 1 || rep.Failed[0] != badURI {
		t.Errorf("Failed = %v, want [%q]", rep.Failed, badURI)
	}
	if len(rep.Deleted) != 0 {
		t.Errorf("Deleted = %v, want empty", rep.Deleted)
	}
}
