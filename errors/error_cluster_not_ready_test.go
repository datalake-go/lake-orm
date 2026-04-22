package errors

import (
	"errors"
	"testing"
)

// TestNewErrClusterNotReady_ParsesPending pins the contract
// NewErrClusterNotReady relies on — the exact string shape Databricks
// returns when a cluster is warming up. If Databricks ever rewords
// their error, these will fail loudly and tell us to update the
// detection logic.
func TestNewErrClusterNotReady_ParsesPending(t *testing.T) {
	cases := []struct {
		name, msg, wantState string
	}{
		{
			name:      "canonical Pending",
			msg:       "rpc error: code = Internal desc = [FailedPrecondition] The cluster is in state Pending and cannot execute the request",
			wantState: "PENDING", // canonical default when no explicit [state=...] in the string
		},
		{
			name:      "uppercase PENDING variant",
			msg:       "rpc error: [FailedPrecondition] cluster is in state PENDING",
			wantState: "PENDING",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := errors.New(c.msg)
			got := NewErrClusterNotReady(err)
			if got == nil {
				t.Fatalf("NewErrClusterNotReady returned nil for %q", c.msg)
			}
			if !got.IsRetryable() {
				t.Errorf("IsRetryable() = false, want true")
			}
		})
	}
}

func TestNewErrClusterNotReady_IgnoresUnrelated(t *testing.T) {
	cases := []string{
		"some random error",
		"[InvalidArgument] foo bar",
		"[FailedPrecondition] but not about cluster state",
		"",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			if got := NewErrClusterNotReady(errors.New(c)); got != nil {
				t.Errorf("NewErrClusterNotReady(%q) = %+v, want nil", c, got)
			}
		})
	}
}

func TestNewErrClusterNotReady_NilInput(t *testing.T) {
	if got := NewErrClusterNotReady(nil); got != nil {
		t.Errorf("NewErrClusterNotReady(nil) should return nil, got %+v", got)
	}
}

func TestIsErrClusterNotReady(t *testing.T) {
	err := NewErrClusterNotReady(errors.New("[FailedPrecondition] state Pending"))
	if !IsErrClusterNotReady(err) {
		t.Error("IsErrClusterNotReady should recognize the typed error")
	}
	if IsErrClusterNotReady(errors.New("boom")) {
		t.Error("IsErrClusterNotReady should reject unrelated errors")
	}
}
