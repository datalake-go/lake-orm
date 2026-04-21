package lakeorm

import (
	"errors"
	"testing"
)

// TestNewClusterNotReady_ParsesPending pins the contract
// NewClusterNotReady relies on — the exact string shape Databricks
// returns when a cluster is warming up. If Databricks ever rewords
// their error, these will fail loudly and tell us to update the
// detection logic.
func TestNewClusterNotReady_ParsesPending(t *testing.T) {
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
			got := NewClusterNotReady(err)
			if got == nil {
				t.Fatalf("NewClusterNotReady returned nil for %q", c.msg)
			}
			if !got.IsRetryable() {
				t.Errorf("IsRetryable() = false, want true")
			}
		})
	}
}

func TestNewClusterNotReady_IgnoresUnrelated(t *testing.T) {
	cases := []string{
		"some random error",
		"[InvalidArgument] foo bar",
		"[FailedPrecondition] but not about cluster state",
		"",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			if got := NewClusterNotReady(errors.New(c)); got != nil {
				t.Errorf("NewClusterNotReady(%q) = %+v, want nil", c, got)
			}
		})
	}
}

func TestNewClusterNotReady_NilInput(t *testing.T) {
	if got := NewClusterNotReady(nil); got != nil {
		t.Errorf("NewClusterNotReady(nil) should return nil, got %+v", got)
	}
}

func TestIsClusterNotReady(t *testing.T) {
	err := NewClusterNotReady(errors.New("[FailedPrecondition] state Pending"))
	if !IsClusterNotReady(err) {
		t.Error("IsClusterNotReady should recognize the typed error")
	}
	if IsClusterNotReady(errors.New("boom")) {
		t.Error("IsClusterNotReady should reject unrelated errors")
	}
}
