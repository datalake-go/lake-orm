package lakeorm

import (
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors used across the package.
var (
	ErrNotImplemented    = errors.New("lakeorm: not implemented")
	ErrAlreadyCommitted  = errors.New("lakeorm: finalizer already committed")
	ErrNoRows            = errors.New("lakeorm: no rows")
	ErrSessionPoolClosed = errors.New("lakeorm: session pool closed")
	ErrInvalidTag        = errors.New("lakeorm: invalid struct tag")
	ErrUnknownDriver     = errors.New("lakeorm: unknown driver")
	ErrDriverMismatch    = errors.New("lakeorm: driver mismatch")
)

// ErrClusterNotReady indicates the backing Spark cluster is warming
// up and the operation should be retried. Databricks in particular
// returns [FailedPrecondition] errors with state Pending during
// cluster startup — without this typed error, callers get an opaque
// gRPC failure and no way to distinguish "cluster warming up, retry"
// from "cluster dead, give up."
type ErrClusterNotReady struct {
	State     string // e.g. "Pending", "PENDING"
	RequestID string
	Message   string
	Cause     error
}

func (e *ErrClusterNotReady) Error() string {
	return fmt.Sprintf("cluster not ready (state=%s): %s", e.State, e.Message)
}

func (e *ErrClusterNotReady) Unwrap() error { return e.Cause }

// IsRetryable marks this error as safe to retry after backoff.
func (e *ErrClusterNotReady) IsRetryable() bool { return true }

// IsClusterNotReady is a convenience for errors.As callers.
func IsClusterNotReady(err error) bool {
	var e *ErrClusterNotReady
	return errors.As(err, &e)
}

// NewClusterNotReady parses a Databricks gRPC error for the
// [FailedPrecondition] + "state Pending" pattern and returns a typed
// error. Returns nil if the error doesn't match. The string-matching
// looks fragile, but it's the canonical detection pattern and has
// been stable across multiple Databricks runtime versions.
func NewClusterNotReady(err error) *ErrClusterNotReady {
	if err == nil {
		return nil
	}

	errStr := err.Error()

	if !strings.Contains(errStr, "[FailedPrecondition]") ||
		(!strings.Contains(errStr, "state Pending") &&
			!strings.Contains(errStr, "state PENDING")) {
		return nil
	}

	state := "PENDING"
	if idx := strings.Index(errStr, "[state="); idx != -1 {
		endIdx := strings.Index(errStr[idx:], "]")
		if endIdx != -1 {
			state = errStr[idx+7 : idx+endIdx]
		}
	}

	var requestID string
	if idx := strings.Index(errStr, "(requestId="); idx != -1 {
		endIdx := strings.Index(errStr[idx:], ")")
		if endIdx != -1 {
			requestID = errStr[idx+11 : idx+endIdx]
		}
	}

	return &ErrClusterNotReady{
		State:     state,
		RequestID: requestID,
		Message:   "The cluster is starting up. Please retry your request in a few moments.",
		Cause:     err,
	}
}

// ErrClientStaging is returned when the Go client fails to write a
// staging object via Backend. Indicates a client-side reachability or
// credential problem.
type ErrClientStaging struct {
	URI         string
	BackendName string
	Op          string // "stage-write" | "probe-write" | "cleanup"
	Cause       error
}

func (e *ErrClientStaging) Error() string {
	return fmt.Sprintf("lakeorm: client-side %s failed on %s (backend=%s): %v",
		e.Op, e.URI, e.BackendName, e.Cause)
}
func (e *ErrClientStaging) Unwrap() error { return e.Cause }

// ErrDriverRead is returned when the compute driver fails to read a
// staging prefix that the client wrote successfully. Indicates the
// driver cannot see (or cannot authenticate against) storage the
// client can reach — the split-view failure mode where client and
// driver credentials diverge silently.
type ErrDriverRead struct {
	StagingURI string
	DriverName string
	Cause      error
}

func (e *ErrDriverRead) Error() string {
	return fmt.Sprintf("lakeorm: driver %s failed to read %s: %v",
		e.DriverName, e.StagingURI, e.Cause)
}
func (e *ErrDriverRead) Unwrap() error { return e.Cause }

// ErrURIMismatch is returned when client and driver both succeed at
// their respective operations but resolve the same URI to different
// physical storage. Detected by lakeorm.Verify via probe comparison.
type ErrURIMismatch struct {
	ClientURI string
	DriverURI string
	Detail    string
}

func (e *ErrURIMismatch) Error() string {
	return fmt.Sprintf("lakeorm: URI mismatch between client (%s) and driver (%s): %s",
		e.ClientURI, e.DriverURI, e.Detail)
}
