package lakeorm

import (
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors used across the package.
var (
	ErrNotImplemented   = errors.New("lakeorm: not implemented")
	ErrAlreadyCommitted = errors.New("lakeorm: finalizer already committed")
	ErrNoRows           = errors.New("lakeorm: no rows")
	ErrInvalidTag       = errors.New("lakeorm: invalid struct tag")
	ErrUnknownDriver    = errors.New("lakeorm: unknown driver")
	ErrDriverMismatch   = errors.New("lakeorm: driver mismatch")
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
