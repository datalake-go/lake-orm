package errors

import (
	"errors"
	"fmt"
	"strings"
)

// ErrClusterNotReady indicates the backing Spark cluster is
// warming up and the operation should be retried. Databricks in
// particular returns [FailedPrecondition] errors with state
// Pending during cluster startup — without this typed error,
// callers get an opaque gRPC failure and no way to distinguish
// "cluster warming up, retry" from "cluster dead, give up."
type ErrClusterNotReady struct {
	State     string // e.g. "Pending", "PENDING"
	RequestID string
	Message   string
	Cause     error
}

// Error implements error.
func (e *ErrClusterNotReady) Error() string {
	return fmt.Sprintf("cluster not ready (state=%s): %s", e.State, e.Message)
}

// Unwrap returns the underlying cause so errors.Is / errors.As
// reach it.
func (e *ErrClusterNotReady) Unwrap() error { return e.Cause }

// IsRetryable marks this error as safe to retry after backoff.
// Retry loops higher up the stack check this before deciding
// whether to resend.
func (e *ErrClusterNotReady) IsRetryable() bool { return true }

// IsErrClusterNotReady reports whether err (or anything it
// wraps) is an ErrClusterNotReady. errors.As convenience.
func IsErrClusterNotReady(err error) bool {
	var e *ErrClusterNotReady
	return errors.As(err, &e)
}

// NewErrClusterNotReady classifies a Databricks gRPC error as a
// cluster-warming-up case. Returns nil if the supplied error
// doesn't match the [FailedPrecondition] + "state Pending"
// pattern Databricks emits during cluster startup. The string-
// matching looks fragile but has been stable across multiple
// Databricks runtime versions and is the canonical detection.
func NewErrClusterNotReady(err error) *ErrClusterNotReady {
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
