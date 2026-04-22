// Package errors is the public error surface of lake-orm —
// sentinel values callers check with errors.Is, typed wrappers
// callers check with errors.As, and constructors that build them.
//
// Naming is uniform:
//
//	errors.ErrX         the sentinel / typed error
//	errors.NewErrX      the constructor (where one exists)
//	errors.IsErrX       the predicate (errors.As convenience)
//
// So ErrClusterNotReady pairs with NewErrClusterNotReady and
// IsErrClusterNotReady; the three always grep together.
//
// Every error message is prefixed "lakeorm: <subject>" so an
// unwrapped string still identifies the source library.
package errors

import "errors"

// Sentinel errors every caller can check with errors.Is.
var (
	ErrAlreadyCommitted = errors.New("lakeorm: finalizer already committed")
	ErrDriverMismatch   = errors.New("lakeorm: driver mismatch")
	ErrNoRows           = errors.New("lakeorm: no rows")
	ErrNotImplemented   = errors.New("lakeorm: not implemented")
	ErrUnknownDriver    = errors.New("lakeorm: unknown driver")
)
