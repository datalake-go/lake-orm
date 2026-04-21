package lakeorm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// FromJSON decodes raw JSON bytes into a typed T, rejecting any
// field that isn't declared on the struct, and runs the registered
// validator on the result.
//
// This is the "models are the semantic layer" hook: callers who
// ingest data from an upstream HTTP API, message queue, or file
// take the raw JSON and unmarshal it straight into their lake-orm
// model. Anything the model doesn't declare — extra fields,
// renamed fields, nested objects that don't correspond to a
// struct-typed field — is rejected at decode time.
//
// Why strict: lake-orm deliberately doesn't support schema
// evolution at the storage layer (see the Philosophy section of
// the README). The corollary at the ingest boundary is "the
// struct IS the schema" — if your upstream payload has a field
// your model doesn't declare, either the producer is wrong and
// needs fixing, or your model is wrong and needs updating before
// a migration lands. Silent drop-on-ingest would make those two
// cases indistinguishable.
//
// Usage:
//
//	var book *Book
//	book, err := lakeorm.FromJSON[Book](payload)
//	if err != nil {
//	    return status.Errorf(codes.InvalidArgument, "%v", err)
//	}
//	// book is decoded + validated; safe to hand to Client.Insert
//
// Semantics:
//
//   - Unknown JSON keys (no matching struct field under any lake/
//     lakeorm/spark tag alias, and no matching encoding/json tag)
//     → decode error via encoding/json's DisallowUnknownFields.
//   - Type mismatches (JSON number where the struct declares a
//     string, JSON object where the struct declares an int, etc.)
//     → decode error via encoding/json's normal type checking.
//   - Required-field failures → validator error (unwraps to
//     validator.ValidationErrors).
//
// NOT checked:
//
//   - JSON keys that ARE declared as struct fields but hold unknown
//     sub-keys when the field itself is a `map[string]any`. If you
//     declare a bag, you've opted out of strictness on that field.
//     Prefer concrete struct-typed fields for anything you want
//     checked.
//   - Nested structs with their own `json:"..."` tags are still
//     walked. "Nested columns aren't part of the Go struct" is
//     enforced at the top-level key layer — nested structs that
//     the model DOES declare are part of the struct.
func FromJSON[T any](data []byte) (*T, error) {
	return FromJSONReader[T](bytes.NewReader(data))
}

// FromJSONReader is the io.Reader variant of FromJSON. Same
// semantics; useful when streaming from an HTTP request body.
func FromJSONReader[T any](r io.Reader) (*T, error) {
	var out T
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("lakeorm.FromJSON: %w", err)
	}
	// Trailing content after the JSON object is rejected — matches
	// Go's usual one-value-per-Decode contract and catches malformed
	// payloads where the caller concatenated two documents.
	if dec.More() {
		return nil, fmt.Errorf("lakeorm.FromJSON: trailing data after JSON value")
	}
	if err := Validate(&out); err != nil {
		return nil, err
	}
	return &out, nil
}
