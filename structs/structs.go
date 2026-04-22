// Package structs owns the "Go struct is the schema contract"
// invariant. It's the single place that reads user-declared struct
// shapes — every lake / lakeorm / spark tag, every validate rule,
// every JSON payload that decodes into a tagged model — and turns
// them into the introspection primitives the rest of lake-orm
// consumes (LakeSchema, LakeField, IndexIntent, LayoutIntent, etc.).
//
// Three entry points callers reach for at the application boundary:
//
//	structs.FromJSON[Book](payload)              // strict decode + validate
//	structs.Validate(records)                    // validate a pointer / slice
//	structs.ParseSchema(reflect.TypeOf(Book{}))  // introspect tags
//
// Everything else this package exports (the LakeSchema type,
// intent/strategy enums, ErrInvalidTag, etc.) is the vocabulary
// dialects, drivers, and the migration authoring layer read off a
// parsed struct.
//
// The invariant this package enforces is strict: an unknown JSON
// key, a disallowed tag modifier, or two tags claiming the same
// column on one field are all errors, never silent drops. Schema
// drift fails loudly at parse time instead of landing as corrupted
// rows hours later.
package structs
