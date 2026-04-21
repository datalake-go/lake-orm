// Package parquet is lakeorm's parquet-writing layer. Two things
// live here:
//
//  1. Schema synthesis from `spark:"..."` struct tags. Users tag their
//     structs once; this package derives the parquet schema (types,
//     TIMESTAMP units, required/optional) so callers don't need to
//     declare `parquet:"..."` tags too.
//
//  2. The partition writer used by the fast-path insert flow. Rows
//     stream into a live parquet.Writer backed by a bytes buffer,
//     parts rotate when the configured byte or row threshold trips,
//     each rotation uploads to a Backend, and at the end the caller
//     issues a single Spark SQL statement over the whole prefix.
//
// The partition writer's Flush-per-Write pattern is load-bearing —
// see partition_writer.go for the rationale, and the regression
// tests in partition_writer_test.go for what we're pinning.
package parquet
