package types

// SystemIngestIDColumn is the reserved name for lake-orm's
// per-operation ingest_id column. Every table lake-orm creates
// carries this column, populated with a UUIDv7 per Insert call.
// User structs must NOT declare it — the column is synthesised
// at DDL time by each dialect and stamped at write time by the
// driver layer.
//
// Reconciliation queries that want the column on the read side
// declare a separate result-shape struct with a field tagged
// `lake:"_ingest_id"`. The root scanner drops unmapped columns
// for structs that don't, so Query[T] over a T that ignores
// _ingest_id returns clean typed rows.
const SystemIngestIDColumn = "_ingest_id"
