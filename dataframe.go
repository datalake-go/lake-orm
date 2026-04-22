package lakeorm

import (
	"context"
	"iter"
)

// DataFrame is the Driver-agnostic handle to a remote DataFrame. At v0
// it wraps the Spark Connect DataFrame; v1+ drivers may implement their
// own. The interface is deliberately minimal — anything fancier is
// available by unwrapping with DriverType().
type DataFrame interface {
	Schema(ctx context.Context) ([]ColumnInfo, error)
	Collect(ctx context.Context) ([][]any, error)
	Count(ctx context.Context) (int64, error)
	Stream(ctx context.Context) iter.Seq2[Row, error]

	// DriverType returns the Driver's native DataFrame handle for
	// callers that need to drop to the underlying API. Kept as any
	// to avoid leaking driver-specific types into the public
	// signature; callers type-assert to the concrete type they
	// expect (e.g. sparksql.DataFrame). Prefer the lakeorm.CollectAs
	// / lakeorm.StreamAs helpers when materialising into a typed
	// result struct.
	DriverType() any
}

// Row is a Driver-agnostic row handle. Scanner consumes these to
// populate typed structs.
type Row interface {
	Values() []any
	Columns() []string
}

// RowStream is the Driver's streaming primitive — one Row per
// iteration, constant memory, natural backpressure. It is an alias
// for iter.Seq2[Row, error] so it rangeable directly.
type RowStream = iter.Seq2[Row, error]
