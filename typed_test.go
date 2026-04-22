package lakeorm

import (
	"context"
	"errors"
	"iter"
	"testing"
	lkerrors "github.com/datalake-go/lake-orm/errors"
)

// fakeDataFrame is a driver-agnostic DataFrame fixture for the
// generic materialisation path. The Stream() implementation is a
// no-op iterator so CollectAs returns an empty slice and StreamAs
// yields nothing — enough to exercise the "non-Spark driver uses
// generic path" branch without a real *sql.DB.
type fakeDataFrame struct{ native any }

func (f *fakeDataFrame) Schema(context.Context) ([]ColumnInfo, error) { return nil, nil }
func (f *fakeDataFrame) Collect(context.Context) ([][]any, error)     { return nil, nil }
func (f *fakeDataFrame) Count(context.Context) (int64, error)         { return 0, nil }
func (f *fakeDataFrame) Stream(context.Context) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {}
}
func (f *fakeDataFrame) DriverType() any { return f.native }

type dummyResult struct{ X int }

func TestCollectAs_NilDataFrameReturnsDriverMismatch(t *testing.T) {
	_, err := CollectAs[dummyResult](context.Background(), nil)
	if !errors.Is(err, lkerrors.ErrDriverMismatch) {
		t.Errorf("err = %v, want wraps lkerrors.ErrDriverMismatch", err)
	}
}

// Pre-duckdb-driver lake-orm errored on any non-Spark DataFrame with
// lkerrors.ErrDriverMismatch. The typed helpers now fall back to a generic
// Row + Scanner path, so non-Spark drivers work unchanged through
// Query[T] / CollectAs[T]. The fakeDataFrame below is the contract
// fixture: an empty generic Stream returns an empty slice cleanly,
// no error.
func TestCollectAs_NonSparkDriverFallsBackToGenericPath(t *testing.T) {
	df := &fakeDataFrame{native: "not-a-spark-df"}
	rows, err := CollectAs[dummyResult](context.Background(), df)
	if err != nil {
		t.Fatalf("CollectAs non-Spark: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("rows = %v, want empty slice", rows)
	}
}

func TestStreamAs_NonSparkDriverFallsBackToGenericPath(t *testing.T) {
	df := &fakeDataFrame{native: 42}
	count := 0
	for _, err := range StreamAs[dummyResult](context.Background(), df) {
		if err != nil {
			t.Fatalf("StreamAs non-Spark yielded error: %v", err)
		}
		count++
	}
	if count != 0 {
		t.Errorf("yielded %d rows, want 0 (empty stream)", count)
	}
}

func TestFirstAs_NilDataFrameReturnsDriverMismatch(t *testing.T) {
	_, err := FirstAs[dummyResult](context.Background(), nil)
	if !errors.Is(err, lkerrors.ErrDriverMismatch) {
		t.Errorf("err = %v, want wraps lkerrors.ErrDriverMismatch", err)
	}
}

func TestFirstAs_NonSparkEmptyStreamReturnsNoRows(t *testing.T) {
	df := &fakeDataFrame{native: "not-a-spark-df"}
	_, err := FirstAs[dummyResult](context.Background(), df)
	if !errors.Is(err, lkerrors.ErrNoRows) {
		t.Errorf("err = %v, want wraps lkerrors.ErrNoRows", err)
	}
}
