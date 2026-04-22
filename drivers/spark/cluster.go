package spark

import (
	"github.com/datalake-go/lake-orm"
)

// translateClusterError returns an *lakeorm.ErrClusterNotReady if the
// underlying error matches the Databricks "[FailedPrecondition] + state
// Pending" pattern. Otherwise returns the original error unchanged.
//
// The underlying classification is lakeorm.NewClusterNotReady; this
// thin wrapper lets every RPC site in the driver convert raw errors
// to the typed sentinel without re-implementing the pattern.
func translateClusterError(err error) error {
	if err == nil {
		return nil
	}
	if classified := lakeorm.NewClusterNotReady(err); classified != nil {
		return classified
	}
	return err
}
