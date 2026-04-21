package spark

import (
	"strings"

	"github.com/datalake-go/lake-orm"
)

// translateClusterError returns an *lakeorm.ErrClusterNotReady if the
// underlying error matches the Databricks "[FailedPrecondition] + state
// Pending" pattern. Otherwise returns the original error unchanged.
//
// Duplicated logic in lakeorm.NewClusterNotReady (the library exports it
// so users can inspect any error from the driver); kept here as a thin
// helper so the driver doesn't need to import dorm just to name the
// detection — callers in the Spark driver path call this from every
// RPC site.
func translateClusterError(err error) error {
	if err == nil {
		return nil
	}
	if classified := lakeorm.NewClusterNotReady(err); classified != nil {
		return classified
	}
	return err
}

// looksLikeClusterPending is kept separate for unit testing without
// the exported sentinel wrapper.
func looksLikeClusterPending(msg string) bool {
	return strings.Contains(msg, "[FailedPrecondition]") &&
		(strings.Contains(msg, "state Pending") || strings.Contains(msg, "state PENDING"))
}
