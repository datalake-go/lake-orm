// Package databricksconnect is lake-orm's Databricks Connect
// driver. Handles OAuth token acquisition + refresh, the
// x-databricks-cluster-id Spark Connect URL parameter, and the
// 5-minute safety-margin token refresh Databricks clusters expect.
//
// "Databricks Connect" names the specific Spark Connect client
// shape Databricks workspaces expose. It is distinct from the
// Databricks Workspace SDK (databricks-sdk-go), which talks REST
// APIs rather than Spark Connect and is about workspace
// administration (clusters, jobs, workflows) — different surface,
// different problem. Lakeorm's Databricks driver is specifically
// the Spark-Connect-over-Databricks shape; the WorkspaceSDK remains
// the right tool for things like starting clusters or managing
// tokens from Go, and can be used alongside this package.
//
// Sibling to driver/spark: same underlying machinery (session pool,
// Execute/Query dispatch, cluster-not-ready translation) wired via
// spark.FromFactory, different session acquisition. Use
// spark.Remote for plain Spark Connect; use this package for
// Databricks clusters.
//
// See also [caldempsey/databricks-connect-go] — an independent
// Databricks Connect library that pre-dates this one and covers
// roughly the same surface. If you already depend on it, wrapping
// its SessionManager with spark.FromFactory is a two-line
// integration once the upstream Spark Connect type unification
// lands (caldempsey's lib pins apache/spark-connect-go/v40; lake-
// orm pins the datalake-go fork).
//
// [caldempsey/databricks-connect-go]: https://github.com/caldempsey/databricks-connect-go
package databricksconnect

import (
	"context"
	"fmt"

	scsql "github.com/datalake-go/spark-connect-go/spark/sql"

	"github.com/datalake-go/lake-orm"
	"github.com/datalake-go/lake-orm/drivers/spark"
)

// Driver returns a lakeorm.Driver that connects to a Databricks
// workspace via Spark Connect. Accepts OAuth M2M or PAT credentials
// through the Auth interface:
//
//	drv := databricksconnect.Driver(databricksconnect.OAuthM2M{
//	    WorkspaceURL: "https://acme.cloud.databricks.com",
//	    ClientID:     "...",
//	    ClientSecret: "...",
//	    ClusterID:    "0123-456789-abcdef01",
//	})
//	db, _ := lakeorm.Open(drv, iceberg.Dialect(), store)
//
// Options like WithLogger / WithPoolSize / WithSessionConfs from
// driver/spark work the same here — they tune the shared driver +
// session-pool plumbing.
func Driver(auth Auth, opts ...spark.RemoteOption) lakeorm.Driver {
	factory := sessionFactory(auth)
	return spark.FromFactory("databricks-connect", factory, opts...)
}

// sessionFactory returns a function that produces a SparkSession
// against the Databricks workspace for the given Auth. Kept
// separate from Driver so tests can exercise token-refresh paths
// without standing up the full driver.
func sessionFactory(auth Auth) func(context.Context) (scsql.SparkSession, error) {
	switch a := auth.(type) {
	case OAuthM2M:
		src := newOAuthTokenSource(a)
		return func(ctx context.Context) (scsql.SparkSession, error) {
			tok, err := src.Token()
			if err != nil {
				return nil, fmt.Errorf("databricksconnect: OAuth token: %w", err)
			}
			url := buildSparkConnectURL(a.WorkspaceURL, tok.AccessToken, a.ClusterID)
			s, err := (&scsql.SparkSessionBuilder{}).Remote(url).Build(ctx)
			if err != nil {
				return nil, fmt.Errorf("databricksconnect: build session: %w", err)
			}
			return s, nil
		}
	case PAT:
		return func(ctx context.Context) (scsql.SparkSession, error) {
			url := buildSparkConnectURL(a.WorkspaceURL, a.Token, a.ClusterID)
			s, err := (&scsql.SparkSessionBuilder{}).Remote(url).Build(ctx)
			if err != nil {
				return nil, fmt.Errorf("databricksconnect: build session: %w", err)
			}
			return s, nil
		}
	default:
		return func(context.Context) (scsql.SparkSession, error) {
			return nil, fmt.Errorf("databricksconnect: unsupported Auth type %T", auth)
		}
	}
}
