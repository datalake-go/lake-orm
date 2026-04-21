package testutils

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// SparkConnectResult bundles the running Spark Connect server and
// its reachable sc:// URI.
type SparkConnectResult struct {
	URI string // sc://127.0.0.1:54321
}

// NewSparkConnectOptions configures NewSparkConnect.
type NewSparkConnectOptions struct {
	// Image overrides the default spark-connect image. The default
	// runs stock apache/spark with JARs resolved via --packages at
	// startup, which is slow on first run but requires no image
	// build. CI-friendly overrides: the dorm-spark-connect image
	// from lake-k8s/images/spark-connect with JARs pre-baked.
	Image string
	// S3AEndpoint, S3AAccessKey, S3ASecretKey point Spark's Hadoop
	// S3A at a MinIO / SeaweedFS running in the same docker network.
	// Leave blank to skip S3A configuration (pure-SQL smoke tests).
	S3AEndpoint  string
	S3AAccessKey string
	S3ASecretKey string
	// Warehouse is the s3a:// URI Spark should treat as the Iceberg
	// warehouse. Used with the Hadoop catalog — no separate catalog
	// service required. Leave blank to skip catalog wiring.
	Warehouse string
	// StartupTimeout bounds how long we wait for the Spark Connect
	// server to start accepting gRPC. Defaults to 300s because the
	// apache/spark image resolves several hundred MB of JARs on
	// first run — CI runners have cold disks.
	StartupTimeout time.Duration
}

// NewSparkConnect brings up a Spark Connect testcontainer and
// returns the resolved sc:// URI. Cleanup is registered on t.
//
// v0 implementation: stub that returns ErrSkip (via t.Skipf) when the
// required image isn't available locally. Exposes the full options
// surface so the teste2e suite and adjacent tests can share the
// constructor — the real startup logic lands once the
// dorm-spark-connect image is published, which keeps CI cycle time
// sane. Until then, the teste2e suite runs against the
// docker-compose stack the developer brings up manually.
//
// Rationale for the stub: bringing up a full Spark Connect server
// (stock apache/spark + --packages resolution) takes 2-5 minutes on
// cold runners, which is too expensive for every go-test pass. The
// production testcontainer image fixes this by baking the JARs in.
// Once that image is published, this function starts a container and
// returns the URI; until then, adjacent tests use a local endpoint
// via env var or are skipped.
func NewSparkConnect(t *testing.T, opts NewSparkConnectOptions) *SparkConnectResult {
	t.Helper()

	if opts.Image == "" {
		// Document the contract — until dorm-spark-connect publishes,
		// point DORM_SPARK_URI at the docker-compose stack or a local
		// cluster, and we skip container startup.
		t.Skip("testutils.NewSparkConnect: dorm-spark-connect image not specified; set opts.Image to a pre-baked Spark Connect image or use the docker-compose stack directly")
	}
	if opts.StartupTimeout == 0 {
		opts.StartupTimeout = 300 * time.Second
	}

	ctx := context.Background()
	const port = "15002/tcp"

	cmd := []string{
		"/opt/spark/sbin/start-connect-server.sh",
		"--conf", "spark.connect.grpc.binding.port=15002",
	}
	if opts.S3AEndpoint != "" {
		cmd = append(cmd,
			"--conf", "spark.hadoop.fs.s3a.endpoint="+opts.S3AEndpoint,
			"--conf", "spark.hadoop.fs.s3a.path.style.access=true",
			"--conf", "spark.hadoop.fs.s3a.access.key="+opts.S3AAccessKey,
			"--conf", "spark.hadoop.fs.s3a.secret.key="+opts.S3ASecretKey,
			"--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
		)
	}
	if opts.Warehouse != "" {
		// Hadoop catalog — the filesystem IS the catalog. No separate
		// service required; Iceberg metadata lives as `metadata/` JSON
		// files alongside the data.
		cmd = append(cmd,
			"--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
			"--conf", "spark.sql.catalog.dorm=org.apache.iceberg.spark.SparkCatalog",
			"--conf", "spark.sql.catalog.lakeorm.type=hadoop",
			"--conf", "spark.sql.catalog.lakeorm.warehouse="+opts.Warehouse,
		)
	}

	req := testcontainers.ContainerRequest{
		Image:        opts.Image,
		ExposedPorts: []string{port},
		Cmd:          cmd,
		WaitingFor: wait.ForListeningPort(port).
			WithStartupTimeout(opts.StartupTimeout),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start spark-connect container: %v", err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminate spark-connect container: %v", err)
		}
	})

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("get spark-connect host: %v", err)
	}
	mapped, err := ctr.MappedPort(ctx, "15002")
	if err != nil {
		t.Fatalf("get spark-connect port: %v", err)
	}
	return &SparkConnectResult{URI: fmt.Sprintf("sc://%s:%s", host, mapped.Port())}
}
