// Package iceberg implements lakeorm's default Dialect — Apache
// Iceberg tables written via Spark Connect. Iceberg is the v0 default
// because of its engine-agnostic portability (Trino, DuckDB, Snowflake
// all read it natively) and its hidden partitioning (much better DX
// than Delta's generated-columns approach).
//
// # Catalogs
//
// The Iceberg catalog (Hadoop / REST / Glue / Unity / Polaris) is
// configured at the Spark Connect server level via
// `spark.sql.catalog.<name>.*` confs. The Go Dialect only needs to
// know the catalog's *registered name* so it can qualify table
// references (CREATE TABLE <name>.<db>.<table>) — it does not make
// catalog API calls itself. Override the name via WithCatalogName.
// v1 adds an in-process iceberg-go path that bypasses Spark for
// metadata operations; that's when a real Catalog abstraction lands.
package iceberg

import (
	"fmt"
	"strings"

	"github.com/datalake-go/lake-orm/dialects"
	"github.com/datalake-go/lake-orm/drivers"
	"github.com/datalake-go/lake-orm/structs"
	"github.com/datalake-go/lake-orm/types"
)

// DialectOption tunes the Iceberg Dialect.
type DialectOption func(*config)

type config struct {
	catalogName    string // Spark catalog name: SQL references become <catalogName>.<db>.<table>
	database       string // default database for unqualified table names
	indexStrategy  map[string]structs.IndexStrategy
	layoutHint     map[string]structs.LayoutStrategy
	defaultBucketN int
}

func newConfig() *config {
	return &config{
		indexStrategy: map[string]structs.IndexStrategy{},
		layoutHint:    map[string]structs.LayoutStrategy{},
		// "lakeorm" matches the catalog name the docker-compose /
		// Helm chart register with Spark (spark.sql.catalog.lakeorm=...).
		// If you register the Iceberg catalog under a different name
		// on your Spark cluster, override via WithCatalogName.
		catalogName: "lakeorm",
		// "default" is Spark's conventional database for
		// unqualified references. Matches Hive / Iceberg REST default.
		database: "default",
		// 64 buckets is a reasonable default for `indexed` columns —
		// enough to distribute a large table but not so many that
		// small tables end up with tiny files. Override per-column
		// via WithIndexStrategy.
		defaultBucketN: 64,
	}
}

// WithCatalogName sets the Spark catalog identifier the Iceberg
// catalog is registered as. Default "lakeorm" — matches what
// docker-compose and lake-k8s configure. Override to match an
// existing cluster's catalog registration.
func WithCatalogName(name string) DialectOption {
	return func(cfg *config) {
		if name != "" {
			cfg.catalogName = name
		}
	}
}

// WithDatabase overrides the default database for unqualified
// tables. Default "default".
func WithDatabase(name string) DialectOption {
	return func(cfg *config) {
		if name != "" {
			cfg.database = name
		}
	}
}

// WithIndexStrategy overrides the default `indexed` → `bucket(N)`
// mapping for a specific column.
func WithIndexStrategy(column string, strategy structs.IndexStrategy) DialectOption {
	return func(cfg *config) { cfg.indexStrategy[column] = strategy }
}

// WithLayoutHint overrides the default `sortable` → sort-order mapping.
func WithLayoutHint(column string, strategy structs.LayoutStrategy) DialectOption {
	return func(cfg *config) { cfg.layoutHint[column] = strategy }
}

// WithDefaultBucketCount changes the default bucket count used when a
// column is tagged `indexed` without a column-specific override.
func WithDefaultBucketCount(n int) DialectOption {
	return func(cfg *config) {
		if n > 0 {
			cfg.defaultBucketN = n
		}
	}
}

// BucketPartition returns a bucket-N IndexStrategy descriptor.
func BucketPartition(n int) structs.IndexStrategy {
	return structs.IndexStrategy(fmt.Sprintf("bucket:%d", n))
}

// SortOrder marks a column as contributing to the table sort order.
var SortOrder structs.LayoutStrategy = "sort_order"

// Dialect constructs the Iceberg data-dialect.
func Dialect(opts ...DialectOption) dialects.Dialect {
	cfg := newConfig()
	for _, o := range opts {
		o(cfg)
	}
	return &dialect{cfg: cfg}
}

type dialect struct {
	cfg *config
}

func (d *dialect) Name() string { return "iceberg" }

// IndexStrategy translates the lake tag intent into a physical
// strategy. `indexed` / `mergeKey` default to bucket(cfg.defaultBucketN);
// column-level overrides win via WithIndexStrategy.
func (d *dialect) IndexStrategy(intent structs.IndexIntent) structs.IndexStrategy {
	switch intent {
	case structs.IntentIndexed, structs.IntentMergeKey:
		return structs.IndexStrategy(fmt.Sprintf("bucket:%d", d.cfg.defaultBucketN))
	default:
		return ""
	}
}

func (d *dialect) LayoutStrategy(intent structs.LayoutIntent) structs.LayoutStrategy {
	if intent == structs.LayoutSortable {
		return SortOrder
	}
	return ""
}

// CreateTableDDL emits an Iceberg CREATE TABLE statement.
// v0: basic schema rendering + PARTITIONED BY derived from tags;
// v1 adds full type-system fidelity, identifier columns, sort orders.
func (d *dialect) CreateTableDDL(schema *structs.LakeSchema, loc types.Location) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("iceberg: nil schema")
	}

	cols := make([]string, 0, len(schema.Fields)+1)
	for i := range schema.Fields {
		fl := schema.Fields[i]
		if fl.Ignored {
			continue
		}
		icebergType, err := goTypeToIceberg(fl.Type)
		if err != nil {
			return "", fmt.Errorf("iceberg: column %s: %w", fl.Column, err)
		}
		col := fl.Column + " " + icebergType
		if !fl.IsNullable {
			col += " NOT NULL"
		}
		cols = append(cols, col)
	}
	// System-managed ingest_id column: every table lake-orm creates
	// carries this, stamped per-Insert at the driver layer. Nullable
	// so pre-existing tables can gain the column via ALTER TABLE
	// without a backfill, and so existing rows (untracked batches)
	// keep NULL. See types.SystemIngestIDColumn.
	cols = append(cols, types.SystemIngestIDColumn+" STRING")

	partitions := d.partitionClause(schema)

	// LOCATION is intentionally omitted when the Iceberg catalog is
	// a REST catalog (Nessie / Polaris / Tabular). The REST catalog
	// derives table locations from its configured `warehouse` prefix;
	// passing an explicit LOCATION conflicts with that and yields
	// opaque "Unable to process" errors from the Iceberg server. For
	// non-REST catalogs (Hive, Hadoop), LOCATION is still useful —
	// v1 threads this through when other catalog implementations land.
	_ = loc

	// Spark SQL ordering is strict: USING <format> must precede
	// PARTITIONED BY. Swapping the order produces PARSE_SYNTAX_ERROR
	// at "USING".
	//
	// Table names are three-part-qualified (<catalog>.<db>.<table>)
	// so Spark routes writes through the registered Iceberg catalog
	// instead of the default spark_catalog. Without the prefix, the
	// CREATE TABLE can land in the wrong catalog and subsequent
	// SELECTs fail with "iceberg is not a valid Spark SQL Data Source".
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s) USING iceberg%s",
		d.qualify(schema.TableName), strings.Join(cols, ", "), partitions,
	), nil
}

// qualify prefixes a bare table name with <catalog>.<database>. If the
// caller already passed a dotted name, it is returned unchanged —
// callers who know what they're doing should be able to address any
// catalog the cluster has registered.
func (d *dialect) qualify(table string) string {
	if strings.Contains(table, ".") {
		return table
	}
	return fmt.Sprintf("%s.%s.%s", d.cfg.catalogName, d.cfg.database, table)
}

func (d *dialect) partitionClause(schema *structs.LakeSchema) string {
	if len(schema.Partitions) == 0 {
		// Auto-partition on mergeKey columns as bucket(defaultN) when
		// no explicit partition spec was declared. This is the key DX
		// win of Iceberg hidden partitioning — users write
		// `WHERE email = 'x'` and pruning happens automatically.
		if len(schema.MergeKeys) == 0 {
			return ""
		}
		expr := make([]string, 0, len(schema.MergeKeys))
		for _, idx := range schema.MergeKeys {
			col := schema.Fields[idx].Column
			if override, ok := d.cfg.indexStrategy[col]; ok {
				expr = append(expr, renderStrategy(override, col))
			} else {
				expr = append(expr, fmt.Sprintf("bucket(%d, %s)", d.cfg.defaultBucketN, col))
			}
		}
		return " PARTITIONED BY (" + strings.Join(expr, ", ") + ")"
	}

	expr := make([]string, 0, len(schema.Partitions))
	for _, p := range schema.Partitions {
		col := schema.Fields[p.FieldIndex].Column
		switch p.Strategy {
		case structs.PartitionRaw:
			expr = append(expr, col)
		case structs.PartitionBucket:
			n := p.Param
			if n == 0 {
				n = d.cfg.defaultBucketN
			}
			expr = append(expr, fmt.Sprintf("bucket(%d, %s)", n, col))
		case structs.PartitionTruncate:
			expr = append(expr, fmt.Sprintf("truncate(%d, %s)", p.Param, col))
		}
	}
	return " PARTITIONED BY (" + strings.Join(expr, ", ") + ")"
}

func renderStrategy(s structs.IndexStrategy, col string) string {
	str := string(s)
	if strings.HasPrefix(str, "bucket:") {
		return fmt.Sprintf("bucket(%s, %s)", strings.TrimPrefix(str, "bucket:"), col)
	}
	if strings.HasPrefix(str, "truncate:") {
		return fmt.Sprintf("truncate(%s, %s)", strings.TrimPrefix(str, "truncate:"), col)
	}
	return col
}

// PlanInsert routes to DirectIngest, ParquetIngest, or ParquetMerge
// based on batch size, caller-requested overrides, and whether the
// schema declares any mergeKey field. mergeKey presence turns the
// fast path from append (INSERT INTO target SELECT * FROM staging)
// into upsert (MERGE INTO target USING (SELECT * FROM staging WHERE
// _ingest_id = '<batch>') ON target.<mergeKey> = source.<mergeKey>
// WHEN MATCHED UPDATE WHEN NOT MATCHED INSERT).
func (d *dialect) PlanInsert(req drivers.WriteRequest) (drivers.ExecutionPlan, error) {
	if req.ForcePath == drivers.WritePathGRPC ||
		(req.ForcePath == drivers.WritePathAuto && req.ApproxRowBytes < req.FastPathBytes) {
		return drivers.ExecutionPlan{
			Kind:     drivers.KindDirectIngest,
			IngestID: req.IngestID,
			Target:   d.qualify(req.Schema.TableName),
			Rows:     req.Records,
			Schema:   req.Schema,
			Options:  req.Options,
		}, nil
	}

	if req.Backend == nil {
		return drivers.ExecutionPlan{}, fmt.Errorf("iceberg: fast-path requires a Backend")
	}

	kind := drivers.KindParquetIngest
	if len(req.Schema.MergeKeys) > 0 {
		kind = drivers.KindParquetMerge
	}

	prefix := req.Backend.StagingPrefix(req.IngestID)
	return drivers.ExecutionPlan{
		Kind:     kind,
		IngestID: req.IngestID,
		Target:   d.qualify(req.Schema.TableName),
		Schema:   req.Schema,
		Staging: drivers.StagingRef{
			Backend: req.Backend,
			Prefix:  prefix,
			// StagingLocation handles the Scheme/Bucket/Path tuple
			// correctly (s3 vs. s3a), and inserts the bucket segment
			// the Driver's SQL needs.
			Location: req.Backend.StagingLocation(req.IngestID),
		},
		Options: req.Options,
	}, nil
}

func goTypeToIceberg(t any) (string, error) {
	return goReflectTypeToIceberg(t)
}
