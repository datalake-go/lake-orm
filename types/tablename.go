package types

import (
	"fmt"
	"strings"
)

// SparkTableName is a validated three-part Spark table identifier
// (catalog.database.table). Catalog is optional; two-part names are
// normalized to default_catalog.database.table by the Dialect layer.
type SparkTableName struct {
	Catalog  string
	Database string
	Table    string
}

// ParseSparkTableName parses "catalog.database.table", "database.table",
// or "table". Multi-part names are validated; bare names are returned
// with empty Catalog/Database for the caller to resolve via defaults.
func ParseSparkTableName(s string) (SparkTableName, error) {
	if s == "" {
		return SparkTableName{}, fmt.Errorf("empty table name")
	}
	parts := strings.Split(s, ".")
	switch len(parts) {
	case 1:
		return SparkTableName{Table: parts[0]}, nil
	case 2:
		return SparkTableName{Database: parts[0], Table: parts[1]}, nil
	case 3:
		return SparkTableName{Catalog: parts[0], Database: parts[1], Table: parts[2]}, nil
	default:
		return SparkTableName{}, fmt.Errorf("invalid table name %q: expected catalog.database.table", s)
	}
}

func (n SparkTableName) String() string {
	parts := make([]string, 0, 3)
	if n.Catalog != "" {
		parts = append(parts, n.Catalog)
	}
	if n.Database != "" {
		parts = append(parts, n.Database)
	}
	parts = append(parts, n.Table)
	return strings.Join(parts, ".")
}

// Qualified returns the full catalog.database.table form, substituting
// defaults for any empty component. Used by Dialect.PlanQuery when
// interpolating SQL.
func (n SparkTableName) Qualified(defaultCatalog, defaultDatabase string) string {
	cat := n.Catalog
	if cat == "" {
		cat = defaultCatalog
	}
	db := n.Database
	if db == "" {
		db = defaultDatabase
	}
	if cat != "" {
		return cat + "." + db + "." + n.Table
	}
	if db != "" {
		return db + "." + n.Table
	}
	return n.Table
}
