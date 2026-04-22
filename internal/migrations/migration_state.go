package migrations

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

// stateField is the JSON-friendly shadow of Field. reflect.Type
// doesn't marshal, so we record the SQL type string the generator
// emitted and the Go type name for symmetry with Django's
// MigrationLoader-style replay: the state is whatever the prior
// file claimed, regardless of how the struct looks today.
type stateField struct {
	Column   string `json:"column"`
	SQLType  string `json:"sql_type"`
	GoType   string `json:"go_type,omitempty"`
	Nullable bool   `json:"nullable"`
	PK       bool   `json:"pk,omitempty"`
	MergeKey bool   `json:"merge_key,omitempty"`
}

// tableState is the JSON shape of one table's schema at the point a
// migration file wrote it — the target state AFTER applying every Up
// statement in that file. A single file can declare multiple tables.
type tableState struct {
	TableName string       `json:"table_name"`
	Fields    []stateField `json:"fields"`
}

// stateHeaderPrefix is the one-line marker Generate emits into the
// file header. Parseable with a single strings.HasPrefix check at
// replay time.
const stateHeaderPrefix = "-- State-JSON: "

// EncodeState returns the one-line State-JSON header for a Schema.
// Generate embeds this in the file after the Dialect line so
// subsequent MigrateGenerate runs can reconstruct the target state
// without needing a DESCRIBE TABLE round-trip.
//
// Matches Django's MigrationLoader pattern: the state recorded in
// the file is the source of truth for "what this migration
// produced." The model-class-at-the-time and the catalog-state are
// both irrelevant during replay; only the file's declared output
// matters.
func EncodeState(s *Schema) (string, error) {
	if s == nil {
		return "", nil
	}
	ts := tableState{
		TableName: s.TableName,
		Fields:    make([]stateField, 0, len(s.Fields)),
	}
	for _, f := range s.Fields {
		if f.Ignored {
			continue
		}
		fld := stateField{
			Column:   f.Column,
			SQLType:  goTypeToSQL(f.GoType),
			Nullable: f.Nullable,
			PK:       f.PK,
			MergeKey: f.MergeKey,
		}
		if f.GoType != nil {
			fld.GoType = f.GoType.String()
		}
		ts.Fields = append(ts.Fields, fld)
	}
	// Sort for deterministic output — the replay reducer cares about
	// presence/absence, not order, and a stable encoding makes diffs
	// human-friendly when reading the file directly.
	sort.Slice(ts.Fields, func(i, j int) bool { return ts.Fields[i].Column < ts.Fields[j].Column })

	b, err := json.Marshal(ts)
	if err != nil {
		return "", err
	}
	return stateHeaderPrefix + string(b), nil
}

// ReplayLatestState scans dir for migration files whose names match
// the YYYYMMDDHHMMSS_<slug>.sql pattern, reads the State-JSON
// header from the most recent file that declared the given
// tableName, and returns the reconstructed Schema.
//
// Returns (nil, nil) if no prior file declares tableName — callers
// treat this as a bootstrap (the first MigrateGenerate run for this
// table).
//
// Parse failures on individual files are reported as errors rather
// than skipped — a corrupt file is a bug worth surfacing.
func ReplayLatestState(dir, tableName string) (*Schema, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	type fileEntry struct {
		name string
		path string
	}
	var sqls []fileEntry
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		sqls = append(sqls, fileEntry{name: e.Name(), path: filepath.Join(dir, e.Name())})
	}
	// Filename timestamp prefix gives us chronological order; sort
	// descending so the first match is the most recent declaration.
	sort.Slice(sqls, func(i, j int) bool { return sqls[i].name > sqls[j].name })

	for _, f := range sqls {
		state, err := parseStateFromFile(f.path)
		if err != nil {
			return nil, fmt.Errorf("replay %s: %w", f.name, err)
		}
		if state != nil && state.TableName == tableName {
			return stateToSchema(state), nil
		}
	}
	return nil, nil
}

// parseStateFromFile reads the first State-JSON header in a
// migration file (there's at most one per file in the v0
// generator). Returns nil if the file has no State-JSON line —
// typical for hand-authored migrations that don't declare a
// target state.
func parseStateFromFile(path string) (*tableState, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseStateFromReader(f)
}

// parseStateFromReader is the testable core of parseStateFromFile.
// Stops at the first `-- +goose Up` marker so we only look in the
// header — a stray State-JSON comment inside the Up block doesn't
// register.
func parseStateFromReader(r io.Reader) (*tableState, error) {
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 64*1024), 1024*1024)
	for scan.Scan() {
		line := scan.Text()
		if strings.HasPrefix(line, "-- +goose Up") {
			return nil, nil
		}
		if !strings.HasPrefix(line, stateHeaderPrefix) {
			continue
		}
		payload := strings.TrimPrefix(line, stateHeaderPrefix)
		var ts tableState
		if err := json.Unmarshal([]byte(payload), &ts); err != nil {
			return nil, fmt.Errorf("malformed State-JSON: %w", err)
		}
		return &ts, nil
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

// stateToSchema rebuilds a migrate.Schema from a parsed tableState.
// GoType is left nil — reflect.Type doesn't survive the JSON
// round-trip, and Diff only uses the SQL-type string for
// widen/narrow classification, which we can compute from SQLType
// alone when the need arises in v1.
func stateToSchema(ts *tableState) *Schema {
	if ts == nil {
		return nil
	}
	fields := make([]Field, 0, len(ts.Fields))
	for _, f := range ts.Fields {
		fields = append(fields, Field{
			Column:   f.Column,
			Nullable: f.Nullable,
			PK:       f.PK,
			MergeKey: f.MergeKey,
		})
	}
	return &Schema{TableName: ts.TableName, Fields: fields}
}

// goTypeToSQL emits a coarse SQL type for a Go type. Matches the
// type strings the iceberg/delta dialects produce via their own
// reflection; duplicating the mapping here avoids a cyclic import
// (dialect packages depend on lakeorm, lakeorm on this subpackage).
// The mapping is deliberately conservative — unfamiliar types fall
// through as the Go type name so the generated State-JSON still
// round-trips unambiguously.
func goTypeToSQL(t reflect.Type) string {
	if t == nil {
		return ""
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return "STRING"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int32:
		return "INT"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Float32:
		return "FLOAT"
	case reflect.Float64:
		return "DOUBLE"
	default:
		return t.String()
	}
}
