package migrations

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// lakeorm.sum manifest.
//
// Every MigrateGenerate call rewrites a lakeorm.sum file at the
// migrations-dir root. The format is:
//
//	h1:<base64(sha256(body))>
//	20260419153012_users.sql h1:<base64(sha256(file))>
//	20260420091523_orders.sql h1:<base64(sha256(file))>
//
// Line 1's hash covers every subsequent line. Any post-generation
// edit — renaming a file, editing a file, reordering lines —
// invalidates the top-line hash, so downstream tooling (a future
// `lakeorm migrate --check`, custom CI jobs) can detect drift
// with a single shell one-liner.
//
// lakeorm writes the manifest; verification is a reviewer / CI
// concern, not a runtime gate. The file on disk is the contract
// the reviewer signed off on.

// ManifestFilename is the name every generated manifest uses.
// Consumers that verify the manifest locate it by this name.
const ManifestFilename = "lakeorm.sum"

// WriteManifest scans dir for .sql files, computes each file's
// sha256, and writes lakeorm.sum. Called from the MigrateGenerate
// flow after every per-table file is written.
func WriteManifest(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	type fileHash struct {
		name string
		hash string
	}
	var hashes []fileHash
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		full := filepath.Join(dir, e.Name())
		f, err := os.Open(full)
		if err != nil {
			return err
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			_ = f.Close()
			return err
		}
		_ = f.Close()
		hashes = append(hashes, fileHash{
			name: e.Name(),
			hash: "h1:" + base64.StdEncoding.EncodeToString(h.Sum(nil)),
		})
	}
	sort.Slice(hashes, func(i, j int) bool { return hashes[i].name < hashes[j].name })

	var body strings.Builder
	for _, h := range hashes {
		fmt.Fprintf(&body, "%s %s\n", h.name, h.hash)
	}
	dirHash := sha256.Sum256([]byte(body.String()))

	var out strings.Builder
	fmt.Fprintf(&out, "h1:%s\n", base64.StdEncoding.EncodeToString(dirHash[:]))
	out.WriteString(body.String())

	return os.WriteFile(filepath.Join(dir, ManifestFilename), []byte(out.String()), 0o644)
}
