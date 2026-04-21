// Package file provides a local-disk Backend. Writes are atomic via
// temp-file + fsync + rename (same discipline as Aegis's FileSystemBackend).
package file

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/datalake-go/lake-orm/types"
)

// Backend is a local-filesystem implementation of lakeorm.Backend. Root is
// the lakehouse root directory; table data and staging both live beneath.
type Backend struct {
	root string
}

// New constructs a file backend rooted at the given absolute path.
// Creates the directory if needed.
func New(root string) (*Backend, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("file backend: resolve root: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("file backend: mkdir root: %w", err)
	}
	return &Backend{root: abs}, nil
}

func (b *Backend) Name() string    { return "file" }
func (b *Backend) RootURI() string { return "file://" + b.root }

func (b *Backend) TableLocation(tableName string) types.Location {
	return types.Location{Scheme: "file", Path: filepath.Join(b.root, "tables", tableName)}
}

func (b *Backend) StagingPrefix(ingestID string) string {
	return filepath.Join("staging", ingestID)
}

func (b *Backend) StagingLocation(ingestID string) types.Location {
	return types.Location{Scheme: "file", Path: filepath.Join(b.root, b.StagingPrefix(ingestID))}
}

func (b *Backend) Writer(_ context.Context, key string) (io.WriteCloser, error) {
	full := filepath.Join(b.root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(full), err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(full), ".dorm-*")
	if err != nil {
		return nil, fmt.Errorf("create temp: %w", err)
	}
	return &atomicWriter{f: tmp, target: full}, nil
}

func (b *Backend) Reader(_ context.Context, key string) (io.ReadCloser, error) {
	full := filepath.Join(b.root, key)
	return os.Open(full)
}

func (b *Backend) Delete(_ context.Context, key string) error {
	full := filepath.Join(b.root, key)
	err := os.Remove(full)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (b *Backend) List(_ context.Context, prefix string) ([]string, error) {
	base := filepath.Join(b.root, prefix)
	var out []string
	err := filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(b.root, path)
		if err != nil {
			return err
		}
		out = append(out, rel)
		return nil
	})
	return out, err
}

func (b *Backend) CleanupStaging(_ context.Context, prefix string) error {
	err := os.RemoveAll(filepath.Join(b.root, prefix))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

type atomicWriter struct {
	f      *os.File
	target string
}

func (w *atomicWriter) Write(p []byte) (int, error) { return w.f.Write(p) }
func (w *atomicWriter) Close() error {
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		_ = os.Remove(w.f.Name())
		return fmt.Errorf("fsync: %w", err)
	}
	if err := w.f.Close(); err != nil {
		_ = os.Remove(w.f.Name())
		return fmt.Errorf("close temp: %w", err)
	}
	if err := os.Rename(w.f.Name(), w.target); err != nil {
		_ = os.Remove(w.f.Name())
		return fmt.Errorf("rename to %s: %w", w.target, err)
	}
	return nil
}
