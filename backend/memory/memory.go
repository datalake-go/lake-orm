// Package memory provides an in-memory Backend, useful for tests and
// for the fast-path regression harness. Data lives in a map protected
// by a sync.RWMutex and is lost when the Backend is garbage-collected.
package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/datalake-go/lake-orm/types"
)

// Backend is an in-memory implementation of lakeorm.Backend.
type Backend struct {
	mu   sync.RWMutex
	data map[string][]byte
	root string
}

// New constructs a memory backend with the given logical bucket name.
// The name is only used for RootURI() rendering — it does not have to
// correspond to anything real.
func New(name string) *Backend {
	if name == "" {
		name = "in-memory"
	}
	return &Backend{
		data: map[string][]byte{},
		root: name,
	}
}

func (b *Backend) Name() string    { return "memory" }
func (b *Backend) RootURI() string { return "memory://" + b.root }

func (b *Backend) TableLocation(tableName string) types.Location {
	return types.Location{Scheme: "memory", Bucket: b.root, Path: "tables/" + tableName}
}

func (b *Backend) StagingPrefix(ingestID string) string {
	return "staging/" + ingestID
}

func (b *Backend) StagingLocation(ingestID string) types.Location {
	return types.Location{Scheme: "memory", Bucket: b.root, Path: b.StagingPrefix(ingestID)}
}

func (b *Backend) Writer(_ context.Context, key string) (io.WriteCloser, error) {
	return &memWriter{backend: b, key: key}, nil
}

func (b *Backend) Reader(_ context.Context, key string) (io.ReadCloser, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	v, ok := b.data[key]
	if !ok {
		return nil, fmt.Errorf("memory: key not found: %s", key)
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), v...))), nil
}

func (b *Backend) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.data, key)
	return nil
}

func (b *Backend) List(_ context.Context, prefix string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0)
	for k := range b.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (b *Backend) CleanupStaging(ctx context.Context, prefix string) error {
	keys, err := b.List(ctx, prefix)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range keys {
		delete(b.data, k)
	}
	return nil
}

type memWriter struct {
	backend *Backend
	key     string
	buf     bytes.Buffer
}

func (w *memWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *memWriter) Close() error {
	w.backend.mu.Lock()
	defer w.backend.mu.Unlock()
	w.backend.data[w.key] = append([]byte(nil), w.buf.Bytes()...)
	return nil
}
