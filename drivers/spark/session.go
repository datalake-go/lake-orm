package spark

import (
	"context"
	"errors"
	"fmt"
	"sync"

	scsql "github.com/datalake-go/spark-connect-go/spark/sql"
	"github.com/rs/zerolog"
)

// sessionFactory produces a fresh Spark Connect SparkSession. Supplied
// by remote.go (plain URL) or databricks.go (URL with OAuth token).
type sessionFactory func(ctx context.Context) (scsql.SparkSession, error)

// SessionPool manages a bounded pool of SparkSession instances.
//
// Spark Connect sessions are stateful on the server — each holds its
// own catalog context, temp views, config overrides. Three options:
//
//   - One session per goroutine. Simple model, state-isolated, but
//     cluster-side overhead (auth, init) is per-session — painful at
//     high concurrency.
//   - One shared session. Cheap, but any operation with side effects
//     (SET, CREATE TEMP VIEW, catalog use) races.
//   - A bounded pool (this type). Serializes state within a borrow,
//     caps cluster-side overhead at `size`.
//
// Default size is 8; tune via lakeorm.WithSessionPoolSize.
type SessionPool struct {
	factory sessionFactory
	size    int

	mu     sync.Mutex
	idle   []scsql.SparkSession
	closed bool
}

func newSessionPool(size int, factory sessionFactory) *SessionPool {
	if size <= 0 {
		size = 1
	}
	return &SessionPool{
		factory: factory,
		size:    size,
		idle:    make([]scsql.SparkSession, 0, size),
	}
}

// Borrow returns a session from the pool, creating one on demand up to
// the configured size. Callers must call Return() when done.
func (p *SessionPool) Borrow(ctx context.Context) (scsql.SparkSession, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("lakeorm/spark: session pool closed")
	}
	if n := len(p.idle); n > 0 {
		s := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return s, nil
	}
	p.mu.Unlock()

	return p.factory(ctx)
}

// Return hands a session back to the pool. If the pool is full, the
// session is stopped.
func (p *SessionPool) Return(s scsql.SparkSession) {
	if s == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = s.Stop()
		return
	}
	if len(p.idle) < p.size {
		p.idle = append(p.idle, s)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	_ = s.Stop()
}

// applyConfs runs `SET key = value` for every entry in confs against
// a fresh SparkSession. Used by the Remote / Databricks factories to
// bake in user-supplied spark.* tunings (Hadoop S3A creds, Arrow
// batch sizes, scheduler pool) once at session creation. A malformed
// key/value is a hard failure — the session is unusable if its conf
// didn't apply, and surfacing it here is less confusing than failing
// later with a misleading Spark error.
func applyConfs(ctx context.Context, s scsql.SparkSession, confs map[string]string, logger zerolog.Logger) error {
	for k, v := range confs {
		df, err := s.Sql(ctx, fmt.Sprintf("SET %s = %s", k, quoteSQL(v)))
		if err != nil {
			return fmt.Errorf("set %s: %w", k, err)
		}
		if _, err := df.Collect(ctx); err != nil {
			return fmt.Errorf("collect SET %s: %w", k, err)
		}
		logger.Debug().Str("key", k).Str("value", v).Msg("applied session conf")
	}
	return nil
}

// Close stops every idle session and refuses further borrows.
func (p *SessionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var firstErr error
	for _, s := range p.idle {
		if err := s.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.idle = nil
	return firstErr
}
