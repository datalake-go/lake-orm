//go:build e2e

package teste2e

import (
	"os"
	"testing"
)

func envOr(t *testing.T, key, def string) string {
	t.Helper()
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
