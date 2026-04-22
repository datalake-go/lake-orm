package types

import (
	"fmt"
	"strings"
)

// Location is a typed storage URI — scheme + bucket + path. It's the
// coordinate that Client and Driver must agree on at the string level,
// regardless of whether they resolve to the same physical endpoint via
// the same credentials. See lakeorm.Verify for the reachability probe
// that surfaces the mismatch cleanly.
type Location struct {
	Scheme string // "s3", "gs", "file", "memory"
	Bucket string // bucket/container/root — empty for "file" and "memory"
	Path   string // object key or filesystem path under Bucket
}

// ParseLocation parses a canonical URI ("s3://bucket/key",
// "gs://bucket/key", "file:///abs/path", "memory://name/key") into a Location.
func ParseLocation(uri string) (Location, error) {
	i := strings.Index(uri, "://")
	if i < 0 {
		return Location{}, fmt.Errorf("invalid location %q: missing scheme", uri)
	}
	scheme := uri[:i]
	rest := uri[i+3:]
	switch scheme {
	case "s3", "gs", "memory":
		j := strings.IndexByte(rest, '/')
		if j < 0 {
			return Location{Scheme: scheme, Bucket: rest}, nil
		}
		return Location{Scheme: scheme, Bucket: rest[:j], Path: rest[j+1:]}, nil
	case "file":
		// file:///abs/path — bucket empty
		return Location{Scheme: scheme, Path: strings.TrimPrefix(rest, "/")}, nil
	default:
		return Location{}, fmt.Errorf("unsupported scheme %q in %q", scheme, uri)
	}
}

// URI renders the Location back to its canonical string.
func (l Location) URI() string {
	switch l.Scheme {
	case "file":
		return "file:///" + l.Path
	case "":
		return ""
	default:
		if l.Bucket == "" {
			return l.Scheme + "://" + l.Path
		}
		if l.Path == "" {
			return l.Scheme + "://" + l.Bucket
		}
		return l.Scheme + "://" + l.Bucket + "/" + l.Path
	}
}

func (l Location) String() string { return l.URI() }

// Join returns a new Location with additional path components appended.
func (l Location) Join(parts ...string) Location {
	out := l
	out.Path = joinPath(append([]string{l.Path}, parts...)...)
	return out
}

func joinPath(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.Trim(p, "/")
		if p == "" {
			continue
		}
		clean = append(clean, p)
	}
	return strings.Join(clean, "/")
}
