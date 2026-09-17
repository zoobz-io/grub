package grub

import (
	"strings"
	"time"

	"github.com/zoobz-io/grub/internal/shared"
)

// ObjectInfo is re-exported from internal/shared for the public API.
type ObjectInfo = shared.ObjectInfo

// Object wraps payload T with blob metadata for atomization.
// The entire structure is atomizable, enabling field-level operations
// on both metadata and payload.
type Object[T any] struct {
	Key         string            `json:"key" atom:"key"`
	ContentType string            `json:"content_type" atom:"content_type"`
	Size        int64             `json:"size" atom:"size"`
	ETag        string            `json:"etag,omitempty" atom:"etag"`
	Metadata    map[string]string `json:"metadata,omitempty" atom:"metadata"`

	// LastModified is the time the object was last written.
	// It is read-side only: populated by Bucket[T].Get and ignored on Put.
	LastModified time.Time `json:"last_modified,omitempty" atom:"last_modified"`

	Data T `json:"data" atom:"data"`
}

// Level is one level of a key hierarchy under a prefix.
type Level struct {
	// Objects are the keys directly under the prefix.
	Objects []ObjectInfo
	// Prefixes are the common prefixes directly under the prefix.
	// Each one ends with the delimiter.
	Prefixes []string
	// Next is the cursor for the next page. Empty means no more pages.
	Next string
}

// FoldLevel builds a Level from a flat listing of infos under prefix, where
// delimiter separates the levels. Keys directly under prefix become Objects;
// keys nested deeper collapse into common Prefixes, each ending with the
// delimiter. It is provided for BucketProvider implementations outside this
// repo that lack a native hierarchy listing; the library does not call it.
func FoldLevel(prefix, delimiter string, infos []ObjectInfo) *Level {
	level := &Level{}
	seen := make(map[string]struct{})
	for _, info := range infos {
		if !strings.HasPrefix(info.Key, prefix) {
			continue
		}
		rest := info.Key[len(prefix):]
		if delimiter == "" {
			level.Objects = append(level.Objects, info)
			continue
		}
		idx := strings.Index(rest, delimiter)
		if idx < 0 {
			level.Objects = append(level.Objects, info)
			continue
		}
		commonPrefix := prefix + rest[:idx+len(delimiter)]
		if _, ok := seen[commonPrefix]; ok {
			continue
		}
		seen[commonPrefix] = struct{}{}
		level.Prefixes = append(level.Prefixes, commonPrefix)
	}
	return level
}
