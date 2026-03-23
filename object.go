package grub

import "github.com/zoobz-io/grub/internal/shared"

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
	Data        T                 `json:"data" atom:"data"`
}
