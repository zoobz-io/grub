// Package shared provides canonical type definitions used across grub modules.
package shared //nolint:revive // internal shared package is intentional

import (
	"time"
)

// ObjectInfo holds provider-level metadata for blob storage.
// Used by BucketProvider implementations.
type ObjectInfo struct {
	Key         string
	ContentType string
	Size        int64
	ETag        string
	Metadata    map[string]string

	// LastModified is the time the object was last written.
	// It is read-side only: set by Get, List, ListPage, ListLevel, and Stat,
	// and ignored on Put/PutStream.
	LastModified time.Time
}
