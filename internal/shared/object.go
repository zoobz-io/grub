// Package shared provides canonical type definitions used across grub modules.
package shared //nolint:revive // internal shared package is intentional

import "github.com/zoobz-io/atom"

// ObjectInfo holds provider-level metadata for blob storage.
// Used by BucketProvider implementations.
type ObjectInfo struct {
	Key         string
	ContentType string
	Size        int64
	ETag        string
	Metadata    map[string]string
}

// AtomicObject holds blob metadata with an atomized payload.
// Used by AtomicBucket for type-agnostic access to blob data.
type AtomicObject struct {
	Key         string
	ContentType string
	Size        int64
	ETag        string
	Metadata    map[string]string
	Data        *atom.Atom
}
