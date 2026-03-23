// Package shared contains canonical type definitions shared across grub.
package shared //nolint:revive // internal shared package is intentional

import (
	"github.com/google/uuid"
	"github.com/zoobz-io/atom"
)

// VectorInfo holds provider-level metadata for vector storage.
type VectorInfo struct {
	// ID is the unique identifier for the vector.
	ID uuid.UUID

	// Dimension is the vector dimensionality.
	Dimension int

	// Score is the similarity score (populated on search results).
	Score float32

	// Metadata holds user-defined metadata as raw bytes.
	Metadata []byte
}

// DistanceMetric defines the distance function for similarity search.
type DistanceMetric string

const (
	// DistanceL2 represents Euclidean (L2) distance.
	DistanceL2 DistanceMetric = "l2"

	// DistanceCosine represents cosine similarity distance.
	DistanceCosine DistanceMetric = "cosine"

	// DistanceInnerProduct represents inner product (dot product) distance.
	DistanceInnerProduct DistanceMetric = "inner_product"
)

// VectorRecord represents a vector for batch operations.
type VectorRecord struct {
	ID       uuid.UUID
	Vector   []float32
	Metadata []byte
}

// VectorResult represents a search result with score.
type VectorResult struct {
	ID       uuid.UUID
	Vector   []float32
	Metadata []byte
	Score    float32
}

// AtomicVector holds vector data with an atomized metadata payload.
// Used by AtomicIndex for type-agnostic access to vector data.
type AtomicVector struct {
	ID       uuid.UUID
	Vector   []float32
	Score    float32
	Metadata *atom.Atom
}
