package atomix

import (
	"context"

	"github.com/google/uuid"
	"github.com/zoobzio/atom"
	"github.com/zoobzio/grub/internal/shared"
	"github.com/zoobzio/vecna"
)

// VectorProvider defines raw vector storage operations.
// Duplicated here to avoid import cycle with parent package.
type VectorProvider interface {
	Upsert(ctx context.Context, id uuid.UUID, vector []float32, metadata []byte) error
	UpsertBatch(ctx context.Context, vectors []shared.VectorRecord) error
	Get(ctx context.Context, id uuid.UUID) ([]float32, *shared.VectorInfo, error)
	Delete(ctx context.Context, id uuid.UUID) error
	DeleteBatch(ctx context.Context, ids []uuid.UUID) error
	Search(ctx context.Context, vector []float32, k int, filter map[string]any) ([]shared.VectorResult, error)
	Query(ctx context.Context, vector []float32, k int, filter *vecna.Filter) ([]shared.VectorResult, error)
	Filter(ctx context.Context, filter *vecna.Filter, limit int) ([]shared.VectorResult, error)
	List(ctx context.Context, limit int) ([]uuid.UUID, error)
	Exists(ctx context.Context, id uuid.UUID) (bool, error)
}

// Index provides atom-based vector storage operations.
// Satisfies the grub.AtomicIndex interface.
type Index[T any] struct {
	provider VectorProvider
	codec    Codec
	spec     atom.Spec
}

// NewIndex creates an atomic Index wrapper.
func NewIndex[T any](provider VectorProvider, codec Codec, spec atom.Spec) *Index[T] {
	return &Index[T]{
		provider: provider,
		codec:    codec,
		spec:     spec,
	}
}

// Spec returns the atom spec for this index's metadata type.
func (i *Index[T]) Spec() atom.Spec {
	return i.spec
}

// Get retrieves the vector at ID with atomized metadata.
func (i *Index[T]) Get(ctx context.Context, id uuid.UUID) (*shared.AtomicVector, error) {
	vector, info, err := i.provider.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	metadata, err := i.metadataToAtom(info.Metadata)
	if err != nil {
		return nil, err
	}
	return &shared.AtomicVector{
		ID:       info.ID,
		Vector:   vector,
		Score:    info.Score,
		Metadata: metadata,
	}, nil
}

// Upsert stores a vector with atomized metadata.
func (i *Index[T]) Upsert(ctx context.Context, id uuid.UUID, vector []float32, metadata *atom.Atom) error {
	m, err := i.atomToMetadata(metadata)
	if err != nil {
		return err
	}
	return i.provider.Upsert(ctx, id, vector, m)
}

// Delete removes the vector at ID.
func (i *Index[T]) Delete(ctx context.Context, id uuid.UUID) error {
	return i.provider.Delete(ctx, id)
}

// Exists checks whether an ID exists.
func (i *Index[T]) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	return i.provider.Exists(ctx, id)
}

// Search performs similarity search returning atomized results.
func (i *Index[T]) Search(ctx context.Context, vector []float32, k int, filter *atom.Atom) ([]shared.AtomicVector, error) {
	filterMap, err := i.atomToFilter(filter)
	if err != nil {
		return nil, err
	}
	results, err := i.provider.Search(ctx, vector, k, filterMap)
	if err != nil {
		return nil, err
	}
	atomicResults := make([]shared.AtomicVector, len(results))
	for idx, r := range results {
		metadata, err := i.metadataToAtom(r.Metadata)
		if err != nil {
			return nil, err
		}
		atomicResults[idx] = shared.AtomicVector{
			ID:       r.ID,
			Vector:   r.Vector,
			Score:    r.Score,
			Metadata: metadata,
		}
	}
	return atomicResults, nil
}

// Query performs similarity search with vecna filter support.
func (i *Index[T]) Query(ctx context.Context, vector []float32, k int, filter *vecna.Filter) ([]shared.AtomicVector, error) {
	results, err := i.provider.Query(ctx, vector, k, filter)
	if err != nil {
		return nil, err
	}
	atomicResults := make([]shared.AtomicVector, len(results))
	for idx, r := range results {
		metadata, err := i.metadataToAtom(r.Metadata)
		if err != nil {
			return nil, err
		}
		atomicResults[idx] = shared.AtomicVector{
			ID:       r.ID,
			Vector:   r.Vector,
			Score:    r.Score,
			Metadata: metadata,
		}
	}
	return atomicResults, nil
}

// Filter returns vectors matching the metadata filter without similarity search.
func (i *Index[T]) Filter(ctx context.Context, filter *vecna.Filter, limit int) ([]shared.AtomicVector, error) {
	results, err := i.provider.Filter(ctx, filter, limit)
	if err != nil {
		return nil, err
	}
	atomicResults := make([]shared.AtomicVector, len(results))
	for idx, r := range results {
		metadata, err := i.metadataToAtom(r.Metadata)
		if err != nil {
			return nil, err
		}
		atomicResults[idx] = shared.AtomicVector{
			ID:       r.ID,
			Vector:   r.Vector,
			Score:    r.Score,
			Metadata: metadata,
		}
	}
	return atomicResults, nil
}

// metadataToAtom converts bytes metadata to an Atom via T.
func (i *Index[T]) metadataToAtom(data []byte) (*atom.Atom, error) {
	if data == nil {
		return nil, nil
	}
	var value T
	if err := i.codec.Decode(data, &value); err != nil {
		return nil, err
	}
	atomizer, err := atom.Use[T]()
	if err != nil {
		return nil, err
	}
	return atomizer.Atomize(&value), nil
}

// atomToMetadata converts an Atom to bytes metadata via T.
func (i *Index[T]) atomToMetadata(a *atom.Atom) ([]byte, error) {
	if a == nil {
		return nil, nil
	}
	atomizer, err := atom.Use[T]()
	if err != nil {
		return nil, err
	}
	value, err := atomizer.Deatomize(a)
	if err != nil {
		return nil, err
	}
	return i.codec.Encode(value)
}

// atomToFilter converts an Atom to map[string]any for search filtering.
func (i *Index[T]) atomToFilter(a *atom.Atom) (map[string]any, error) {
	if a == nil {
		return nil, nil
	}
	atomizer, err := atom.Use[T]()
	if err != nil {
		return nil, err
	}
	value, err := atomizer.Deatomize(a)
	if err != nil {
		return nil, err
	}
	data, err := i.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := i.codec.Decode(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}
