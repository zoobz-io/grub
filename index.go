package grub

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/zoobz-io/atom"
	"github.com/zoobz-io/grub/internal/atomix"
	"github.com/zoobz-io/vecna"
)

// Index provides type-safe vector storage operations with metadata of type T.
// Wraps a VectorProvider, handling serialization of T to/from map[string]any.
type Index[T any] struct {
	provider   VectorProvider
	codec      Codec
	atomic     *atomix.Index[T]
	atomicOnce sync.Once
}

// NewIndex creates an Index for metadata type T backed by the given provider.
// Uses JSON codec by default.
func NewIndex[T any](provider VectorProvider) *Index[T] {
	return &Index[T]{
		provider: provider,
		codec:    JSONCodec{},
	}
}

// NewIndexWithCodec creates an Index for metadata type T with a custom codec.
func NewIndexWithCodec[T any](provider VectorProvider, codec Codec) *Index[T] {
	return &Index[T]{
		provider: provider,
		codec:    codec,
	}
}

// Upsert stores or updates a vector with associated metadata.
// If the ID exists, the vector and metadata are replaced.
func (i *Index[T]) Upsert(ctx context.Context, id uuid.UUID, vector []float32, metadata *T) error {
	if metadata != nil {
		if err := callBeforeSave(ctx, metadata); err != nil {
			return err
		}
	}
	m, err := i.encodeMetadata(metadata)
	if err != nil {
		return err
	}
	if err := i.provider.Upsert(ctx, id, vector, m); err != nil {
		return err
	}
	if metadata != nil {
		return callAfterSave(ctx, metadata)
	}
	return nil
}

// UpsertBatch stores or updates multiple vectors.
func (i *Index[T]) UpsertBatch(ctx context.Context, vectors []Vector[T]) error {
	records := make([]VectorRecord, len(vectors))
	for idx := range vectors {
		if err := callBeforeSave(ctx, &vectors[idx].Metadata); err != nil {
			return err
		}
		m, err := i.encodeMetadata(&vectors[idx].Metadata)
		if err != nil {
			return err
		}
		records[idx] = VectorRecord{
			ID:       vectors[idx].ID,
			Vector:   vectors[idx].Vector,
			Metadata: m,
		}
	}
	if err := i.provider.UpsertBatch(ctx, records); err != nil {
		return err
	}
	for idx := range vectors {
		if err := callAfterSave(ctx, &vectors[idx].Metadata); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves a vector by ID.
// Returns ErrNotFound if the ID does not exist.
func (i *Index[T]) Get(ctx context.Context, id uuid.UUID) (*Vector[T], error) {
	vector, info, err := i.provider.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	var metadata T
	if err := i.decodeMetadata(info.Metadata, &metadata); err != nil {
		return nil, err
	}
	if err := callAfterLoad(ctx, &metadata); err != nil {
		return nil, err
	}
	return &Vector[T]{
		ID:       info.ID,
		Vector:   vector,
		Score:    info.Score,
		Metadata: metadata,
	}, nil
}

// Delete removes a vector by ID.
// Returns ErrNotFound if the ID does not exist.
func (i *Index[T]) Delete(ctx context.Context, id uuid.UUID) error {
	if err := callBeforeDelete[T](ctx); err != nil {
		return err
	}
	if err := i.provider.Delete(ctx, id); err != nil {
		return err
	}
	return callAfterDelete[T](ctx)
}

// DeleteBatch removes multiple vectors by ID.
// Non-existent IDs are silently ignored.
func (i *Index[T]) DeleteBatch(ctx context.Context, ids []uuid.UUID) error {
	if err := callBeforeDelete[T](ctx); err != nil {
		return err
	}
	if err := i.provider.DeleteBatch(ctx, ids); err != nil {
		return err
	}
	return callAfterDelete[T](ctx)
}

// Search performs similarity search and returns the k nearest neighbors.
// filter is optional metadata filtering (nil means no filter).
func (i *Index[T]) Search(ctx context.Context, vector []float32, k int, filter *T) ([]*Vector[T], error) {
	filterMap, err := i.encodeFilter(filter)
	if err != nil {
		return nil, err
	}
	results, err := i.provider.Search(ctx, vector, k, filterMap)
	if err != nil {
		return nil, err
	}
	vectors := make([]*Vector[T], len(results))
	for idx, r := range results {
		var metadata T
		if err := i.decodeMetadata(r.Metadata, &metadata); err != nil {
			return nil, err
		}
		if err := callAfterLoad(ctx, &metadata); err != nil {
			return nil, err
		}
		vectors[idx] = &Vector[T]{
			ID:       r.ID,
			Vector:   r.Vector,
			Score:    r.Score,
			Metadata: metadata,
		}
	}
	return vectors, nil
}

// Query performs similarity search with vecna filter support.
// Returns ErrInvalidQuery if the filter contains validation errors.
// Returns ErrOperatorNotSupported if the provider doesn't support an operator.
func (i *Index[T]) Query(ctx context.Context, vector []float32, k int, filter *vecna.Filter) ([]*Vector[T], error) {
	results, err := i.provider.Query(ctx, vector, k, filter)
	if err != nil {
		return nil, err
	}
	vectors := make([]*Vector[T], len(results))
	for idx, r := range results {
		var metadata T
		if err := i.decodeMetadata(r.Metadata, &metadata); err != nil {
			return nil, err
		}
		if err := callAfterLoad(ctx, &metadata); err != nil {
			return nil, err
		}
		vectors[idx] = &Vector[T]{
			ID:       r.ID,
			Vector:   r.Vector,
			Score:    r.Score,
			Metadata: metadata,
		}
	}
	return vectors, nil
}

// Filter returns vectors matching the metadata filter without similarity search.
// Result ordering is provider-dependent and not guaranteed.
// Limit of 0 returns all matching vectors.
// Returns ErrFilterNotSupported if the provider cannot perform metadata-only filtering.
func (i *Index[T]) Filter(ctx context.Context, filter *vecna.Filter, limit int) ([]*Vector[T], error) {
	results, err := i.provider.Filter(ctx, filter, limit)
	if err != nil {
		return nil, err
	}
	vectors := make([]*Vector[T], len(results))
	for idx, r := range results {
		var metadata T
		if err := i.decodeMetadata(r.Metadata, &metadata); err != nil {
			return nil, err
		}
		if err := callAfterLoad(ctx, &metadata); err != nil {
			return nil, err
		}
		vectors[idx] = &Vector[T]{
			ID:       r.ID,
			Vector:   r.Vector,
			Score:    r.Score,
			Metadata: metadata,
		}
	}
	return vectors, nil
}

// List returns vector IDs.
// Limit of 0 means no limit.
func (i *Index[T]) List(ctx context.Context, limit int) ([]uuid.UUID, error) {
	return i.provider.List(ctx, limit)
}

// Exists checks whether a vector ID exists.
func (i *Index[T]) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	return i.provider.Exists(ctx, id)
}

// Atomic returns an atom-based view of this index.
// The returned atomix.Index satisfies the AtomicIndex interface.
// The instance is created once and cached for subsequent calls.
// Panics if T is not atomizable (a programmer error).
func (i *Index[T]) Atomic() *atomix.Index[T] {
	i.atomicOnce.Do(func() {
		atomizer, err := atom.Use[T]()
		if err != nil {
			panic("grub: invalid type for atomization: " + err.Error())
		}
		i.atomic = atomix.NewIndex[T](i.provider, i.codec, atomizer.Spec())
	})
	return i.atomic
}

// encodeMetadata converts typed metadata to bytes via codec.
func (i *Index[T]) encodeMetadata(metadata *T) ([]byte, error) {
	if metadata == nil {
		return nil, nil
	}
	return i.codec.Encode(metadata)
}

// decodeMetadata converts bytes to typed metadata via codec.
func (i *Index[T]) decodeMetadata(data []byte, metadata *T) error {
	if data == nil {
		return nil
	}
	return i.codec.Decode(data, metadata)
}

// encodeFilter converts typed filter to map[string]any via codec for search operations.
func (i *Index[T]) encodeFilter(filter *T) (map[string]any, error) {
	if filter == nil {
		return nil, nil
	}
	data, err := i.codec.Encode(filter)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := i.codec.Decode(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}
