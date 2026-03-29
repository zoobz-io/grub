package grub

import (
	"context"
	"sync"

	"github.com/zoobz-io/atom"
	"github.com/zoobz-io/grub/internal/atomix"
	"github.com/zoobz-io/lucene"
)

// Document represents a search document with ID and typed content.
type Document[T any] struct {
	ID      string
	Content T
	Score   float64 // Populated on search results
}

// SearchResult contains search response data.
type SearchResult[T any] struct {
	Hits     []*Document[T]
	Total    int64
	MaxScore float64
	// Aggregations are returned as raw JSON for flexibility.
	Aggregations map[string]any
	// TypedAggs contains typed aggregation results parsed using
	// the aggregation definitions from the search request.
	// Nil when no aggregations were requested or parsed.
	TypedAggs []AggResult
}

// Agg returns the typed aggregation result by name, or nil if not found.
func (r *SearchResult[T]) Agg(name string) *AggResult {
	for i := range r.TypedAggs {
		if r.TypedAggs[i].Name == name {
			return &r.TypedAggs[i]
		}
	}
	return nil
}

// Search provides type-safe search operations for documents of type T.
// Wraps a SearchProvider, handling serialization of T to/from bytes.
type Search[T any] struct {
	provider   SearchProvider
	index      string
	codec      Codec
	builder    *lucene.Builder[T]
	atomic     *atomix.Search[T]
	atomicOnce sync.Once
}

// NewSearch creates a Search for type T backed by the given provider.
// Uses JSON codec by default.
func NewSearch[T any](provider SearchProvider, index string) *Search[T] {
	return &Search[T]{
		provider: provider,
		index:    index,
		codec:    JSONCodec{},
		builder:  lucene.New[T](),
	}
}

// NewSearchWithCodec creates a Search for type T with a custom codec.
func NewSearchWithCodec[T any](provider SearchProvider, index string, codec Codec) *Search[T] {
	return &Search[T]{
		provider: provider,
		index:    index,
		codec:    codec,
		builder:  lucene.New[T](),
	}
}

// Index stores a document with the given ID.
// If the ID exists, the document is replaced.
func (s *Search[T]) Index(ctx context.Context, id string, doc *T) error {
	if err := callBeforeSave(ctx, doc); err != nil {
		return err
	}
	data, err := s.codec.Encode(doc)
	if err != nil {
		return err
	}
	if err := s.provider.Index(ctx, s.index, id, data); err != nil {
		return err
	}
	return callAfterSave(ctx, doc)
}

// IndexBatch stores multiple documents.
func (s *Search[T]) IndexBatch(ctx context.Context, docs map[string]*T) error {
	items := make(map[string][]byte, len(docs))
	for id, doc := range docs {
		if err := callBeforeSave(ctx, doc); err != nil {
			return err
		}
		data, err := s.codec.Encode(doc)
		if err != nil {
			return err
		}
		items[id] = data
	}
	if err := s.provider.IndexBatch(ctx, s.index, items); err != nil {
		return err
	}
	for _, doc := range docs {
		if err := callAfterSave(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves a document by ID.
// Returns ErrNotFound if the ID does not exist.
func (s *Search[T]) Get(ctx context.Context, id string) (*Document[T], error) {
	data, err := s.provider.Get(ctx, s.index, id)
	if err != nil {
		return nil, err
	}
	var content T
	if err := s.codec.Decode(data, &content); err != nil {
		return nil, err
	}
	if err := callAfterLoad(ctx, &content); err != nil {
		return nil, err
	}
	return &Document[T]{
		ID:      id,
		Content: content,
	}, nil
}

// Delete removes a document by ID.
// Returns ErrNotFound if the ID does not exist.
func (s *Search[T]) Delete(ctx context.Context, id string) error {
	if err := callBeforeDelete[T](ctx); err != nil {
		return err
	}
	if err := s.provider.Delete(ctx, s.index, id); err != nil {
		return err
	}
	return callAfterDelete[T](ctx)
}

// DeleteBatch removes multiple documents by ID.
// Non-existent IDs are silently ignored.
func (s *Search[T]) DeleteBatch(ctx context.Context, ids []string) error {
	if err := callBeforeDelete[T](ctx); err != nil {
		return err
	}
	if err := s.provider.DeleteBatch(ctx, s.index, ids); err != nil {
		return err
	}
	return callAfterDelete[T](ctx)
}

// Exists checks whether a document ID exists.
func (s *Search[T]) Exists(ctx context.Context, id string) (bool, error) {
	return s.provider.Exists(ctx, s.index, id)
}

// Query returns the type-safe query builder for constructing searches.
// The builder validates field names against the schema of T.
func (s *Search[T]) Query() *lucene.Builder[T] {
	return s.builder
}

// Execute performs a search using the provided search request.
func (s *Search[T]) Execute(ctx context.Context, search *lucene.Search) (*SearchResult[T], error) {
	if err := search.Err(); err != nil {
		return nil, err
	}
	result, err := s.provider.Search(ctx, s.index, search)
	if err != nil {
		return nil, err
	}
	hits := make([]*Document[T], len(result.Hits))
	for i, hit := range result.Hits {
		var content T
		if err := s.codec.Decode(hit.Source, &content); err != nil {
			return nil, err
		}
		if err := callAfterLoad(ctx, &content); err != nil {
			return nil, err
		}
		hits[i] = &Document[T]{
			ID:      hit.ID,
			Content: content,
			Score:   hit.Score,
		}
	}
	return &SearchResult[T]{
		Hits:         hits,
		Total:        result.Total,
		MaxScore:     result.MaxScore,
		Aggregations: result.Aggregations,
		TypedAggs:    result.TypedAggs,
	}, nil
}

// Count returns the number of documents matching the query.
func (s *Search[T]) Count(ctx context.Context, query lucene.Query) (int64, error) {
	if query != nil {
		if err := query.Err(); err != nil {
			return 0, err
		}
	}
	return s.provider.Count(ctx, s.index, query)
}

// Refresh makes recent operations visible for search.
// Use sparingly as it can impact performance.
func (s *Search[T]) Refresh(ctx context.Context) error {
	return s.provider.Refresh(ctx, s.index)
}

// Atomic returns an atom-based view of this search index.
// The instance is created once and cached for subsequent calls.
// Panics if T is not atomizable (a programmer error).
func (s *Search[T]) Atomic() *atomix.Search[T] {
	s.atomicOnce.Do(func() {
		atomizer, err := atom.Use[T]()
		if err != nil {
			panic("grub: invalid type for atomization: " + err.Error())
		}
		s.atomic = atomix.NewSearch[T](s.provider, s.index, s.codec, atomizer.Spec())
	})
	return s.atomic
}
