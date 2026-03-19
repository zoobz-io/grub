package atomix

import (
	"context"

	"github.com/zoobzio/atom"
	"github.com/zoobzio/grub/internal/shared"
	"github.com/zoobzio/lucene"
)

// SearchProvider defines raw search storage operations.
// Duplicated here to avoid import cycle with parent package.
type SearchProvider interface {
	Index(ctx context.Context, index, id string, doc []byte) error
	IndexBatch(ctx context.Context, index string, docs map[string][]byte) error
	Get(ctx context.Context, index, id string) ([]byte, error)
	Delete(ctx context.Context, index, id string) error
	DeleteBatch(ctx context.Context, index string, ids []string) error
	Exists(ctx context.Context, index, id string) (bool, error)
	Search(ctx context.Context, index string, search *lucene.Search) (*shared.SearchResponse, error)
	Count(ctx context.Context, index string, query lucene.Query) (int64, error)
	Refresh(ctx context.Context, index string) error
}

// Search provides atom-based search operations.
// Satisfies the grub.AtomicSearch interface.
type Search[T any] struct {
	provider  SearchProvider
	indexName string
	codec     Codec
	spec      atom.Spec
}

// NewSearch creates an atomic Search wrapper.
func NewSearch[T any](provider SearchProvider, index string, codec Codec, spec atom.Spec) *Search[T] {
	return &Search[T]{
		provider:  provider,
		indexName: index,
		codec:     codec,
		spec:      spec,
	}
}

// Index returns the index name.
func (s *Search[T]) Index() string {
	return s.indexName
}

// Spec returns the atom spec for this search's document type.
func (s *Search[T]) Spec() atom.Spec {
	return s.spec
}

// Get retrieves the document at ID with atomized content.
func (s *Search[T]) Get(ctx context.Context, id string) (*shared.AtomicDocument, error) {
	data, err := s.provider.Get(ctx, s.indexName, id)
	if err != nil {
		return nil, err
	}
	content, err := s.contentToAtom(data)
	if err != nil {
		return nil, err
	}
	return &shared.AtomicDocument{
		ID:      id,
		Content: content,
	}, nil
}

// IndexDoc stores a document with atomized content.
func (s *Search[T]) IndexDoc(ctx context.Context, id string, doc *atom.Atom) error {
	data, err := s.atomToContent(doc)
	if err != nil {
		return err
	}
	return s.provider.Index(ctx, s.indexName, id, data)
}

// Delete removes the document at ID.
func (s *Search[T]) Delete(ctx context.Context, id string) error {
	return s.provider.Delete(ctx, s.indexName, id)
}

// Exists checks whether a document ID exists.
func (s *Search[T]) Exists(ctx context.Context, id string) (bool, error) {
	return s.provider.Exists(ctx, s.indexName, id)
}

// Search performs a search returning atomized results.
func (s *Search[T]) Search(ctx context.Context, search *lucene.Search) ([]shared.AtomicDocument, error) {
	result, err := s.provider.Search(ctx, s.indexName, search)
	if err != nil {
		return nil, err
	}
	docs := make([]shared.AtomicDocument, len(result.Hits))
	for i, hit := range result.Hits {
		content, err := s.contentToAtom(hit.Source)
		if err != nil {
			return nil, err
		}
		docs[i] = shared.AtomicDocument{
			ID:      hit.ID,
			Content: content,
			Score:   hit.Score,
		}
	}
	return docs, nil
}

// contentToAtom converts bytes content to an Atom via T.
func (s *Search[T]) contentToAtom(data []byte) (*atom.Atom, error) {
	if data == nil {
		return nil, nil
	}
	var value T
	if err := s.codec.Decode(data, &value); err != nil {
		return nil, err
	}
	atomizer, err := atom.Use[T]()
	if err != nil {
		return nil, err
	}
	return atomizer.Atomize(&value), nil
}

// atomToContent converts an Atom to bytes content via T.
func (s *Search[T]) atomToContent(a *atom.Atom) ([]byte, error) {
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
	return s.codec.Encode(value)
}
