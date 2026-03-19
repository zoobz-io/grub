package atomix

import (
	"context"
	"time"

	"github.com/zoobzio/atom"
)

// StoreProvider defines raw key-value storage operations.
// Duplicated here to avoid import cycle with parent package.
type StoreProvider interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
}

// Codec defines encoding/decoding operations.
// Duplicated here to avoid import cycle with parent package.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

// Store provides atom-based key-value storage operations.
// Satisfies the grub.AtomicStore interface.
type Store[T any] struct {
	provider StoreProvider
	codec    Codec
	spec     atom.Spec
}

// NewStore creates an atomic Store wrapper.
func NewStore[T any](provider StoreProvider, codec Codec, spec atom.Spec) *Store[T] {
	return &Store[T]{
		provider: provider,
		codec:    codec,
		spec:     spec,
	}
}

// Spec returns the atom spec for this store's type.
func (s *Store[T]) Spec() atom.Spec {
	return s.spec
}

// Get retrieves the value at key as an Atom.
func (s *Store[T]) Get(ctx context.Context, key string) (*atom.Atom, error) {
	data, err := s.provider.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	var value T
	if decErr := s.codec.Decode(data, &value); decErr != nil {
		return nil, decErr
	}
	atomizer, err := atom.Use[T]()
	if err != nil {
		return nil, err
	}
	return atomizer.Atomize(&value), nil
}

// Set stores an Atom at key with optional TTL.
func (s *Store[T]) Set(ctx context.Context, key string, a *atom.Atom, ttl time.Duration) error {
	atomizer, err := atom.Use[T]()
	if err != nil {
		return err
	}
	value, err := atomizer.Deatomize(a)
	if err != nil {
		return err
	}
	data, err := s.codec.Encode(value)
	if err != nil {
		return err
	}
	return s.provider.Set(ctx, key, data, ttl)
}

// Delete removes the value at key.
func (s *Store[T]) Delete(ctx context.Context, key string) error {
	return s.provider.Delete(ctx, key)
}

// Exists checks whether a key exists.
func (s *Store[T]) Exists(ctx context.Context, key string) (bool, error) {
	return s.provider.Exists(ctx, key)
}
