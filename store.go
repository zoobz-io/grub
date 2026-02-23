package grub

import (
	"context"
	"sync"
	"time"

	"github.com/zoobzio/atom"
	"github.com/zoobzio/grub/internal/atomix"
)

// Store provides type-safe key-value storage operations for T.
// Wraps a StoreProvider, handling serialization of T to/from bytes.
type Store[T any] struct {
	provider   StoreProvider
	codec      Codec
	atomic     *atomix.Store[T]
	atomicOnce sync.Once
}

// NewStore creates a Store for type T backed by the given provider.
// Uses JSON codec by default.
func NewStore[T any](provider StoreProvider) *Store[T] {
	return &Store[T]{
		provider: provider,
		codec:    JSONCodec{},
	}
}

// NewStoreWithCodec creates a Store for type T with a custom codec.
func NewStoreWithCodec[T any](provider StoreProvider, codec Codec) *Store[T] {
	return &Store[T]{
		provider: provider,
		codec:    codec,
	}
}

// Get retrieves the value at key as T.
func (s *Store[T]) Get(ctx context.Context, key string) (*T, error) {
	data, err := s.provider.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	var value T
	if err := s.codec.Decode(data, &value); err != nil {
		return nil, err
	}
	if err := callAfterLoad(ctx, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

// Set stores value at key with optional TTL.
// TTL of 0 means no expiration.
func (s *Store[T]) Set(ctx context.Context, key string, value *T, ttl time.Duration) error {
	if err := callBeforeSave(ctx, value); err != nil {
		return err
	}
	data, err := s.codec.Encode(value)
	if err != nil {
		return err
	}
	if err := s.provider.Set(ctx, key, data, ttl); err != nil {
		return err
	}
	return callAfterSave(ctx, value)
}

// Delete removes the value at key.
func (s *Store[T]) Delete(ctx context.Context, key string) error {
	if err := callBeforeDelete[T](ctx); err != nil {
		return err
	}
	if err := s.provider.Delete(ctx, key); err != nil {
		return err
	}
	return callAfterDelete[T](ctx)
}

// Exists checks whether a key exists.
func (s *Store[T]) Exists(ctx context.Context, key string) (bool, error) {
	return s.provider.Exists(ctx, key)
}

// List returns keys matching the given prefix.
// Limit of 0 means no limit.
func (s *Store[T]) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	return s.provider.List(ctx, prefix, limit)
}

// GetBatch retrieves multiple values by key.
// Missing keys are omitted from the result.
func (s *Store[T]) GetBatch(ctx context.Context, keys []string) (map[string]*T, error) {
	raw, err := s.provider.GetBatch(ctx, keys)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*T, len(raw))
	for k, data := range raw {
		var value T
		if err := s.codec.Decode(data, &value); err != nil {
			return nil, err
		}
		if err := callAfterLoad(ctx, &value); err != nil {
			return nil, err
		}
		result[k] = &value
	}
	return result, nil
}

// SetBatch stores multiple key-value pairs with optional TTL.
// TTL of 0 means no expiration.
func (s *Store[T]) SetBatch(ctx context.Context, items map[string]*T, ttl time.Duration) error {
	raw := make(map[string][]byte, len(items))
	for k, v := range items {
		if err := callBeforeSave(ctx, v); err != nil {
			return err
		}
		data, err := s.codec.Encode(v)
		if err != nil {
			return err
		}
		raw[k] = data
	}
	if err := s.provider.SetBatch(ctx, raw, ttl); err != nil {
		return err
	}
	for _, v := range items {
		if err := callAfterSave(ctx, v); err != nil {
			return err
		}
	}
	return nil
}

// Atomic returns an atom-based view of this store.
// The returned atomix.Store satisfies the AtomicStore interface.
// The instance is created once and cached for subsequent calls.
// Panics if T is not atomizable (a programmer error).
func (s *Store[T]) Atomic() *atomix.Store[T] {
	s.atomicOnce.Do(func() {
		atomizer, err := atom.Use[T]()
		if err != nil {
			panic("grub: invalid type for atomization: " + err.Error())
		}
		s.atomic = atomix.NewStore[T](s.provider, s.codec, atomizer.Spec())
	})
	return s.atomic
}
