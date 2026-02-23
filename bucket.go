package grub

import (
	"context"
	"sync"

	"github.com/zoobzio/atom"
	"github.com/zoobzio/grub/internal/atomix"
)

// Bucket provides type-safe blob storage operations for T.
// Wraps a BucketProvider, handling serialization of Object[T] to/from bytes.
type Bucket[T any] struct {
	provider   BucketProvider
	codec      Codec
	atomic     *atomix.Bucket[T]
	atomicOnce sync.Once
}

// NewBucket creates a Bucket for type T backed by the given provider.
// Uses JSON codec by default.
func NewBucket[T any](provider BucketProvider) *Bucket[T] {
	return &Bucket[T]{
		provider: provider,
		codec:    JSONCodec{},
	}
}

// NewBucketWithCodec creates a Bucket for type T with a custom codec.
func NewBucketWithCodec[T any](provider BucketProvider, codec Codec) *Bucket[T] {
	return &Bucket[T]{
		provider: provider,
		codec:    codec,
	}
}

// Get retrieves the object at key.
func (b *Bucket[T]) Get(ctx context.Context, key string) (*Object[T], error) {
	data, info, err := b.provider.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	var payload T
	if err := b.codec.Decode(data, &payload); err != nil {
		return nil, err
	}
	if err := callAfterLoad(ctx, &payload); err != nil {
		return nil, err
	}
	return &Object[T]{
		Key:         info.Key,
		ContentType: info.ContentType,
		Size:        info.Size,
		ETag:        info.ETag,
		Metadata:    info.Metadata,
		Data:        payload,
	}, nil
}

// Put stores an object at key.
func (b *Bucket[T]) Put(ctx context.Context, obj *Object[T]) error {
	if err := callBeforeSave(ctx, &obj.Data); err != nil {
		return err
	}
	data, err := b.codec.Encode(obj.Data)
	if err != nil {
		return err
	}
	info := &ObjectInfo{
		Key:         obj.Key,
		ContentType: obj.ContentType,
		Size:        int64(len(data)),
		Metadata:    obj.Metadata,
	}
	if err := b.provider.Put(ctx, obj.Key, data, info); err != nil {
		return err
	}
	return callAfterSave(ctx, &obj.Data)
}

// Delete removes the object at key.
func (b *Bucket[T]) Delete(ctx context.Context, key string) error {
	if err := callBeforeDelete[T](ctx); err != nil {
		return err
	}
	if err := b.provider.Delete(ctx, key); err != nil {
		return err
	}
	return callAfterDelete[T](ctx)
}

// Exists checks whether a key exists.
func (b *Bucket[T]) Exists(ctx context.Context, key string) (bool, error) {
	return b.provider.Exists(ctx, key)
}

// List returns object info for keys matching the given prefix.
// Limit of 0 means no limit.
func (b *Bucket[T]) List(ctx context.Context, prefix string, limit int) ([]ObjectInfo, error) {
	return b.provider.List(ctx, prefix, limit)
}

// Atomic returns an atom-based view of this bucket.
// The returned atomix.Bucket satisfies the AtomicBucket interface.
// The instance is created once and cached for subsequent calls.
// Panics if T is not atomizable (a programmer error).
func (b *Bucket[T]) Atomic() *atomix.Bucket[T] {
	b.atomicOnce.Do(func() {
		atomizer, err := atom.Use[T]()
		if err != nil {
			panic("grub: invalid type for atomization: " + err.Error())
		}
		b.atomic = atomix.NewBucket[T](b.provider, b.codec, atomizer.Spec())
	})
	return b.atomic
}
