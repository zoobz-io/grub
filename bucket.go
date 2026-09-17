package grub

import (
	"context"
	"io"
	"sync"

	"github.com/zoobz-io/atom"
	"github.com/zoobz-io/grub/internal/atomix"
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
		Key:          info.Key,
		ContentType:  info.ContentType,
		Size:         info.Size,
		ETag:         info.ETag,
		Metadata:     info.Metadata,
		LastModified: info.LastModified,
		Data:         payload,
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

// Stat returns the metadata of the object at key, without transferring the data.
// Returns ErrNotFound if the key does not exist.
func (b *Bucket[T]) Stat(ctx context.Context, key string) (*ObjectInfo, error) {
	return b.provider.Stat(ctx, key)
}

// ListPage returns one page of object info for keys matching prefix.
// cursor is an opaque provider-defined token; pass "" for the first page.
// An empty next cursor means no more pages. Limit of 0 uses the provider default page size.
func (b *Bucket[T]) ListPage(ctx context.Context, prefix, cursor string, limit int) (infos []ObjectInfo, next string, err error) {
	return b.provider.ListPage(ctx, prefix, cursor, limit)
}

// ListLevel returns the objects and common prefixes directly under prefix,
// where delimiter separates the levels. cursor pages through large levels;
// pass "" for the first page. Limit of 0 uses the provider default page size.
func (b *Bucket[T]) ListLevel(ctx context.Context, prefix, delimiter, cursor string, limit int) (*Level, error) {
	return b.provider.ListLevel(ctx, prefix, delimiter, cursor, limit)
}

// GetStream returns a reader over the raw object bytes at key.
// This is raw access: it bypasses the codec and the lifecycle hooks.
// The caller must close the reader. Returns ErrNotFound if the key does not exist.
func (b *Bucket[T]) GetStream(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error) {
	return b.provider.GetStream(ctx, key)
}

// PutStream stores raw data from r at key.
// This is raw access: it bypasses the codec and the lifecycle hooks.
// info.Size may be set if known; providers handle unknown-size streams internally.
func (b *Bucket[T]) PutStream(ctx context.Context, key string, r io.Reader, info *ObjectInfo) error {
	return b.provider.PutStream(ctx, key, r, info)
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
