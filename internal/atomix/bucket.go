package atomix

import (
	"context"

	"github.com/zoobz-io/atom"
	"github.com/zoobz-io/grub/internal/shared"
)

// BucketProvider defines raw blob storage operations.
// Duplicated here to avoid import cycle with parent package.
type BucketProvider interface {
	Get(ctx context.Context, key string) ([]byte, *shared.ObjectInfo, error)
	Put(ctx context.Context, key string, data []byte, info *shared.ObjectInfo) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
}

// Bucket provides atom-based blob storage operations.
// Satisfies the grub.AtomicBucket interface.
// Only the payload T is atomized; metadata remains as-is.
type Bucket[T any] struct {
	provider BucketProvider
	codec    Codec
	spec     atom.Spec
}

// NewBucket creates an atomic Bucket wrapper.
func NewBucket[T any](provider BucketProvider, codec Codec, spec atom.Spec) *Bucket[T] {
	return &Bucket[T]{
		provider: provider,
		codec:    codec,
		spec:     spec,
	}
}

// Spec returns the atom spec for this bucket's payload type T.
func (b *Bucket[T]) Spec() atom.Spec {
	return b.spec
}

// Get retrieves the blob at key with atomized payload.
func (b *Bucket[T]) Get(ctx context.Context, key string) (*shared.AtomicObject, error) {
	data, info, err := b.provider.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	var payload T
	if decErr := b.codec.Decode(data, &payload); decErr != nil {
		return nil, decErr
	}
	atomizer, err := atom.Use[T]()
	if err != nil {
		return nil, err
	}
	return &shared.AtomicObject{
		Key:         info.Key,
		ContentType: info.ContentType,
		Size:        info.Size,
		ETag:        info.ETag,
		Metadata:    info.Metadata,
		Data:        atomizer.Atomize(&payload),
	}, nil
}

// Put stores an object with atomized payload at key.
func (b *Bucket[T]) Put(ctx context.Context, key string, obj *shared.AtomicObject) error {
	atomizer, err := atom.Use[T]()
	if err != nil {
		return err
	}
	payload, err := atomizer.Deatomize(obj.Data)
	if err != nil {
		return err
	}
	data, err := b.codec.Encode(payload)
	if err != nil {
		return err
	}
	info := &shared.ObjectInfo{
		Key:         obj.Key,
		ContentType: obj.ContentType,
		Size:        int64(len(data)),
		Metadata:    obj.Metadata,
	}
	return b.provider.Put(ctx, key, data, info)
}

// Delete removes the blob at key.
func (b *Bucket[T]) Delete(ctx context.Context, key string) error {
	return b.provider.Delete(ctx, key)
}

// Exists checks whether a key exists.
func (b *Bucket[T]) Exists(ctx context.Context, key string) (bool, error) {
	return b.provider.Exists(ctx, key)
}
