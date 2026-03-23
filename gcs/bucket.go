// Package gcs provides a grub BucketProvider implementation for Google Cloud Storage.
package gcs

import (
	"context"
	"errors"
	"io"

	"cloud.google.com/go/storage"
	"github.com/zoobz-io/grub"
	"google.golang.org/api/iterator"
)

// Provider implements grub.BucketProvider for Google Cloud Storage.
type Provider struct {
	client *storage.Client
	bucket string
}

// New creates a GCS provider with the given client and bucket name.
func New(client *storage.Client, bucket string) *Provider {
	return &Provider{
		client: client,
		bucket: bucket,
	}
}

// Get retrieves the blob at key.
func (p *Provider) Get(ctx context.Context, key string) ([]byte, *grub.ObjectInfo, error) {
	obj := p.client.Bucket(p.bucket).Object(key)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, nil, grub.ErrNotFound
		}
		return nil, nil, err
	}

	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, err
	}

	info := &grub.ObjectInfo{
		Key:         key,
		ContentType: attrs.ContentType,
		Size:        attrs.Size,
		ETag:        attrs.Etag,
		Metadata:    attrs.Metadata,
	}

	return data, info, nil
}

// Put stores data at key with associated metadata.
func (p *Provider) Put(ctx context.Context, key string, data []byte, info *grub.ObjectInfo) error {
	obj := p.client.Bucket(p.bucket).Object(key)
	writer := obj.NewWriter(ctx)

	if info != nil {
		if info.ContentType != "" {
			writer.ContentType = info.ContentType
		}
		if len(info.Metadata) > 0 {
			writer.Metadata = info.Metadata
		}
	}

	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return err
	}

	return writer.Close()
}

// Delete removes the blob at key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	obj := p.client.Bucket(p.bucket).Object(key)
	err := obj.Delete(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return grub.ErrNotFound
		}
		return err
	}
	return nil
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	obj := p.client.Bucket(p.bucket).Object(key)
	_, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List returns object info for keys matching the given prefix.
func (p *Provider) List(ctx context.Context, prefix string, limit int) ([]grub.ObjectInfo, error) {
	var results []grub.ObjectInfo

	query := &storage.Query{Prefix: prefix}
	it := p.client.Bucket(p.bucket).Objects(ctx, query)

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		results = append(results, grub.ObjectInfo{
			Key:         attrs.Name,
			ContentType: attrs.ContentType,
			Size:        attrs.Size,
			ETag:        attrs.Etag,
			Metadata:    attrs.Metadata,
		})

		if limit > 0 && len(results) >= limit {
			break
		}
	}

	return results, nil
}
