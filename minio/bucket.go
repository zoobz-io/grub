// Package minio provides a grub BucketProvider implementation for MinIO.
package minio

import (
	"bytes"
	"context"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/zoobz-io/grub"
)

// Provider implements grub.BucketProvider for MinIO.
type Provider struct {
	client *minio.Client
	bucket string
}

// New creates a MinIO provider with the given client and bucket name.
func New(client *minio.Client, bucket string) *Provider {
	return &Provider{
		client: client,
		bucket: bucket,
	}
}

// Get retrieves the blob at key.
func (p *Provider) Get(ctx context.Context, key string) ([]byte, *grub.ObjectInfo, error) {
	obj, err := p.client.GetObject(ctx, p.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = obj.Close() }()

	stat, err := obj.Stat()
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, nil, grub.ErrNotFound
		}
		return nil, nil, err
	}

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, nil, err
	}

	info := &grub.ObjectInfo{
		Key:         key,
		Size:        stat.Size,
		ContentType: stat.ContentType,
		ETag:        stat.ETag,
		Metadata:    stat.UserMetadata,
	}

	return data, info, nil
}

// Put stores data at key with associated metadata.
func (p *Provider) Put(ctx context.Context, key string, data []byte, info *grub.ObjectInfo) error {
	opts := minio.PutObjectOptions{}
	if info != nil {
		if info.ContentType != "" {
			opts.ContentType = info.ContentType
		}
		if len(info.Metadata) > 0 {
			opts.UserMetadata = info.Metadata
		}
	}
	_, err := p.client.PutObject(ctx, p.bucket, key, bytes.NewReader(data), int64(len(data)), opts)
	return err
}

// Delete removes the blob at key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	exists, err := p.Exists(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return grub.ErrNotFound
	}

	return p.client.RemoveObject(ctx, p.bucket, key, minio.RemoveObjectOptions{})
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	_, err := p.client.StatObject(ctx, p.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List returns object info for keys matching the given prefix.
func (p *Provider) List(ctx context.Context, prefix string, limit int) ([]grub.ObjectInfo, error) {
	var results []grub.ObjectInfo

	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}

	for obj := range p.client.ListObjects(ctx, p.bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		results = append(results, grub.ObjectInfo{
			Key:  obj.Key,
			Size: obj.Size,
			ETag: obj.ETag,
		})
		if limit > 0 && len(results) >= limit {
			break
		}
	}

	return results, nil
}
