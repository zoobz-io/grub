// Package minio provides a grub BucketProvider implementation for MinIO.
package minio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/zoobz-io/grub"
)

// defaultPageSize is the page size used when a caller passes limit 0.
const defaultPageSize = 1000

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
	r, info, err := p.GetStream(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = r.Close() }()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}
	return data, info, nil
}

// GetStream returns a reader over the blob at key. The caller must close it.
func (p *Provider) GetStream(ctx context.Context, key string) (io.ReadCloser, *grub.ObjectInfo, error) {
	obj, err := p.client.GetObject(ctx, p.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, nil, err
	}

	stat, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, nil, grub.ErrNotFound
		}
		return nil, nil, err
	}

	return obj, statToInfo(key, stat), nil
}

// Stat returns the metadata of the object at key without transferring the data.
func (p *Provider) Stat(ctx context.Context, key string) (*grub.ObjectInfo, error) {
	stat, err := p.client.StatObject(ctx, p.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, grub.ErrNotFound
		}
		return nil, err
	}
	return statToInfo(key, stat), nil
}

// statToInfo converts a minio ObjectInfo into a grub.ObjectInfo.
func statToInfo(key string, stat minio.ObjectInfo) *grub.ObjectInfo {
	return &grub.ObjectInfo{
		Key:          key,
		Size:         stat.Size,
		ContentType:  stat.ContentType,
		ETag:         stat.ETag,
		Metadata:     stat.UserMetadata,
		LastModified: stat.LastModified,
	}
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

// PutStream stores data from r at key. Unknown-length streams (info.Size <= 0)
// use minio's streaming multipart upload (size -1).
func (p *Provider) PutStream(ctx context.Context, key string, r io.Reader, info *grub.ObjectInfo) error {
	opts := minio.PutObjectOptions{}
	size := int64(-1)
	if info != nil {
		if info.ContentType != "" {
			opts.ContentType = info.ContentType
		}
		if len(info.Metadata) > 0 {
			opts.UserMetadata = info.Metadata
		}
		if info.Size > 0 {
			size = info.Size
		}
	}
	_, err := p.client.PutObject(ctx, p.bucket, key, r, size, opts)
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
	_, err := p.Stat(ctx, key)
	if err != nil {
		if errors.Is(err, grub.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List returns object info for keys matching the given prefix.
func (p *Provider) List(ctx context.Context, prefix string, limit int) ([]grub.ObjectInfo, error) {
	var results []grub.ObjectInfo
	cursor := ""
	for {
		pageLimit := 0
		if limit > 0 {
			pageLimit = limit - len(results)
		}
		infos, next, err := p.ListPage(ctx, prefix, cursor, pageLimit)
		if err != nil {
			return nil, err
		}
		results = append(results, infos...)
		if limit > 0 && len(results) >= limit {
			return results[:limit], nil
		}
		if next == "" {
			return results, nil
		}
		cursor = next
	}
}

// ListPage returns one page of object info for keys matching prefix.
// The cursor is the last key returned by the previous page (minio StartAfter).
func (p *Provider) ListPage(ctx context.Context, prefix, cursor string, limit int) ([]grub.ObjectInfo, string, error) {
	pageSize := limit
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	opts := minio.ListObjectsOptions{
		Prefix:     prefix,
		Recursive:  true,
		StartAfter: cursor,
	}

	var results []grub.ObjectInfo
	var next string
	for obj := range p.client.ListObjects(ctx, p.bucket, opts) {
		if obj.Err != nil {
			return nil, "", obj.Err
		}
		results = append(results, listEntryToInfo(obj))
		if len(results) >= pageSize {
			next = obj.Key
			break
		}
	}

	return results, next, nil
}

// ListLevel returns the objects and common prefixes directly under prefix.
// minio groups on the "/" delimiter when Recursive is false; a listing entry
// whose key ends with the delimiter is a common prefix.
func (p *Provider) ListLevel(ctx context.Context, prefix, delimiter, cursor string, limit int) (*grub.Level, error) {
	pageSize := limit
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	opts := minio.ListObjectsOptions{
		Prefix:     prefix,
		Recursive:  false,
		StartAfter: cursor,
	}

	level := &grub.Level{}
	count := 0
	for obj := range p.client.ListObjects(ctx, p.bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if delimiter != "" && strings.HasSuffix(obj.Key, delimiter) {
			level.Prefixes = append(level.Prefixes, obj.Key)
		} else {
			level.Objects = append(level.Objects, listEntryToInfo(obj))
		}
		count++
		if count >= pageSize {
			level.Next = obj.Key
			break
		}
	}

	return level, nil
}

// listEntryToInfo converts a minio listing entry into a grub.ObjectInfo.
func listEntryToInfo(obj minio.ObjectInfo) grub.ObjectInfo {
	return grub.ObjectInfo{
		Key:          obj.Key,
		Size:         obj.Size,
		ETag:         obj.ETag,
		LastModified: obj.LastModified,
	}
}
