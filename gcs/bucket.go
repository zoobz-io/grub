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

// defaultPageSize is the page size used when a caller passes limit 0.
const defaultPageSize = 1000

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
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, nil, grub.ErrNotFound
		}
		return nil, nil, err
	}

	return reader, attrsToInfo(key, attrs), nil
}

// Stat returns the metadata of the object at key without transferring the data.
func (p *Provider) Stat(ctx context.Context, key string) (*grub.ObjectInfo, error) {
	attrs, err := p.client.Bucket(p.bucket).Object(key).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, grub.ErrNotFound
		}
		return nil, err
	}
	return attrsToInfo(key, attrs), nil
}

// attrsToInfo converts GCS object attributes into a grub.ObjectInfo.
func attrsToInfo(key string, attrs *storage.ObjectAttrs) *grub.ObjectInfo {
	return &grub.ObjectInfo{
		Key:          key,
		ContentType:  attrs.ContentType,
		Size:         attrs.Size,
		ETag:         attrs.Etag,
		Metadata:     attrs.Metadata,
		LastModified: attrs.Updated,
	}
}

// Put stores data at key with associated metadata.
func (p *Provider) Put(ctx context.Context, key string, data []byte, info *grub.ObjectInfo) error {
	writer := p.newWriter(ctx, key, info)
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

// PutStream stores data from r at key. GCS writers stream to the object without
// requiring a known length.
func (p *Provider) PutStream(ctx context.Context, key string, r io.Reader, info *grub.ObjectInfo) error {
	writer := p.newWriter(ctx, key, info)
	if _, err := io.Copy(writer, r); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

// newWriter creates an object writer with content type and metadata applied.
func (p *Provider) newWriter(ctx context.Context, key string, info *grub.ObjectInfo) *storage.Writer {
	writer := p.client.Bucket(p.bucket).Object(key).NewWriter(ctx)
	if info != nil {
		if info.ContentType != "" {
			writer.ContentType = info.ContentType
		}
		if len(info.Metadata) > 0 {
			writer.Metadata = info.Metadata
		}
	}
	return writer
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
// The cursor is a GCS iterator page token.
func (p *Provider) ListPage(ctx context.Context, prefix, cursor string, limit int) ([]grub.ObjectInfo, string, error) {
	it := p.client.Bucket(p.bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	pager := iterator.NewPager(it, pageSize(limit), cursor)

	var attrsList []*storage.ObjectAttrs
	next, err := pager.NextPage(&attrsList)
	if err != nil {
		return nil, "", err
	}

	results := make([]grub.ObjectInfo, 0, len(attrsList))
	for _, attrs := range attrsList {
		results = append(results, *attrsToInfo(attrs.Name, attrs))
	}
	return results, next, nil
}

// ListLevel returns the objects and common prefixes directly under prefix,
// grouping on delimiter. An entry with ObjectAttrs.Prefix set is a common prefix.
func (p *Provider) ListLevel(ctx context.Context, prefix, delimiter, cursor string, limit int) (*grub.Level, error) {
	it := p.client.Bucket(p.bucket).Objects(ctx, &storage.Query{Prefix: prefix, Delimiter: delimiter})
	pager := iterator.NewPager(it, pageSize(limit), cursor)

	var attrsList []*storage.ObjectAttrs
	next, err := pager.NextPage(&attrsList)
	if err != nil {
		return nil, err
	}

	level := &grub.Level{Next: next}
	for _, attrs := range attrsList {
		if attrs.Prefix != "" {
			level.Prefixes = append(level.Prefixes, attrs.Prefix)
			continue
		}
		level.Objects = append(level.Objects, *attrsToInfo(attrs.Name, attrs))
	}
	return level, nil
}

// pageSize resolves a caller limit to a concrete page size.
func pageSize(limit int) int {
	if limit <= 0 {
		return defaultPageSize
	}
	return limit
}
