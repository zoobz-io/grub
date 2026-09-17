// Package azure provides a grub BucketProvider implementation for Azure Blob Storage.
package azure

import (
	"context"
	"errors"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/zoobz-io/grub"
)

// Provider implements grub.BucketProvider for Azure Blob Storage.
type Provider struct {
	client        *azblob.Client
	containerName string
}

// New creates an Azure Blob provider with the given client and container name.
func New(client *azblob.Client, containerName string) *Provider {
	return &Provider{
		client:        client,
		containerName: containerName,
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
	resp, err := p.client.DownloadStream(ctx, p.containerName, key, nil)
	if err != nil {
		if isNotFound(err) {
			return nil, nil, grub.ErrNotFound
		}
		return nil, nil, err
	}

	info := &grub.ObjectInfo{Key: key}
	if resp.ContentType != nil {
		info.ContentType = *resp.ContentType
	}
	if resp.ContentLength != nil {
		info.Size = *resp.ContentLength
	}
	if resp.ETag != nil {
		info.ETag = string(*resp.ETag)
	}
	if resp.LastModified != nil {
		info.LastModified = *resp.LastModified
	}

	// DownloadStream may not include metadata; fetch via blob properties.
	props, err := p.blobClient(key).GetProperties(ctx, nil)
	if err == nil && len(props.Metadata) > 0 {
		info.Metadata = ptrMapToMap(props.Metadata)
	}

	return resp.Body, info, nil
}

// Stat returns the metadata of the blob at key without transferring the data.
func (p *Provider) Stat(ctx context.Context, key string) (*grub.ObjectInfo, error) {
	props, err := p.blobClient(key).GetProperties(ctx, nil)
	if err != nil {
		if isNotFound(err) {
			return nil, grub.ErrNotFound
		}
		return nil, err
	}

	info := &grub.ObjectInfo{Key: key}
	if props.ContentType != nil {
		info.ContentType = *props.ContentType
	}
	if props.ContentLength != nil {
		info.Size = *props.ContentLength
	}
	if props.ETag != nil {
		info.ETag = string(*props.ETag)
	}
	if props.LastModified != nil {
		info.LastModified = *props.LastModified
	}
	if len(props.Metadata) > 0 {
		info.Metadata = ptrMapToMap(props.Metadata)
	}
	return info, nil
}

// Put stores data at key with associated metadata.
func (p *Provider) Put(ctx context.Context, key string, data []byte, info *grub.ObjectInfo) error {
	opts := &azblob.UploadBufferOptions{}
	if info != nil {
		if info.ContentType != "" {
			opts.HTTPHeaders = &blob.HTTPHeaders{BlobContentType: &info.ContentType}
		}
		if len(info.Metadata) > 0 {
			opts.Metadata = mapToPtrMap(info.Metadata)
		}
	}
	_, err := p.client.UploadBuffer(ctx, p.containerName, key, data, opts)
	return err
}

// PutStream stores data from r at key using a streaming block upload, which does
// not require a known length.
func (p *Provider) PutStream(ctx context.Context, key string, r io.Reader, info *grub.ObjectInfo) error {
	opts := &azblob.UploadStreamOptions{}
	if info != nil {
		if info.ContentType != "" {
			opts.HTTPHeaders = &blob.HTTPHeaders{BlobContentType: &info.ContentType}
		}
		if len(info.Metadata) > 0 {
			opts.Metadata = mapToPtrMap(info.Metadata)
		}
	}
	_, err := p.client.UploadStream(ctx, p.containerName, key, r, opts)
	return err
}

// Delete removes the blob at key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	_, err := p.client.DeleteBlob(ctx, p.containerName, key, nil)
	if err != nil {
		if isNotFound(err) {
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
// The cursor is an Azure list marker.
func (p *Provider) ListPage(ctx context.Context, prefix, cursor string, limit int) ([]grub.ObjectInfo, string, error) {
	opts := &container.ListBlobsFlatOptions{Prefix: &prefix}
	if cursor != "" {
		opts.Marker = &cursor
	}
	if limit > 0 {
		mr := int32(limit) //nolint:gosec // page size, caller-bounded
		opts.MaxResults = &mr
	}

	pager := p.client.NewListBlobsFlatPager(p.containerName, opts)
	if !pager.More() {
		return nil, "", nil
	}
	page, err := pager.NextPage(ctx)
	if err != nil {
		return nil, "", err
	}

	var results []grub.ObjectInfo
	for _, b := range page.Segment.BlobItems {
		results = append(results, blobItemToInfo(b))
	}
	return results, marker(page.NextMarker), nil
}

// ListLevel returns the blobs and common prefixes directly under prefix,
// grouping on delimiter.
func (p *Provider) ListLevel(ctx context.Context, prefix, delimiter, cursor string, limit int) (*grub.Level, error) {
	opts := &container.ListBlobsHierarchyOptions{Prefix: &prefix}
	if cursor != "" {
		opts.Marker = &cursor
	}
	if limit > 0 {
		mr := int32(limit) //nolint:gosec // page size, caller-bounded
		opts.MaxResults = &mr
	}

	containerClient := p.client.ServiceClient().NewContainerClient(p.containerName)
	pager := containerClient.NewListBlobsHierarchyPager(delimiter, opts)

	level := &grub.Level{}
	if !pager.More() {
		return level, nil
	}
	page, err := pager.NextPage(ctx)
	if err != nil {
		return nil, err
	}

	for _, b := range page.Segment.BlobItems {
		level.Objects = append(level.Objects, blobItemToInfo(b))
	}
	for _, bp := range page.Segment.BlobPrefixes {
		if bp.Name != nil {
			level.Prefixes = append(level.Prefixes, *bp.Name)
		}
	}
	level.Next = marker(page.NextMarker)
	return level, nil
}

// blobClient returns a blob client for the given key.
func (p *Provider) blobClient(key string) *blob.Client {
	return p.client.ServiceClient().NewContainerClient(p.containerName).NewBlobClient(key)
}

// blobItemToInfo converts an Azure listing entry into a grub.ObjectInfo.
func blobItemToInfo(b *container.BlobItem) grub.ObjectInfo {
	info := grub.ObjectInfo{Key: *b.Name}
	if b.Properties != nil {
		if b.Properties.ContentType != nil {
			info.ContentType = *b.Properties.ContentType
		}
		if b.Properties.ContentLength != nil {
			info.Size = *b.Properties.ContentLength
		}
		if b.Properties.ETag != nil {
			info.ETag = string(*b.Properties.ETag)
		}
		if b.Properties.LastModified != nil {
			info.LastModified = *b.Properties.LastModified
		}
	}
	if b.Metadata != nil {
		info.Metadata = ptrMapToMap(b.Metadata)
	}
	return info
}

// marker dereferences an Azure next-marker pointer into a cursor string.
func marker(m *string) string {
	if m == nil {
		return ""
	}
	return *m
}

// isNotFound reports whether err represents a missing blob.
func isNotFound(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == 404 {
		return true
	}
	return bloberror.HasCode(err, bloberror.BlobNotFound)
}

// ptrMapToMap converts map[string]*string to map[string]string.
func ptrMapToMap(m map[string]*string) map[string]string {
	if m == nil {
		return nil
	}
	result := make(map[string]string, len(m))
	for k, v := range m {
		if v != nil {
			result[k] = *v
		}
	}
	return result
}

// mapToPtrMap converts map[string]string to map[string]*string.
func mapToPtrMap(m map[string]string) map[string]*string {
	if m == nil {
		return nil
	}
	result := make(map[string]*string, len(m))
	for k, v := range m {
		result[k] = &v
	}
	return result
}
