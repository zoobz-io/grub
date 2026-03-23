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
	resp, err := p.client.DownloadStream(ctx, p.containerName, key, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, nil, grub.ErrNotFound
		}
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	info := &grub.ObjectInfo{
		Key: key,
	}
	if resp.ContentType != nil {
		info.ContentType = *resp.ContentType
	}
	if resp.ContentLength != nil {
		info.Size = *resp.ContentLength
	}
	if resp.ETag != nil {
		info.ETag = string(*resp.ETag)
	}

	// DownloadStream may not include metadata; fetch via blob properties
	blobClient := p.client.ServiceClient().NewContainerClient(p.containerName).NewBlobClient(key)
	props, err := blobClient.GetProperties(ctx, nil)
	if err == nil && props.Metadata != nil && len(props.Metadata) > 0 {
		info.Metadata = ptrMapToMap(props.Metadata)
	}

	return data, info, nil
}

// Put stores data at key with associated metadata.
func (p *Provider) Put(ctx context.Context, key string, data []byte, info *grub.ObjectInfo) error {
	opts := &azblob.UploadBufferOptions{}
	if info != nil {
		if info.ContentType != "" {
			opts.HTTPHeaders = &blob.HTTPHeaders{
				BlobContentType: &info.ContentType,
			}
		}
		if len(info.Metadata) > 0 {
			opts.Metadata = mapToPtrMap(info.Metadata)
		}
	}
	_, err := p.client.UploadBuffer(ctx, p.containerName, key, data, opts)
	return err
}

// Delete removes the blob at key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	_, err := p.client.DeleteBlob(ctx, p.containerName, key, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return grub.ErrNotFound
		}
		return err
	}
	return nil
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	blobClient := p.client.ServiceClient().NewContainerClient(p.containerName).NewBlobClient(key)
	_, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == 404 {
			return false, nil
		}
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List returns object info for keys matching the given prefix.
func (p *Provider) List(ctx context.Context, prefix string, limit int) ([]grub.ObjectInfo, error) {
	var results []grub.ObjectInfo

	pager := p.client.NewListBlobsFlatPager(p.containerName, &container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, b := range page.Segment.BlobItems {
			info := grub.ObjectInfo{
				Key: *b.Name,
			}
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
			}
			if b.Metadata != nil {
				info.Metadata = ptrMapToMap(b.Metadata)
			}
			results = append(results, info)

			if limit > 0 && len(results) >= limit {
				return results, nil
			}
		}
	}

	return results, nil
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
		// Create new variable for pointer
		result[k] = &v
	}
	return result
}
