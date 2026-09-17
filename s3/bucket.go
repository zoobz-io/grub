// Package s3 provides a grub BucketProvider implementation for AWS S3.
package s3

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/zoobz-io/grub"
)

// maxPageSize is the largest page S3 returns per ListObjectsV2 request.
const maxPageSize = 1000

// Provider implements grub.BucketProvider for AWS S3.
type Provider struct {
	client *s3.Client
	bucket string
}

// New creates an S3 provider with the given client and bucket name.
func New(client *s3.Client, bucket string) *Provider {
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
	output, err := p.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, nil, grub.ErrNotFound
		}
		return nil, nil, err
	}

	info := &grub.ObjectInfo{
		Key:          key,
		Size:         aws.ToInt64(output.ContentLength),
		ContentType:  aws.ToString(output.ContentType),
		ETag:         aws.ToString(output.ETag),
		Metadata:     output.Metadata,
		LastModified: aws.ToTime(output.LastModified),
	}

	return output.Body, info, nil
}

// Stat returns the metadata of the object at key without transferring the data.
func (p *Provider) Stat(ctx context.Context, key string) (*grub.ObjectInfo, error) {
	output, err := p.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return nil, grub.ErrNotFound
		}
		return nil, err
	}
	return &grub.ObjectInfo{
		Key:          key,
		Size:         aws.ToInt64(output.ContentLength),
		ContentType:  aws.ToString(output.ContentType),
		ETag:         aws.ToString(output.ETag),
		Metadata:     output.Metadata,
		LastModified: aws.ToTime(output.LastModified),
	}, nil
}

// Put stores data at key with associated metadata.
func (p *Provider) Put(ctx context.Context, key string, data []byte, info *grub.ObjectInfo) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	applyPutMetadata(input, info)
	_, err := p.client.PutObject(ctx, input)
	return err
}

// PutStream stores data from r at key using the transfer manager, which handles
// unknown-length readers via streaming multipart upload.
func (p *Provider) PutStream(ctx context.Context, key string, r io.Reader, info *grub.ObjectInfo) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
		Body:   r,
	}
	applyPutMetadata(input, info)
	_, err := manager.NewUploader(p.client).Upload(ctx, input)
	return err
}

// applyPutMetadata copies content type and custom metadata onto a put input.
func applyPutMetadata(input *s3.PutObjectInput, info *grub.ObjectInfo) {
	if info == nil {
		return
	}
	if info.ContentType != "" {
		input.ContentType = aws.String(info.ContentType)
	}
	if len(info.Metadata) > 0 {
		input.Metadata = info.Metadata
	}
}

// Delete removes the blob at key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	// S3 DeleteObject doesn't return an error if the key doesn't exist.
	// Check existence first to maintain semantic consistency.
	exists, err := p.Exists(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return grub.ErrNotFound
	}

	_, err = p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	})
	return err
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
// The cursor is an S3 continuation token.
func (p *Provider) ListPage(ctx context.Context, prefix, cursor string, limit int) ([]grub.ObjectInfo, string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(p.bucket),
		Prefix: aws.String(prefix),
	}
	if cursor != "" {
		input.ContinuationToken = aws.String(cursor)
	}
	if limit > 0 {
		input.MaxKeys = aws.Int32(int32(min(limit, maxPageSize))) //nolint:gosec // bounded by min
	}

	output, err := p.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, "", err
	}

	results := make([]grub.ObjectInfo, 0, len(output.Contents))
	for _, obj := range output.Contents {
		results = append(results, contentToInfo(obj))
	}

	next := ""
	if aws.ToBool(output.IsTruncated) {
		next = aws.ToString(output.NextContinuationToken)
	}
	return results, next, nil
}

// ListLevel returns the objects and common prefixes directly under prefix,
// grouping on delimiter.
func (p *Provider) ListLevel(ctx context.Context, prefix, delimiter, cursor string, limit int) (*grub.Level, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(p.bucket),
		Prefix: aws.String(prefix),
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}
	if cursor != "" {
		input.ContinuationToken = aws.String(cursor)
	}
	if limit > 0 {
		input.MaxKeys = aws.Int32(int32(min(limit, maxPageSize))) //nolint:gosec // bounded by min
	}

	output, err := p.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, err
	}

	level := &grub.Level{}
	for _, obj := range output.Contents {
		level.Objects = append(level.Objects, contentToInfo(obj))
	}
	for _, cp := range output.CommonPrefixes {
		level.Prefixes = append(level.Prefixes, aws.ToString(cp.Prefix))
	}
	if aws.ToBool(output.IsTruncated) {
		level.Next = aws.ToString(output.NextContinuationToken)
	}
	return level, nil
}

// contentToInfo converts an S3 listing entry into a grub.ObjectInfo.
func contentToInfo(obj types.Object) grub.ObjectInfo {
	return grub.ObjectInfo{
		Key:          aws.ToString(obj.Key),
		Size:         aws.ToInt64(obj.Size),
		ETag:         aws.ToString(obj.ETag),
		LastModified: aws.ToTime(obj.LastModified),
	}
}
