// Package s3 provides a grub BucketProvider implementation for AWS S3.
package s3

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/zoobz-io/grub"
)

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
	defer func() { _ = output.Body.Close() }()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, nil, err
	}

	info := &grub.ObjectInfo{
		Key:         key,
		Size:        aws.ToInt64(output.ContentLength),
		ContentType: aws.ToString(output.ContentType),
		ETag:        aws.ToString(output.ETag),
		Metadata:    output.Metadata,
	}

	return data, info, nil
}

// Put stores data at key with associated metadata.
func (p *Provider) Put(ctx context.Context, key string, data []byte, info *grub.ObjectInfo) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	if info != nil {
		if info.ContentType != "" {
			input.ContentType = aws.String(info.ContentType)
		}
		if len(info.Metadata) > 0 {
			input.Metadata = info.Metadata
		}
	}
	_, err := p.client.PutObject(ctx, input)
	return err
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
	_, err := p.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NotFound
		if errors.As(err, &nsk) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List returns object info for keys matching the given prefix.
func (p *Provider) List(ctx context.Context, prefix string, limit int) ([]grub.ObjectInfo, error) {
	var results []grub.ObjectInfo
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(p.bucket),
			Prefix: aws.String(prefix),
		}
		if continuationToken != nil {
			input.ContinuationToken = continuationToken
		}
		if limit > 0 && limit-len(results) < 1000 {
			remaining := limit - len(results)
			input.MaxKeys = aws.Int32(int32(min(remaining, 1000))) //nolint:gosec // bounded by < 1000 check above
		}

		output, err := p.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, err
		}

		for _, obj := range output.Contents {
			results = append(results, grub.ObjectInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
				ETag: aws.ToString(obj.ETag),
			})
			if limit > 0 && len(results) >= limit {
				return results, nil
			}
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return results, nil
}
