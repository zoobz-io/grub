// Package redis provides a grub StoreProvider implementation for Redis.
package redis

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zoobz-io/grub"
)

// Provider implements grub.StoreProvider for Redis.
type Provider struct {
	client *redis.Client
}

// New creates a Redis provider with the given client.
func New(client *redis.Client) *Provider {
	return &Provider{client: client}
}

// Get retrieves the value at key.
func (p *Provider) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := p.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, grub.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Set stores value at key with optional TTL.
func (p *Provider) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return p.client.Set(ctx, key, value, ttl).Err()
}

// Delete removes the value at key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	result, err := p.client.Del(ctx, key).Result()
	if err != nil {
		return err
	}
	if result == 0 {
		return grub.ErrNotFound
	}
	return nil
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	result, err := p.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return result > 0, nil
}

// List returns keys matching the given prefix.
func (p *Provider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	var keys []string
	var cursor uint64
	pattern := prefix + "*"

	for {
		var batch []string
		var err error
		batch, cursor, err = p.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		if limit > 0 && len(keys) >= limit {
			keys = keys[:limit]
			break
		}
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

// GetBatch retrieves multiple values by key.
func (p *Provider) GetBatch(ctx context.Context, keys []string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return make(map[string][]byte), nil
	}
	values, err := p.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	result := make(map[string][]byte, len(keys))
	for i, v := range values {
		if v != nil {
			if s, ok := v.(string); ok {
				result[keys[i]] = []byte(s)
			}
		}
	}
	return result, nil
}

// SetBatch stores multiple key-value pairs with optional TTL.
func (p *Provider) SetBatch(ctx context.Context, items map[string][]byte, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	pipe := p.client.Pipeline()
	for k, v := range items {
		pipe.Set(ctx, k, v, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}
