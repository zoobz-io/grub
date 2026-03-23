// Package bolt provides a grub StoreProvider implementation for BoltDB.
package bolt

import (
	"context"
	"time"

	"github.com/zoobz-io/grub"
	"go.etcd.io/bbolt"
)

// Provider implements grub.StoreProvider for BoltDB.
type Provider struct {
	db     *bbolt.DB
	bucket []byte
}

// New creates a Bolt provider with the given database and bucket name.
func New(db *bbolt.DB, bucket string) *Provider {
	return &Provider{
		db:     db,
		bucket: []byte(bucket),
	}
}

// Get retrieves the value at key.
func (p *Provider) Get(_ context.Context, key string) ([]byte, error) {
	var data []byte
	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			return grub.ErrNotFound
		}
		v := b.Get([]byte(key))
		if v == nil {
			return grub.ErrNotFound
		}
		data = make([]byte, len(v))
		copy(data, v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Set stores value at key.
// Returns ErrTTLNotSupported if TTL > 0, as BoltDB does not support expiration.
func (p *Provider) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl > 0 {
		return grub.ErrTTLNotSupported
	}
	return p.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(p.bucket)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), value)
	})
}

// Delete removes the value at key.
func (p *Provider) Delete(_ context.Context, key string) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			return grub.ErrNotFound
		}
		v := b.Get([]byte(key))
		if v == nil {
			return grub.ErrNotFound
		}
		return b.Delete([]byte(key))
	})
}

// Exists checks whether a key exists.
func (p *Provider) Exists(_ context.Context, key string) (bool, error) {
	var exists bool
	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			exists = false
			return nil
		}
		exists = b.Get([]byte(key)) != nil
		return nil
	})
	return exists, err
}

// List returns keys matching the given prefix.
// Respects context cancellation during iteration.
func (p *Provider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	var keys []string
	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		prefixBytes := []byte(prefix)
		for k, _ := c.Seek(prefixBytes); k != nil; k, _ = c.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if len(prefixBytes) > 0 && !hasPrefix(k, prefixBytes) {
				break
			}
			keys = append(keys, string(k))
			if limit > 0 && len(keys) >= limit {
				break
			}
		}
		return nil
	})
	return keys, err
}

func hasPrefix(s, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i := range prefix {
		if s[i] != prefix[i] {
			return false
		}
	}
	return true
}

// GetBatch retrieves multiple values by key.
func (p *Provider) GetBatch(_ context.Context, keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			return nil
		}
		for _, key := range keys {
			v := b.Get([]byte(key))
			if v != nil {
				val := make([]byte, len(v))
				copy(val, v)
				result[key] = val
			}
		}
		return nil
	})
	return result, err
}

// SetBatch stores multiple key-value pairs.
// Returns ErrTTLNotSupported if TTL > 0, as BoltDB does not support expiration.
func (p *Provider) SetBatch(_ context.Context, items map[string][]byte, ttl time.Duration) error {
	if ttl > 0 {
		return grub.ErrTTLNotSupported
	}
	return p.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(p.bucket)
		if err != nil {
			return err
		}
		for k, v := range items {
			if err := b.Put([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	})
}
