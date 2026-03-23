// Package badger provides a grub StoreProvider implementation for BadgerDB.
package badger

import (
	"context"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/zoobz-io/grub"
)

// Provider implements grub.StoreProvider for BadgerDB.
type Provider struct {
	db *badger.DB
}

// New creates a Badger provider with the given database.
func New(db *badger.DB) *Provider {
	return &Provider{db: db}
}

// Get retrieves the value at key.
func (p *Provider) Get(_ context.Context, key string) ([]byte, error) {
	var data []byte
	err := p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return grub.ErrNotFound
		}
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Set stores value at key with optional TTL.
func (p *Provider) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	return p.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), value)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
}

// Delete removes the value at key.
func (p *Provider) Delete(_ context.Context, key string) error {
	return p.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return grub.ErrNotFound
		}
		if err != nil {
			return err
		}
		return txn.Delete([]byte(key))
	})
}

// Exists checks whether a key exists.
func (p *Provider) Exists(_ context.Context, key string) (bool, error) {
	var exists bool
	err := p.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			exists = false
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return nil
	})
	return exists, err
}

// List returns keys matching the given prefix.
// Respects context cancellation during iteration.
func (p *Provider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	var keys []string
	err := p.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			keys = append(keys, string(it.Item().Key()))
			if limit > 0 && len(keys) >= limit {
				break
			}
		}
		return nil
	})
	return keys, err
}

// GetBatch retrieves multiple values by key.
func (p *Provider) GetBatch(_ context.Context, keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	err := p.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get([]byte(key))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result[key] = val
		}
		return nil
	})
	return result, err
}

// SetBatch stores multiple key-value pairs with optional TTL.
func (p *Provider) SetBatch(_ context.Context, items map[string][]byte, ttl time.Duration) error {
	wb := p.db.NewWriteBatch()
	defer wb.Cancel()

	for k, v := range items {
		entry := badger.NewEntry([]byte(k), v)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		if err := wb.SetEntry(entry); err != nil {
			return err
		}
	}
	return wb.Flush()
}
