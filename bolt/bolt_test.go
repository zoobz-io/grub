package bolt

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zoobz-io/grub"
	"go.etcd.io/bbolt"
)

func setupTestDB(t *testing.T) *bbolt.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(dbPath)
	})
	return db
}

func TestNew(t *testing.T) {
	db := setupTestDB(t)
	provider := New(db, "test-bucket")

	if provider == nil {
		t.Fatal("New returned nil")
	}
	if provider.db != db {
		t.Error("db not set correctly")
	}
	if string(provider.bucket) != "test-bucket" {
		t.Errorf("bucket not set correctly: %q", string(provider.bucket))
	}
}

func TestProvider_Get(t *testing.T) {
	db := setupTestDB(t)
	provider := New(db, "test")
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		// Setup
		err := db.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("test"))
			if err != nil {
				return err
			}
			return b.Put([]byte("key1"), []byte("value1"))
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		data, err := provider.Get(ctx, "key1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if string(data) != "value1" {
			t.Errorf("unexpected value: %q", string(data))
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := provider.Get(ctx, "nonexistent")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("missing bucket", func(t *testing.T) {
		provider2 := New(db, "nonexistent-bucket")
		_, err := provider2.Get(ctx, "key")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound for missing bucket, got %v", err)
		}
	})
}

func TestProvider_Set(t *testing.T) {
	db := setupTestDB(t)
	provider := New(db, "test")
	ctx := context.Background()

	t.Run("basic set", func(t *testing.T) {
		err := provider.Set(ctx, "key1", []byte("value1"), 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Verify
		data, err := provider.Get(ctx, "key1")
		if err != nil {
			t.Fatalf("verification Get failed: %v", err)
		}
		if string(data) != "value1" {
			t.Errorf("unexpected stored value: %q", string(data))
		}
	})

	t.Run("with ttl returns error", func(t *testing.T) {
		err := provider.Set(ctx, "ttl-key", []byte("value"), time.Hour)
		if !errors.Is(err, grub.ErrTTLNotSupported) {
			t.Errorf("expected ErrTTLNotSupported, got %v", err)
		}
	})

	t.Run("zero ttl succeeds", func(t *testing.T) {
		err := provider.Set(ctx, "zero-ttl", []byte("value"), 0)
		if err != nil {
			t.Errorf("Set with zero TTL should succeed: %v", err)
		}
	})

	t.Run("overwrite existing", func(t *testing.T) {
		_ = provider.Set(ctx, "overwrite", []byte("v1"), 0)
		err := provider.Set(ctx, "overwrite", []byte("v2"), 0)
		if err != nil {
			t.Fatalf("overwrite Set failed: %v", err)
		}

		data, _ := provider.Get(ctx, "overwrite")
		if string(data) != "v2" {
			t.Errorf("expected 'v2', got %q", string(data))
		}
	})
}

func TestProvider_Delete(t *testing.T) {
	db := setupTestDB(t)
	provider := New(db, "test")
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		_ = provider.Set(ctx, "delete-me", []byte("value"), 0)

		err := provider.Delete(ctx, "delete-me")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = provider.Get(ctx, "delete-me")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		err := provider.Delete(ctx, "nonexistent")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("missing bucket", func(t *testing.T) {
		provider2 := New(db, "nonexistent-bucket")
		err := provider2.Delete(ctx, "key")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound for missing bucket, got %v", err)
		}
	})
}

func TestProvider_Exists(t *testing.T) {
	db := setupTestDB(t)
	provider := New(db, "test")
	ctx := context.Background()

	_ = provider.Set(ctx, "exists", []byte("value"), 0)

	t.Run("existing key", func(t *testing.T) {
		exists, err := provider.Exists(ctx, "exists")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("expected key to exist")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		exists, err := provider.Exists(ctx, "missing")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected key to not exist")
		}
	})

	t.Run("missing bucket", func(t *testing.T) {
		provider2 := New(db, "nonexistent-bucket")
		exists, err := provider2.Exists(ctx, "key")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected key to not exist in missing bucket")
		}
	})
}

func TestProvider_List(t *testing.T) {
	db := setupTestDB(t)
	provider := New(db, "test")
	ctx := context.Background()

	// Setup test data
	_ = provider.Set(ctx, "prefix/a", []byte("a"), 0)
	_ = provider.Set(ctx, "prefix/b", []byte("b"), 0)
	_ = provider.Set(ctx, "prefix/c", []byte("c"), 0)
	_ = provider.Set(ctx, "other/x", []byte("x"), 0)

	t.Run("with prefix", func(t *testing.T) {
		keys, err := provider.List(ctx, "prefix/", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 3 {
			t.Errorf("expected 3 keys, got %d: %v", len(keys), keys)
		}
	})

	t.Run("with limit", func(t *testing.T) {
		keys, err := provider.List(ctx, "prefix/", 2)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 2 {
			t.Errorf("expected 2 keys, got %d", len(keys))
		}
	})

	t.Run("empty prefix", func(t *testing.T) {
		keys, err := provider.List(ctx, "", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 4 {
			t.Errorf("expected 4 keys, got %d", len(keys))
		}
	})

	t.Run("no matches", func(t *testing.T) {
		keys, err := provider.List(ctx, "nonexistent/", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 0 {
			t.Errorf("expected 0 keys, got %d", len(keys))
		}
	})

	t.Run("missing bucket", func(t *testing.T) {
		provider2 := New(db, "nonexistent-bucket")
		keys, err := provider2.List(ctx, "", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 0 {
			t.Errorf("expected 0 keys for missing bucket, got %d", len(keys))
		}
	})
}

func TestProvider_List_ContextCancellation(t *testing.T) {
	db := setupTestDB(t)
	provider := New(db, "test")

	// Setup many keys
	for i := 0; i < 100; i++ {
		key := "key" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		_ = provider.Set(context.Background(), key, []byte("value"), 0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := provider.List(ctx, "", 0)
	if err == nil {
		t.Error("expected context cancellation error")
	}
}

func TestProvider_GetBatch(t *testing.T) {
	db := setupTestDB(t)
	provider := New(db, "test")
	ctx := context.Background()

	// Setup
	_ = provider.Set(ctx, "batch1", []byte("v1"), 0)
	_ = provider.Set(ctx, "batch2", []byte("v2"), 0)

	t.Run("all exist", func(t *testing.T) {
		result, err := provider.GetBatch(ctx, []string{"batch1", "batch2"})
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if len(result) != 2 {
			t.Errorf("expected 2 results, got %d", len(result))
		}
		if string(result["batch1"]) != "v1" {
			t.Errorf("unexpected value for batch1: %q", string(result["batch1"]))
		}
	})

	t.Run("partial exists", func(t *testing.T) {
		result, err := provider.GetBatch(ctx, []string{"batch1", "missing"})
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if len(result) != 1 {
			t.Errorf("expected 1 result, got %d", len(result))
		}
	})

	t.Run("none exist", func(t *testing.T) {
		result, err := provider.GetBatch(ctx, []string{"x", "y", "z"})
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 results, got %d", len(result))
		}
	})

	t.Run("missing bucket", func(t *testing.T) {
		provider2 := New(db, "nonexistent-bucket")
		result, err := provider2.GetBatch(ctx, []string{"key"})
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 results for missing bucket, got %d", len(result))
		}
	})
}

func TestProvider_SetBatch(t *testing.T) {
	db := setupTestDB(t)
	provider := New(db, "test")
	ctx := context.Background()

	t.Run("basic batch", func(t *testing.T) {
		items := map[string][]byte{
			"sb1": []byte("v1"),
			"sb2": []byte("v2"),
		}
		err := provider.SetBatch(ctx, items, 0)
		if err != nil {
			t.Fatalf("SetBatch failed: %v", err)
		}

		// Verify
		v1, _ := provider.Get(ctx, "sb1")
		v2, _ := provider.Get(ctx, "sb2")
		if string(v1) != "v1" || string(v2) != "v2" {
			t.Errorf("batch values not stored correctly")
		}
	})

	t.Run("with ttl returns error", func(t *testing.T) {
		items := map[string][]byte{"ttl1": []byte("v1")}
		err := provider.SetBatch(ctx, items, time.Hour)
		if !errors.Is(err, grub.ErrTTLNotSupported) {
			t.Errorf("expected ErrTTLNotSupported, got %v", err)
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		err := provider.SetBatch(ctx, map[string][]byte{}, 0)
		if err != nil {
			t.Fatalf("SetBatch empty failed: %v", err)
		}
	})
}

func TestProvider_RoundTrip(t *testing.T) {
	db := setupTestDB(t)
	provider := New(db, "test")
	ctx := context.Background()

	original := []byte("hello world")

	if err := provider.Set(ctx, "rt", original, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, err := provider.Get(ctx, "rt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(original) {
		t.Errorf("mismatch: got %q, want %q", string(retrieved), string(original))
	}
}

func TestHasPrefix(t *testing.T) {
	tests := []struct {
		s      []byte
		prefix []byte
		want   bool
	}{
		{[]byte("hello"), []byte("he"), true},
		{[]byte("hello"), []byte("hello"), true},
		{[]byte("hello"), []byte("helloworld"), false},
		{[]byte("hello"), []byte(""), true},
		{[]byte(""), []byte(""), true},
		{[]byte(""), []byte("a"), false},
		{[]byte("abc"), []byte("xyz"), false},
	}

	for _, tt := range tests {
		got := hasPrefix(tt.s, tt.prefix)
		if got != tt.want {
			t.Errorf("hasPrefix(%q, %q) = %v, want %v", tt.s, tt.prefix, got, tt.want)
		}
	}
}
