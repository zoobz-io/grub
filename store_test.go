package grub

import (
	"context"
	"errors"
	"testing"
	"time"
)

// mockStoreProvider implements StoreProvider for testing.
type mockStoreProvider struct {
	data        map[string][]byte
	getErr      error
	setErr      error
	deleteErr   error
	existsErr   error
	listErr     error
	getBatchErr error
}

func newMockStoreProvider() *mockStoreProvider {
	return &mockStoreProvider{
		data: make(map[string][]byte),
	}
}

func (m *mockStoreProvider) Get(_ context.Context, key string) ([]byte, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	data, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return data, nil
}

func (m *mockStoreProvider) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.data[key] = value
	return nil
}

func (m *mockStoreProvider) Delete(_ context.Context, key string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	if _, ok := m.data[key]; !ok {
		return ErrNotFound
	}
	delete(m.data, key)
	return nil
}

func (m *mockStoreProvider) Exists(_ context.Context, key string) (bool, error) {
	if m.existsErr != nil {
		return false, m.existsErr
	}
	_, ok := m.data[key]
	return ok, nil
}

func (m *mockStoreProvider) List(_ context.Context, prefix string, limit int) ([]string, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	var keys []string
	for k := range m.data {
		if prefix == "" || (len(k) >= len(prefix) && k[:len(prefix)] == prefix) {
			keys = append(keys, k)
			if limit > 0 && len(keys) >= limit {
				break
			}
		}
	}
	return keys, nil
}

func (m *mockStoreProvider) GetBatch(_ context.Context, keys []string) (map[string][]byte, error) {
	if m.getBatchErr != nil {
		return nil, m.getBatchErr
	}
	result := make(map[string][]byte)
	for _, k := range keys {
		if v, ok := m.data[k]; ok {
			result[k] = v
		}
	}
	return result, nil
}

func (m *mockStoreProvider) SetBatch(_ context.Context, items map[string][]byte, _ time.Duration) error {
	if m.setErr != nil {
		return m.setErr
	}
	for k, v := range items {
		m.data[k] = v
	}
	return nil
}

type testRecord struct {
	ID   int    `json:"id" atom:"id"`
	Name string `json:"name" atom:"name"`
}

const testName = "test" // test fixture value

// failingCodec is a codec that can be configured to fail on encode or decode.
type failingCodec struct {
	encodeErr error
	decodeErr error
}

func (f *failingCodec) Encode(v any) ([]byte, error) {
	if f.encodeErr != nil {
		return nil, f.encodeErr
	}
	return JSONCodec{}.Encode(v)
}

func (f *failingCodec) Decode(data []byte, v any) error {
	if f.decodeErr != nil {
		return f.decodeErr
	}
	return JSONCodec{}.Decode(data, v)
}

func TestNewStore(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)

	if store == nil {
		t.Fatal("NewStore returned nil")
	}
	if store.provider != provider {
		t.Error("provider not set correctly")
	}
	if store.codec == nil {
		t.Error("codec should default to JSONCodec")
	}
}

func TestNewStoreWithCodec(t *testing.T) {
	provider := newMockStoreProvider()
	codec := GobCodec{}
	store := NewStoreWithCodec[testRecord](provider, codec)

	if store == nil {
		t.Fatal("NewStoreWithCodec returned nil")
	}
}

func TestStore_Get(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		provider.data["key1"] = []byte(`{"id":1,"name":"` + testName + `"}`)

		record, err := store.Get(ctx, "key1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if record.ID != 1 {
			t.Errorf("expected ID 1, got %d", record.ID)
		}
		if record.Name != testName {
			t.Errorf("expected Name 'test', got %q", record.Name)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := store.Get(ctx, "nonexistent")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		provider.data["bad"] = []byte(`{invalid}`)

		_, err := store.Get(ctx, "bad")
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.getErr = errors.New("provider error")
		defer func() { provider.getErr = nil }()

		_, err := store.Get(ctx, "key1")
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestStore_Set(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)
	ctx := context.Background()

	t.Run("basic set", func(t *testing.T) {
		record := &testRecord{ID: 1, Name: testName}
		err := store.Set(ctx, "key1", record, 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		if _, ok := provider.data["key1"]; !ok {
			t.Error("key not stored in provider")
		}
	})

	t.Run("with ttl", func(t *testing.T) {
		record := &testRecord{ID: 2, Name: "ttl"}
		err := store.Set(ctx, "key2", record, time.Hour)
		if err != nil {
			t.Fatalf("Set with TTL failed: %v", err)
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.setErr = errors.New("set error")
		defer func() { provider.setErr = nil }()

		record := &testRecord{ID: 3, Name: "fail"}
		err := store.Set(ctx, "key3", record, 0)
		if err == nil {
			t.Error("expected provider error")
		}
	})

	t.Run("encode error", func(t *testing.T) {
		codec := &failingCodec{encodeErr: errors.New("encode failed")}
		s := NewStoreWithCodec[testRecord](provider, codec)

		record := &testRecord{ID: 4, Name: "encode-fail"}
		err := s.Set(ctx, "key4", record, 0)
		if err == nil {
			t.Error("expected encode error")
		}
	})
}

func TestStore_Delete(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		provider.data["delete-me"] = []byte(`{}`)

		err := store.Delete(ctx, "delete-me")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if _, ok := provider.data["delete-me"]; ok {
			t.Error("key should have been deleted")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		err := store.Delete(ctx, "nonexistent")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestStore_Exists(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)
	ctx := context.Background()

	provider.data["exists"] = []byte(`{}`)

	t.Run("existing key", func(t *testing.T) {
		exists, err := store.Exists(ctx, "exists")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("expected key to exist")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		exists, err := store.Exists(ctx, "missing")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected key to not exist")
		}
	})
}

func TestStore_List(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)
	ctx := context.Background()

	provider.data["prefix/a"] = []byte(`{}`)
	provider.data["prefix/b"] = []byte(`{}`)
	provider.data["other/c"] = []byte(`{}`)

	t.Run("with prefix", func(t *testing.T) {
		keys, err := store.List(ctx, "prefix/", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 2 {
			t.Errorf("expected 2 keys, got %d", len(keys))
		}
	})

	t.Run("with limit", func(t *testing.T) {
		keys, err := store.List(ctx, "", 1)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 1 {
			t.Errorf("expected 1 key, got %d", len(keys))
		}
	})

	t.Run("empty prefix", func(t *testing.T) {
		keys, err := store.List(ctx, "", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 3 {
			t.Errorf("expected 3 keys, got %d", len(keys))
		}
	})
}

func TestStore_GetBatch(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)
	ctx := context.Background()

	provider.data["batch1"] = []byte(`{"id":1,"name":"one"}`)
	provider.data["batch2"] = []byte(`{"id":2,"name":"two"}`)

	t.Run("all exist", func(t *testing.T) {
		result, err := store.GetBatch(ctx, []string{"batch1", "batch2"})
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if len(result) != 2 {
			t.Errorf("expected 2 results, got %d", len(result))
		}
		if result["batch1"].ID != 1 {
			t.Errorf("unexpected ID for batch1: %d", result["batch1"].ID)
		}
	})

	t.Run("partial exists", func(t *testing.T) {
		result, err := store.GetBatch(ctx, []string{"batch1", "missing"})
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if len(result) != 1 {
			t.Errorf("expected 1 result, got %d", len(result))
		}
	})

	t.Run("none exist", func(t *testing.T) {
		result, err := store.GetBatch(ctx, []string{"x", "y", "z"})
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 results, got %d", len(result))
		}
	})

	t.Run("decode error", func(t *testing.T) {
		codec := &failingCodec{decodeErr: errors.New("decode failed")}
		s := NewStoreWithCodec[testRecord](provider, codec)

		_, err := s.GetBatch(ctx, []string{"batch1"})
		if err == nil {
			t.Error("expected decode error")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.getBatchErr = errors.New("batch get error")
		defer func() { provider.getBatchErr = nil }()

		_, err := store.GetBatch(ctx, []string{"batch1"})
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestStore_SetBatch(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)
	ctx := context.Background()

	t.Run("basic batch", func(t *testing.T) {
		items := map[string]*testRecord{
			"b1": {ID: 1, Name: "one"},
			"b2": {ID: 2, Name: "two"},
		}
		err := store.SetBatch(ctx, items, 0)
		if err != nil {
			t.Fatalf("SetBatch failed: %v", err)
		}

		if len(provider.data) < 2 {
			t.Error("batch items not stored")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.setErr = errors.New("batch error")
		defer func() { provider.setErr = nil }()

		items := map[string]*testRecord{
			"fail": {ID: 99, Name: "fail"},
		}
		err := store.SetBatch(ctx, items, 0)
		if err == nil {
			t.Error("expected provider error")
		}
	})

	t.Run("encode error", func(t *testing.T) {
		codec := &failingCodec{encodeErr: errors.New("encode failed")}
		s := NewStoreWithCodec[testRecord](provider, codec)

		items := map[string]*testRecord{
			"fail": {ID: 100, Name: "encode-fail"},
		}
		err := s.SetBatch(ctx, items, 0)
		if err == nil {
			t.Error("expected encode error")
		}
	})
}

func TestStore_RoundTrip(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)
	ctx := context.Background()

	original := &testRecord{ID: 42, Name: "roundtrip"}

	if err := store.Set(ctx, "rt", original, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, err := store.Get(ctx, "rt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.ID != original.ID {
		t.Errorf("ID mismatch: got %d, want %d", retrieved.ID, original.ID)
	}
	if retrieved.Name != original.Name {
		t.Errorf("Name mismatch: got %q, want %q", retrieved.Name, original.Name)
	}
}

func TestStore_WithGobCodec(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStoreWithCodec[testRecord](provider, GobCodec{})
	ctx := context.Background()

	original := &testRecord{ID: 99, Name: "gob"}

	if err := store.Set(ctx, "gob-key", original, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, err := store.Get(ctx, "gob-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.ID != original.ID || retrieved.Name != original.Name {
		t.Errorf("roundtrip mismatch: got %+v, want %+v", retrieved, original)
	}
}

func TestStore_Atomic(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)
	ctx := context.Background()

	provider.data["atomic-key"] = []byte(`{"id":42,"name":"atomic"}`)

	atomic := store.Atomic()
	if atomic == nil {
		t.Fatal("Atomic returned nil")
	}

	// Verify it returns the same instance
	atomic2 := store.Atomic()
	if atomic != atomic2 {
		t.Error("Atomic should return cached instance")
	}

	// Test that atomic view works
	a, err := atomic.Get(ctx, "atomic-key")
	if err != nil {
		t.Fatalf("Atomic Get failed: %v", err)
	}
	// atom uses struct field names as keys
	if a.Ints["ID"] != 42 {
		t.Errorf("unexpected ID: %v", a.Ints["ID"])
	}
	if a.Strings["Name"] != "atomic" {
		t.Errorf("unexpected Name: %q", a.Strings["Name"])
	}
}
