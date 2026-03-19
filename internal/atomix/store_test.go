package atomix

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/zoobzio/atom"
)

// mockStoreProvider implements StoreProvider for testing.
type mockStoreProvider struct {
	data      map[string][]byte
	getErr    error
	setErr    error
	deleteErr error
	existsErr error
}

func newMockStoreProvider() *mockStoreProvider {
	return &mockStoreProvider{
		data: make(map[string][]byte),
	}
}

var errNotFound = errors.New("not found")

func (m *mockStoreProvider) Get(_ context.Context, key string) ([]byte, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	data, ok := m.data[key]
	if !ok {
		return nil, errNotFound
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
		return errNotFound
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

// jsonCodec implements Codec for testing.
type jsonCodec struct{}

func (jsonCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// failingCodec is a codec that can be configured to fail.
type failingCodec struct {
	encodeErr error
	decodeErr error
}

func (f *failingCodec) Encode(v any) ([]byte, error) {
	if f.encodeErr != nil {
		return nil, f.encodeErr
	}
	return json.Marshal(v)
}

func (f *failingCodec) Decode(data []byte, v any) error {
	if f.decodeErr != nil {
		return f.decodeErr
	}
	return json.Unmarshal(data, v)
}

type testRecord struct {
	ID   int64  `json:"id" atom:"id"`
	Name string `json:"name" atom:"name"`
}

func TestNewStore(t *testing.T) {
	provider := newMockStoreProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testRecord]()
	spec := atomizer.Spec()

	store := NewStore[testRecord](provider, codec, spec)

	if store == nil {
		t.Fatal("NewStore returned nil")
	}
	if store.provider != provider {
		t.Error("provider not set correctly")
	}
}

func TestStore_Spec(t *testing.T) {
	provider := newMockStoreProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testRecord]()
	spec := atomizer.Spec()

	store := NewStore[testRecord](provider, codec, spec)

	// Verify Spec returns a valid spec (can't compare directly due to slice)
	returnedSpec := store.Spec()
	if returnedSpec.TypeName != spec.TypeName {
		t.Error("Spec returned incorrect value")
	}
}

func TestStore_Get(t *testing.T) {
	provider := newMockStoreProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testRecord]()
	spec := atomizer.Spec()
	store := NewStore[testRecord](provider, codec, spec)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		provider.data["key1"] = []byte(`{"id":1,"name":"test"}`)

		result, err := store.Get(ctx, "key1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if result == nil {
			t.Fatal("Get returned nil atom")
		}

		// Verify atom contains correct data using the Ints map
		// Note: atom uses struct field names (ID, Name) not tag values (id, name)
		if result.Ints["ID"] != 1 {
			t.Errorf("unexpected ID value: %v", result.Ints["ID"])
		}
		if result.Strings["Name"] != "test" {
			t.Errorf("unexpected Name value: %v", result.Strings["Name"])
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := store.Get(ctx, "nonexistent")
		if err == nil {
			t.Error("expected error for missing key")
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
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testRecord]()
	spec := atomizer.Spec()
	store := NewStore[testRecord](provider, codec, spec)
	ctx := context.Background()

	t.Run("basic set", func(t *testing.T) {
		record := testRecord{ID: 1, Name: "test"}
		a := atomizer.Atomize(&record)

		err := store.Set(ctx, "key1", a, 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		if _, ok := provider.data["key1"]; !ok {
			t.Error("key not stored in provider")
		}
	})

	t.Run("with ttl", func(t *testing.T) {
		record := testRecord{ID: 2, Name: "ttl"}
		a := atomizer.Atomize(&record)

		err := store.Set(ctx, "key2", a, time.Hour)
		if err != nil {
			t.Fatalf("Set with TTL failed: %v", err)
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.setErr = errors.New("set error")
		defer func() { provider.setErr = nil }()

		record := testRecord{ID: 3, Name: "fail"}
		a := atomizer.Atomize(&record)

		err := store.Set(ctx, "key3", a, 0)
		if err == nil {
			t.Error("expected provider error")
		}
	})

	t.Run("encode error", func(t *testing.T) {
		failCodec := &failingCodec{encodeErr: errors.New("encode failed")}
		s := NewStore[testRecord](provider, failCodec, spec)

		record := testRecord{ID: 4, Name: "encode-fail"}
		a := atomizer.Atomize(&record)

		err := s.Set(ctx, "key4", a, 0)
		if err == nil {
			t.Error("expected encode error")
		}
	})
}

func TestStore_Delete(t *testing.T) {
	provider := newMockStoreProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testRecord]()
	spec := atomizer.Spec()
	store := NewStore[testRecord](provider, codec, spec)
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
		if err == nil {
			t.Error("expected error for missing key")
		}
	})
}

//nolint:dupl // Test structure mirrors TestBucket_Exists intentionally.
func TestStore_Exists(t *testing.T) {
	provider := newMockStoreProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testRecord]()
	spec := atomizer.Spec()
	store := NewStore[testRecord](provider, codec, spec)
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

func TestStore_RoundTrip(t *testing.T) {
	provider := newMockStoreProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testRecord]()
	spec := atomizer.Spec()
	store := NewStore[testRecord](provider, codec, spec)
	ctx := context.Background()

	original := testRecord{ID: 42, Name: "roundtrip"}
	a := atomizer.Atomize(&original)

	if err := store.Set(ctx, "rt", a, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, err := store.Get(ctx, "rt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Ints["ID"] != original.ID {
		t.Errorf("ID mismatch: got %v, want %d", retrieved.Ints["ID"], original.ID)
	}
	if retrieved.Strings["Name"] != original.Name {
		t.Errorf("Name mismatch: got %v, want %q", retrieved.Strings["Name"], original.Name)
	}
}
