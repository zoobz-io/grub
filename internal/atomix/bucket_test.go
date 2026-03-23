package atomix

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/zoobz-io/atom"
	"github.com/zoobz-io/grub/internal/shared"
)

// mockBucketProvider implements BucketProvider for testing.
type mockBucketProvider struct {
	data      map[string][]byte
	info      map[string]*shared.ObjectInfo
	getErr    error
	putErr    error
	deleteErr error
	existsErr error
}

func newMockBucketProvider() *mockBucketProvider {
	return &mockBucketProvider{
		data: make(map[string][]byte),
		info: make(map[string]*shared.ObjectInfo),
	}
}

var errBucketNotFound = errors.New("not found")

func (m *mockBucketProvider) Get(_ context.Context, key string) ([]byte, *shared.ObjectInfo, error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	data, ok := m.data[key]
	if !ok {
		return nil, nil, errBucketNotFound
	}
	info := m.info[key]
	if info == nil {
		info = &shared.ObjectInfo{Key: key}
	}
	return data, info, nil
}

func (m *mockBucketProvider) Put(_ context.Context, key string, data []byte, info *shared.ObjectInfo) error {
	if m.putErr != nil {
		return m.putErr
	}
	m.data[key] = data
	m.info[key] = info
	return nil
}

func (m *mockBucketProvider) Delete(_ context.Context, key string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	if _, ok := m.data[key]; !ok {
		return errBucketNotFound
	}
	delete(m.data, key)
	delete(m.info, key)
	return nil
}

func (m *mockBucketProvider) Exists(_ context.Context, key string) (bool, error) {
	if m.existsErr != nil {
		return false, m.existsErr
	}
	_, ok := m.data[key]
	return ok, nil
}

// failingBucketCodec is a codec that can be configured to fail.
type failingBucketCodec struct {
	encodeErr error
	decodeErr error
}

func (f *failingBucketCodec) Encode(v any) ([]byte, error) {
	if f.encodeErr != nil {
		return nil, f.encodeErr
	}
	return json.Marshal(v)
}

func (f *failingBucketCodec) Decode(data []byte, v any) error {
	if f.decodeErr != nil {
		return f.decodeErr
	}
	return json.Unmarshal(data, v)
}

type testPayload struct {
	Field1 string `json:"field1" atom:"field1"`
	Field2 int64  `json:"field2" atom:"field2"`
}

func TestNewBucket(t *testing.T) {
	provider := newMockBucketProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testPayload]()
	spec := atomizer.Spec()

	bucket := NewBucket[testPayload](provider, codec, spec)

	if bucket == nil {
		t.Fatal("NewBucket returned nil")
	}
	if bucket.provider != provider {
		t.Error("provider not set correctly")
	}
}

func TestBucket_Spec(t *testing.T) {
	provider := newMockBucketProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testPayload]()
	spec := atomizer.Spec()

	bucket := NewBucket[testPayload](provider, codec, spec)

	// Verify Spec returns a valid spec (can't compare directly due to slice)
	returnedSpec := bucket.Spec()
	if returnedSpec.TypeName != spec.TypeName {
		t.Error("Spec returned incorrect value")
	}
}

func TestBucket_Get(t *testing.T) {
	provider := newMockBucketProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testPayload]()
	spec := atomizer.Spec()
	bucket := NewBucket[testPayload](provider, codec, spec)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		provider.data["obj1"] = []byte(`{"field1":"test","field2":42}`)
		provider.info["obj1"] = &shared.ObjectInfo{
			Key:         "obj1",
			ContentType: "application/json",
			Size:        100,
			ETag:        "etag1",
		}

		obj, err := bucket.Get(ctx, "obj1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if obj.Key != "obj1" {
			t.Errorf("unexpected Key: %q", obj.Key)
		}
		if obj.ContentType != "application/json" {
			t.Errorf("unexpected ContentType: %q", obj.ContentType)
		}
		if obj.Data == nil {
			t.Fatal("Data atom is nil")
		}

		// Note: atom uses struct field names (Field1) not tag values (field1)
		if obj.Data.Strings["Field1"] != "test" {
			t.Errorf("unexpected Field1: %v", obj.Data.Strings["Field1"])
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := bucket.Get(ctx, "nonexistent")
		if err == nil {
			t.Error("expected error for missing key")
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		provider.data["bad"] = []byte(`{invalid}`)
		provider.info["bad"] = &shared.ObjectInfo{Key: "bad"}

		_, err := bucket.Get(ctx, "bad")
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
	})
}

func TestBucket_Put(t *testing.T) {
	provider := newMockBucketProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testPayload]()
	spec := atomizer.Spec()
	bucket := NewBucket[testPayload](provider, codec, spec)
	ctx := context.Background()

	t.Run("basic put", func(t *testing.T) {
		payload := testPayload{Field1: "hello", Field2: 123}
		a := atomizer.Atomize(&payload)

		obj := &shared.AtomicObject{
			Key:         "new-obj",
			ContentType: "application/json",
			Metadata:    map[string]string{"key": "value"},
			Data:        a,
		}

		err := bucket.Put(ctx, "new-obj", obj)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		if _, ok := provider.data["new-obj"]; !ok {
			t.Error("object not stored in provider")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.putErr = errors.New("put error")
		defer func() { provider.putErr = nil }()

		payload := testPayload{Field1: "fail", Field2: 0}
		a := atomizer.Atomize(&payload)

		obj := &shared.AtomicObject{
			Key:  "fail-obj",
			Data: a,
		}

		err := bucket.Put(ctx, "fail-obj", obj)
		if err == nil {
			t.Error("expected provider error")
		}
	})

	t.Run("encode error", func(t *testing.T) {
		failCodec := &failingBucketCodec{encodeErr: errors.New("encode failed")}
		b := NewBucket[testPayload](provider, failCodec, spec)

		payload := testPayload{Field1: "encode-fail", Field2: 0}
		a := atomizer.Atomize(&payload)

		obj := &shared.AtomicObject{
			Key:  "encode-fail-obj",
			Data: a,
		}

		err := b.Put(ctx, "encode-fail-obj", obj)
		if err == nil {
			t.Error("expected encode error")
		}
	})
}

func TestBucket_Delete(t *testing.T) {
	provider := newMockBucketProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testPayload]()
	spec := atomizer.Spec()
	bucket := NewBucket[testPayload](provider, codec, spec)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		provider.data["delete-me"] = []byte(`{}`)
		provider.info["delete-me"] = &shared.ObjectInfo{Key: "delete-me"}

		err := bucket.Delete(ctx, "delete-me")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if _, ok := provider.data["delete-me"]; ok {
			t.Error("object should have been deleted")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		err := bucket.Delete(ctx, "nonexistent")
		if err == nil {
			t.Error("expected error for missing key")
		}
	})
}

//nolint:dupl // Test structure mirrors TestStore_Exists intentionally.
func TestBucket_Exists(t *testing.T) {
	provider := newMockBucketProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testPayload]()
	spec := atomizer.Spec()
	bucket := NewBucket[testPayload](provider, codec, spec)
	ctx := context.Background()

	provider.data["exists"] = []byte(`{}`)

	t.Run("existing key", func(t *testing.T) {
		exists, err := bucket.Exists(ctx, "exists")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("expected key to exist")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		exists, err := bucket.Exists(ctx, "missing")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected key to not exist")
		}
	})
}

func TestBucket_RoundTrip(t *testing.T) {
	provider := newMockBucketProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testPayload]()
	spec := atomizer.Spec()
	bucket := NewBucket[testPayload](provider, codec, spec)
	ctx := context.Background()

	original := testPayload{Field1: "hello", Field2: 999}
	a := atomizer.Atomize(&original)

	obj := &shared.AtomicObject{
		Key:         "roundtrip",
		ContentType: "application/json",
		Metadata:    map[string]string{"foo": "bar"},
		Data:        a,
	}

	if err := bucket.Put(ctx, "roundtrip", obj); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, err := bucket.Get(ctx, "roundtrip")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Key != obj.Key {
		t.Errorf("Key mismatch: got %q, want %q", retrieved.Key, obj.Key)
	}

	// Note: atom uses struct field names (Field1, Field2) not tag values (field1, field2)
	if retrieved.Data.Strings["Field1"] != original.Field1 {
		t.Errorf("Field1 mismatch: got %v, want %q", retrieved.Data.Strings["Field1"], original.Field1)
	}
	if retrieved.Data.Ints["Field2"] != original.Field2 {
		t.Errorf("Field2 mismatch: got %v, want %d", retrieved.Data.Ints["Field2"], original.Field2)
	}
}

// Additional test to verify codec is used correctly.
func TestBucket_EncodeDecode(t *testing.T) {
	provider := newMockBucketProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testPayload]()
	spec := atomizer.Spec()
	bucket := NewBucket[testPayload](provider, codec, spec)
	ctx := context.Background()

	payload := testPayload{Field1: "encode-test", Field2: 555}
	a := atomizer.Atomize(&payload)

	obj := &shared.AtomicObject{
		Key:  "encode-key",
		Data: a,
	}

	if err := bucket.Put(ctx, "encode-key", obj); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify raw data is valid JSON
	rawData := provider.data["encode-key"]
	var decoded testPayload
	if err := json.Unmarshal(rawData, &decoded); err != nil {
		t.Fatalf("stored data is not valid JSON: %v", err)
	}
	if decoded.Field1 != payload.Field1 || decoded.Field2 != payload.Field2 {
		t.Errorf("stored data mismatch: got %+v, want %+v", decoded, payload)
	}
}
