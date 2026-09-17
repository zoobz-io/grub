package grub

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"testing"
	"time"
)

// mockBucketProvider implements BucketProvider for testing.
type mockBucketProvider struct {
	data      map[string][]byte
	info      map[string]*ObjectInfo
	getErr    error
	putErr    error
	deleteErr error
	existsErr error
	listErr   error
}

func newMockBucketProvider() *mockBucketProvider {
	return &mockBucketProvider{
		data: make(map[string][]byte),
		info: make(map[string]*ObjectInfo),
	}
}

func (m *mockBucketProvider) Get(_ context.Context, key string) (retData []byte, retInfo *ObjectInfo, retErr error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	data, ok := m.data[key]
	if !ok {
		return nil, nil, ErrNotFound
	}
	info := m.info[key]
	if info == nil {
		info = &ObjectInfo{Key: key}
	}
	return data, info, nil
}

func (m *mockBucketProvider) Put(_ context.Context, key string, data []byte, info *ObjectInfo) error {
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
		return ErrNotFound
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

func (m *mockBucketProvider) List(_ context.Context, prefix string, limit int) ([]ObjectInfo, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	infos := m.sortedInfos(prefix)
	if limit > 0 && limit < len(infos) {
		infos = infos[:limit]
	}
	return infos, nil
}

// sortedInfos returns infos for keys matching prefix, ordered by key.
func (m *mockBucketProvider) sortedInfos(prefix string) []ObjectInfo {
	var results []ObjectInfo
	for k, info := range m.info {
		if prefix != "" && (len(k) < len(prefix) || k[:len(prefix)] != prefix) {
			continue
		}
		if info != nil {
			results = append(results, *info)
		} else {
			results = append(results, ObjectInfo{Key: k})
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Key < results[j].Key })
	return results
}

func (m *mockBucketProvider) Stat(_ context.Context, key string) (*ObjectInfo, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if _, ok := m.data[key]; !ok {
		return nil, ErrNotFound
	}
	if info := m.info[key]; info != nil {
		return info, nil
	}
	return &ObjectInfo{Key: key}, nil
}

func (m *mockBucketProvider) ListPage(_ context.Context, prefix, cursor string, limit int) (infos []ObjectInfo, next string, err error) {
	if m.listErr != nil {
		return nil, "", m.listErr
	}
	all := m.sortedInfos(prefix)
	pageSize := limit
	if pageSize <= 0 {
		pageSize = len(all)
	}
	infos = make([]ObjectInfo, 0, pageSize)
	for _, info := range all {
		if cursor != "" && info.Key <= cursor {
			continue
		}
		infos = append(infos, info)
		if len(infos) >= pageSize {
			next = info.Key
			break
		}
	}
	return infos, next, nil
}

func (m *mockBucketProvider) ListLevel(_ context.Context, prefix, delimiter, _ string, _ int) (*Level, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	// Emulate a native listing by folding the flat listing.
	return FoldLevel(prefix, delimiter, m.sortedInfos(prefix)), nil
}

func (m *mockBucketProvider) GetStream(_ context.Context, key string) (io.ReadCloser, *ObjectInfo, error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	data, ok := m.data[key]
	if !ok {
		return nil, nil, ErrNotFound
	}
	info := m.info[key]
	if info == nil {
		info = &ObjectInfo{Key: key}
	}
	return io.NopCloser(bytes.NewReader(data)), info, nil
}

func (m *mockBucketProvider) PutStream(_ context.Context, key string, r io.Reader, info *ObjectInfo) error {
	if m.putErr != nil {
		return m.putErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.data[key] = data
	m.info[key] = info
	return nil
}

type testPayload struct {
	Field1 string `json:"field1" atom:"field1"`
	Field2 int    `json:"field2" atom:"field2"`
}

// failingBucketCodec is a codec that can be configured to fail on encode or decode.
type failingBucketCodec struct {
	encodeErr error
	decodeErr error
}

func (f *failingBucketCodec) Encode(v any) ([]byte, error) {
	if f.encodeErr != nil {
		return nil, f.encodeErr
	}
	return JSONCodec{}.Encode(v)
}

func (f *failingBucketCodec) Decode(data []byte, v any) error {
	if f.decodeErr != nil {
		return f.decodeErr
	}
	return JSONCodec{}.Decode(data, v)
}

func TestNewBucket(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)

	if bucket == nil {
		t.Fatal("NewBucket returned nil")
	}
	if bucket.provider != provider {
		t.Error("provider not set correctly")
	}
	if bucket.codec == nil {
		t.Error("codec should default to JSONCodec")
	}
}

func TestNewBucketWithCodec(t *testing.T) {
	provider := newMockBucketProvider()
	codec := GobCodec{}
	bucket := NewBucketWithCodec[testPayload](provider, codec)

	if bucket == nil {
		t.Fatal("NewBucketWithCodec returned nil")
	}
}

func TestBucket_Get(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		provider.data["obj1"] = []byte(`{"field1":"` + testName + `","field2":42}`)
		provider.info["obj1"] = &ObjectInfo{
			Key:         "obj1",
			ContentType: "application/json",
			Size:        100,
			ETag:        "etag1",
			Metadata:    map[string]string{"meta": "value"},
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
		if obj.Data.Field1 != testName {
			t.Errorf("unexpected Field1: %q", obj.Data.Field1)
		}
		if obj.Data.Field2 != 42 {
			t.Errorf("unexpected Field2: %d", obj.Data.Field2)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := bucket.Get(ctx, "nonexistent")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		provider.data["bad"] = []byte(`{invalid}`)
		provider.info["bad"] = &ObjectInfo{Key: "bad"}

		_, err := bucket.Get(ctx, "bad")
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.getErr = errors.New("provider error")
		defer func() { provider.getErr = nil }()

		_, err := bucket.Get(ctx, "obj1")
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestBucket_Put(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	t.Run("basic put", func(t *testing.T) {
		obj := &Object[testPayload]{
			Key:         "new-obj",
			ContentType: "application/json",
			Metadata:    map[string]string{"key": "value"},
			Data:        testPayload{Field1: "hello", Field2: 123},
		}
		err := bucket.Put(ctx, obj)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		if _, ok := provider.data["new-obj"]; !ok {
			t.Error("object not stored in provider")
		}
		if provider.info["new-obj"] == nil {
			t.Error("object info not stored in provider")
		}
		if provider.info["new-obj"].ContentType != "application/json" {
			t.Error("content type not preserved")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.putErr = errors.New("put error")
		defer func() { provider.putErr = nil }()

		obj := &Object[testPayload]{
			Key:  "fail-obj",
			Data: testPayload{Field1: "fail", Field2: 0},
		}
		err := bucket.Put(ctx, obj)
		if err == nil {
			t.Error("expected provider error")
		}
	})

	t.Run("encode error", func(t *testing.T) {
		codec := &failingBucketCodec{encodeErr: errors.New("encode failed")}
		b := NewBucketWithCodec[testPayload](provider, codec)

		obj := &Object[testPayload]{
			Key:  "encode-fail",
			Data: testPayload{Field1: "fail", Field2: 0},
		}
		err := b.Put(ctx, obj)
		if err == nil {
			t.Error("expected encode error")
		}
	})
}

func TestBucket_Delete(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		provider.data["delete-me"] = []byte(`{}`)
		provider.info["delete-me"] = &ObjectInfo{Key: "delete-me"}

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
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestBucket_Exists(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
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

func TestBucket_List(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	provider.data["prefix/a"] = []byte(`{}`)
	provider.info["prefix/a"] = &ObjectInfo{Key: "prefix/a", Size: 10}
	provider.data["prefix/b"] = []byte(`{}`)
	provider.info["prefix/b"] = &ObjectInfo{Key: "prefix/b", Size: 20}
	provider.data["other/c"] = []byte(`{}`)
	provider.info["other/c"] = &ObjectInfo{Key: "other/c", Size: 30}

	t.Run("with prefix", func(t *testing.T) {
		infos, err := bucket.List(ctx, "prefix/", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(infos) != 2 {
			t.Errorf("expected 2 infos, got %d", len(infos))
		}
	})

	t.Run("with limit", func(t *testing.T) {
		infos, err := bucket.List(ctx, "", 1)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(infos) != 1 {
			t.Errorf("expected 1 info, got %d", len(infos))
		}
	})
}

func TestBucket_RoundTrip(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	original := &Object[testPayload]{
		Key:         "roundtrip",
		ContentType: "application/json",
		Metadata:    map[string]string{"foo": "bar"},
		Data:        testPayload{Field1: "hello", Field2: 999},
	}

	if err := bucket.Put(ctx, original); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, err := bucket.Get(ctx, "roundtrip")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Key != original.Key {
		t.Errorf("Key mismatch: got %q, want %q", retrieved.Key, original.Key)
	}
	if retrieved.ContentType != original.ContentType {
		t.Errorf("ContentType mismatch: got %q, want %q", retrieved.ContentType, original.ContentType)
	}
	if retrieved.Data.Field1 != original.Data.Field1 {
		t.Errorf("Data.Field1 mismatch: got %q, want %q", retrieved.Data.Field1, original.Data.Field1)
	}
	if retrieved.Data.Field2 != original.Data.Field2 {
		t.Errorf("Data.Field2 mismatch: got %d, want %d", retrieved.Data.Field2, original.Data.Field2)
	}
}

func TestBucket_WithGobCodec(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucketWithCodec[testPayload](provider, GobCodec{})
	ctx := context.Background()

	original := &Object[testPayload]{
		Key:  "gob-obj",
		Data: testPayload{Field1: "gob", Field2: 777},
	}

	if err := bucket.Put(ctx, original); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, err := bucket.Get(ctx, "gob-obj")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Data.Field1 != original.Data.Field1 || retrieved.Data.Field2 != original.Data.Field2 {
		t.Errorf("roundtrip mismatch: got %+v, want %+v", retrieved.Data, original.Data)
	}
}

func TestBucket_Stat(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	mod := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	provider.data["obj1"] = []byte(`{}`)
	provider.info["obj1"] = &ObjectInfo{Key: "obj1", Size: 2, LastModified: mod}

	t.Run("existing key", func(t *testing.T) {
		info, err := bucket.Stat(ctx, "obj1")
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if info.Key != "obj1" || info.Size != 2 {
			t.Errorf("unexpected info: %+v", info)
		}
		if !info.LastModified.Equal(mod) {
			t.Errorf("unexpected LastModified: %v", info.LastModified)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := bucket.Stat(ctx, "missing")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestBucket_GetLastModified(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	mod := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	provider.data["obj1"] = []byte(`{"field1":"a","field2":1}`)
	provider.info["obj1"] = &ObjectInfo{Key: "obj1", LastModified: mod}

	obj, err := bucket.Get(ctx, "obj1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !obj.LastModified.Equal(mod) {
		t.Errorf("expected LastModified %v, got %v", mod, obj.LastModified)
	}
}

func TestBucket_ListPage(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	for _, k := range []string{"p/a", "p/b", "p/c"} {
		provider.data[k] = []byte(`{}`)
		provider.info[k] = &ObjectInfo{Key: k}
	}

	// limit 1 over 3 keys yields all 3 across 3 calls, then an empty cursor.
	var got []string
	cursor := ""
	calls := 0
	for {
		infos, next, err := bucket.ListPage(ctx, "p/", cursor, 1)
		if err != nil {
			t.Fatalf("ListPage failed: %v", err)
		}
		calls++
		for _, info := range infos {
			got = append(got, info.Key)
		}
		if next == "" {
			break
		}
		cursor = next
		if calls > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(got) != 3 {
		t.Errorf("expected 3 keys, got %d (%v)", len(got), got)
	}
}

func TestBucket_ListLevel(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	for _, k := range []string{"a/x", "a/b/y", "a/b/z"} {
		provider.data[k] = []byte(`{}`)
		provider.info[k] = &ObjectInfo{Key: k}
	}

	level, err := bucket.ListLevel(ctx, "a/", "/", "", 0)
	if err != nil {
		t.Fatalf("ListLevel failed: %v", err)
	}
	if len(level.Objects) != 1 || level.Objects[0].Key != "a/x" {
		t.Errorf("expected objects [a/x], got %+v", level.Objects)
	}
	if len(level.Prefixes) != 1 || level.Prefixes[0] != "a/b/" {
		t.Errorf("expected prefixes [a/b/], got %v", level.Prefixes)
	}
}

func TestBucket_Stream(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	payload := []byte("raw stream bytes")
	if err := bucket.PutStream(ctx, "stream-key", bytes.NewReader(payload), &ObjectInfo{Key: "stream-key"}); err != nil {
		t.Fatalf("PutStream failed: %v", err)
	}

	r, info, err := bucket.GetStream(ctx, "stream-key")
	if err != nil {
		t.Fatalf("GetStream failed: %v", err)
	}
	defer func() { _ = r.Close() }()
	if info.Key != "stream-key" {
		t.Errorf("unexpected key: %q", info.Key)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("stream roundtrip mismatch: got %q, want %q", got, payload)
	}

	t.Run("missing key", func(t *testing.T) {
		_, _, err := bucket.GetStream(ctx, "nope")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestFoldLevel(t *testing.T) {
	info := func(keys ...string) []ObjectInfo {
		out := make([]ObjectInfo, len(keys))
		for i, k := range keys {
			out[i] = ObjectInfo{Key: k}
		}
		return out
	}
	keysOf := func(infos []ObjectInfo) []string {
		out := make([]string, len(infos))
		for i, in := range infos {
			out[i] = in.Key
		}
		return out
	}
	eq := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	t.Run("acceptance", func(t *testing.T) {
		level := FoldLevel("a/", "/", info("a/x", "a/b/y", "a/b/z"))
		if !eq(keysOf(level.Objects), []string{"a/x"}) {
			t.Errorf("objects = %v, want [a/x]", keysOf(level.Objects))
		}
		if !eq(level.Prefixes, []string{"a/b/"}) {
			t.Errorf("prefixes = %v, want [a/b/]", level.Prefixes)
		}
	})

	t.Run("prefix without trailing delimiter", func(t *testing.T) {
		level := FoldLevel("a", "/", info("a/x", "a/b/y"))
		if len(level.Objects) != 0 {
			t.Errorf("objects = %v, want []", keysOf(level.Objects))
		}
		if !eq(level.Prefixes, []string{"a/"}) {
			t.Errorf("prefixes = %v, want [a/]", level.Prefixes)
		}
	})

	t.Run("empty delimiter", func(t *testing.T) {
		level := FoldLevel("a/", "", info("a/x", "a/b/y", "a/b/z"))
		if !eq(keysOf(level.Objects), []string{"a/x", "a/b/y", "a/b/z"}) {
			t.Errorf("objects = %v, want all three", keysOf(level.Objects))
		}
		if len(level.Prefixes) != 0 {
			t.Errorf("prefixes = %v, want []", level.Prefixes)
		}
	})

	t.Run("key equal to prefix", func(t *testing.T) {
		level := FoldLevel("a/", "/", info("a/", "a/x"))
		if !eq(keysOf(level.Objects), []string{"a/", "a/x"}) {
			t.Errorf("objects = %v, want [a/ a/x]", keysOf(level.Objects))
		}
		if len(level.Prefixes) != 0 {
			t.Errorf("prefixes = %v, want []", level.Prefixes)
		}
	})

	t.Run("deep keys collapse into one prefix", func(t *testing.T) {
		level := FoldLevel("a/", "/", info("a/b/c/d", "a/b/e/f", "a/b/g"))
		if len(level.Objects) != 0 {
			t.Errorf("objects = %v, want []", keysOf(level.Objects))
		}
		if !eq(level.Prefixes, []string{"a/b/"}) {
			t.Errorf("prefixes = %v, want [a/b/]", level.Prefixes)
		}
	})
}

func TestBucket_Atomic(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testPayload](provider)
	ctx := context.Background()

	provider.data["atomic-obj"] = []byte(`{"field1":"atomic","field2":123}`)
	provider.info["atomic-obj"] = &ObjectInfo{
		Key:         "atomic-obj",
		ContentType: "application/json",
	}

	atomic := bucket.Atomic()
	if atomic == nil {
		t.Fatal("Atomic returned nil")
	}

	// Verify it returns the same instance
	atomic2 := bucket.Atomic()
	if atomic != atomic2 {
		t.Error("Atomic should return cached instance")
	}

	// Test that atomic view works
	obj, err := atomic.Get(ctx, "atomic-obj")
	if err != nil {
		t.Fatalf("Atomic Get failed: %v", err)
	}
	if obj.Key != "atomic-obj" {
		t.Errorf("unexpected Key: %q", obj.Key)
	}
	// atom uses struct field names as keys
	if obj.Data.Strings["Field1"] != "atomic" {
		t.Errorf("unexpected Field1: %q", obj.Data.Strings["Field1"])
	}
	if obj.Data.Ints["Field2"] != 123 {
		t.Errorf("unexpected Field2: %v", obj.Data.Ints["Field2"])
	}
}
