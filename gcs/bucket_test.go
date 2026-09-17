package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zoobz-io/grub"
	"google.golang.org/api/option"
)

var testProvider *Provider
var testStorageClient *storage.Client

const testBucket = "test-bucket"

func TestMain(m *testing.M) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "fsouza/fake-gcs-server:latest",
		ExposedPorts: []string{"4443/tcp"},
		Cmd:          []string{"-scheme", "http", "-public-host", "localhost"},
		WaitingFor:   wait.ForListeningPort("4443/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start fake-gcs-server container: %v\n", err)
		os.Exit(1)
	}

	endpoint, err := container.PortEndpoint(ctx, "4443/tcp", "http")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get endpoint: %v\n", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}

	testStorageClient, err = storage.NewClient(ctx,
		option.WithEndpoint(endpoint+"/storage/v1/"),
		option.WithoutAuthentication(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create storage client: %v\n", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}

	// Create test bucket
	if err := testStorageClient.Bucket(testBucket).Create(ctx, "test-project", nil); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create bucket: %v\n", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}

	testProvider = New(testStorageClient, testBucket)

	code := m.Run()

	_ = testStorageClient.Close()
	_ = container.Terminate(ctx)

	os.Exit(code)
}

func clearBucket(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	it := testStorageClient.Bucket(testBucket).Objects(ctx, nil)
	for {
		attrs, err := it.Next()
		if err != nil {
			break
		}
		_ = testStorageClient.Bucket(testBucket).Object(attrs.Name).Delete(ctx)
	}
}

func TestNew(t *testing.T) {
	if testProvider == nil {
		t.Fatal("New returned nil")
	}
	if testProvider.client == nil {
		t.Error("client not set correctly")
	}
	if testProvider.bucket != testBucket {
		t.Error("bucket not set correctly")
	}
}

func TestProvider_Get(t *testing.T) {
	clearBucket(t)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		obj := testStorageClient.Bucket(testBucket).Object("key1")
		w := obj.NewWriter(ctx)
		_, _ = w.Write([]byte("test content"))
		_ = w.Close()

		result, info, err := testProvider.Get(ctx, "key1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if string(result) != "test content" {
			t.Errorf("unexpected value: %q", string(result))
		}
		if info.Key != "key1" {
			t.Errorf("unexpected key: %q", info.Key)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, _, err := testProvider.Get(ctx, "nonexistent")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestProvider_Put(t *testing.T) {
	clearBucket(t)
	ctx := context.Background()

	t.Run("basic put", func(t *testing.T) {
		data := []byte("new content")
		info := &grub.ObjectInfo{
			Key:         "put-key",
			ContentType: "text/plain",
			Metadata:    map[string]string{"foo": "bar"},
		}

		err := testProvider.Put(ctx, "put-key", data, info)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Verify
		result, _, err := testProvider.Get(ctx, "put-key")
		if err != nil {
			t.Fatalf("verification Get failed: %v", err)
		}
		if string(result) != "new content" {
			t.Errorf("unexpected value: %q", string(result))
		}
	})

	t.Run("put with nil info", func(t *testing.T) {
		err := testProvider.Put(ctx, "nil-info", []byte("data"), nil)
		if err != nil {
			t.Fatalf("Put with nil info failed: %v", err)
		}
	})

	t.Run("overwrite existing", func(t *testing.T) {
		_ = testProvider.Put(ctx, "overwrite", []byte("v1"), nil)
		err := testProvider.Put(ctx, "overwrite", []byte("v2"), nil)
		if err != nil {
			t.Fatalf("overwrite Put failed: %v", err)
		}

		result, _, _ := testProvider.Get(ctx, "overwrite")
		if string(result) != "v2" {
			t.Errorf("expected 'v2', got %q", string(result))
		}
	})
}

func TestProvider_Delete(t *testing.T) {
	clearBucket(t)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		_ = testProvider.Put(ctx, "delete-me", []byte("data"), nil)

		err := testProvider.Delete(ctx, "delete-me")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, _, err = testProvider.Get(ctx, "delete-me")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		err := testProvider.Delete(ctx, "nonexistent")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestProvider_Exists(t *testing.T) {
	clearBucket(t)
	ctx := context.Background()

	_ = testProvider.Put(ctx, "exists", []byte("data"), nil)

	t.Run("existing key", func(t *testing.T) {
		exists, err := testProvider.Exists(ctx, "exists")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("expected key to exist")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		exists, err := testProvider.Exists(ctx, "missing")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected key to not exist")
		}
	})
}

func TestProvider_List(t *testing.T) {
	clearBucket(t)
	ctx := context.Background()

	// Setup test data
	_ = testProvider.Put(ctx, "prefix/a", []byte("a"), nil)
	_ = testProvider.Put(ctx, "prefix/b", []byte("b"), nil)
	_ = testProvider.Put(ctx, "prefix/c", []byte("c"), nil)
	_ = testProvider.Put(ctx, "other/x", []byte("x"), nil)

	t.Run("with prefix", func(t *testing.T) {
		infos, err := testProvider.List(ctx, "prefix/", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(infos) != 3 {
			t.Errorf("expected 3 infos, got %d", len(infos))
		}
	})

	t.Run("with limit", func(t *testing.T) {
		infos, err := testProvider.List(ctx, "prefix/", 2)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(infos) != 2 {
			t.Errorf("expected 2 infos, got %d", len(infos))
		}
	})

	t.Run("empty prefix", func(t *testing.T) {
		infos, err := testProvider.List(ctx, "", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(infos) != 4 {
			t.Errorf("expected 4 infos, got %d", len(infos))
		}
	})

	t.Run("no matches", func(t *testing.T) {
		infos, err := testProvider.List(ctx, "nonexistent/", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(infos) != 0 {
			t.Errorf("expected 0 infos, got %d", len(infos))
		}
	})
}

func TestProvider_Stat(t *testing.T) {
	clearBucket(t)
	ctx := context.Background()

	_ = testProvider.Put(ctx, "stat-key", []byte("stat data"), &grub.ObjectInfo{Key: "stat-key", ContentType: "text/plain"})

	t.Run("existing key", func(t *testing.T) {
		info, err := testProvider.Stat(ctx, "stat-key")
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if info.Size != int64(len("stat data")) {
			t.Errorf("unexpected size: %d", info.Size)
		}
		if info.LastModified.IsZero() {
			t.Error("expected non-zero LastModified")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := testProvider.Stat(ctx, "nonexistent")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestProvider_Stream(t *testing.T) {
	clearBucket(t)
	ctx := context.Background()

	payload := []byte("streaming payload of unknown length")

	t.Run("put unknown length and get", func(t *testing.T) {
		err := testProvider.PutStream(ctx, "stream-key", bytes.NewReader(payload), &grub.ObjectInfo{Key: "stream-key"})
		if err != nil {
			t.Fatalf("PutStream failed: %v", err)
		}

		r, info, err := testProvider.GetStream(ctx, "stream-key")
		if err != nil {
			t.Fatalf("GetStream failed: %v", err)
		}
		defer func() { _ = r.Close() }()
		if info.LastModified.IsZero() {
			t.Error("expected non-zero LastModified")
		}
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("stream mismatch: got %q", got)
		}

		direct, _, err := testProvider.Get(ctx, "stream-key")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !bytes.Equal(direct, payload) {
			t.Errorf("Get mismatch: got %q", direct)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, _, err := testProvider.GetStream(ctx, "nope")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestProvider_ListPage(t *testing.T) {
	clearBucket(t)
	ctx := context.Background()

	for _, k := range []string{"page/a", "page/b", "page/c"} {
		_ = testProvider.Put(ctx, k, []byte("x"), nil)
	}

	var got []string
	cursor := ""
	calls := 0
	for {
		infos, next, err := testProvider.ListPage(ctx, "page/", cursor, 1)
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
		t.Errorf("expected 3 keys across pages, got %d (%v)", len(got), got)
	}
}

func TestProvider_ListLevel(t *testing.T) {
	clearBucket(t)
	ctx := context.Background()

	for _, k := range []string{"a/x", "a/b/y", "a/b/z"} {
		_ = testProvider.Put(ctx, k, []byte("x"), nil)
	}

	level, err := testProvider.ListLevel(ctx, "a/", "/", "", 0)
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

func TestProvider_RoundTrip(t *testing.T) {
	clearBucket(t)
	ctx := context.Background()

	original := []byte("hello world")
	info := &grub.ObjectInfo{
		Key:         "roundtrip",
		ContentType: "application/octet-stream",
		Metadata:    map[string]string{"test": "value"},
	}

	if err := testProvider.Put(ctx, "roundtrip", original, info); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, retrievedInfo, err := testProvider.Get(ctx, "roundtrip")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(original) {
		t.Errorf("data mismatch: got %q, want %q", string(retrieved), string(original))
	}
	if retrievedInfo.Key != "roundtrip" {
		t.Errorf("key mismatch: got %q, want %q", retrievedInfo.Key, "roundtrip")
	}
}
