package minio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zoobz-io/grub"
)

var testProvider *Provider
var testClient *minio.Client

const testBucket = "test-bucket"

func TestMain(m *testing.M) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "minio/minio:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     "minioadmin",
			"MINIO_ROOT_PASSWORD": "minioadmin",
		},
		Cmd:        []string{"server", "/data"},
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start minio container: %v\n", err)
		os.Exit(1)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "9000")

	endpoint := fmt.Sprintf("%s:%s", host, port.Port())

	testClient, err = minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create minio client: %v\n", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}

	err = testClient.MakeBucket(ctx, testBucket, minio.MakeBucketOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create bucket: %v\n", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}

	testProvider = New(testClient, testBucket)

	code := m.Run()

	_ = container.Terminate(ctx)

	os.Exit(code)
}

func clearBucket(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	for obj := range testClient.ListObjects(ctx, testBucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			t.Fatalf("failed to list objects: %v", obj.Err)
		}
		_ = testClient.RemoveObject(ctx, testBucket, obj.Key, minio.RemoveObjectOptions{})
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
		_, err := testClient.PutObject(ctx, testBucket, "key1", bytes.NewReader([]byte("test content")), 12, minio.PutObjectOptions{})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}

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
