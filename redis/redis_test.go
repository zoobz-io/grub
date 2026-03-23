package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zoobz-io/grub"
)

var testProvider *Provider
var testClient *redis.Client

func TestMain(m *testing.M) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start redis container: %v\n", err)
		os.Exit(1)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "6379")

	testClient = redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", host, port.Port()),
	})
	testProvider = New(testClient)

	code := m.Run()

	_ = testClient.Close()
	_ = container.Terminate(ctx)

	os.Exit(code)
}

func clearRedis(t *testing.T) {
	t.Helper()
	if err := testClient.FlushAll(context.Background()).Err(); err != nil {
		t.Fatalf("failed to flush redis: %v", err)
	}
}

func TestNew(t *testing.T) {
	if testProvider == nil {
		t.Fatal("New returned nil")
	}
	if testProvider.client == nil {
		t.Error("client not set correctly")
	}
}

func TestProvider_Get(t *testing.T) {
	clearRedis(t)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		_ = testClient.Set(ctx, "key1", "value1", 0).Err()

		data, err := testProvider.Get(ctx, "key1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if string(data) != "value1" {
			t.Errorf("unexpected value: %q", string(data))
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := testProvider.Get(ctx, "nonexistent")
		if !errors.Is(err, grub.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestProvider_Set(t *testing.T) {
	clearRedis(t)
	ctx := context.Background()

	t.Run("basic set", func(t *testing.T) {
		err := testProvider.Set(ctx, "key1", []byte("value1"), 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		val, err := testClient.Get(ctx, "key1").Result()
		if err != nil {
			t.Fatalf("verification failed: %v", err)
		}
		if val != "value1" {
			t.Errorf("unexpected stored value: %q", val)
		}
	})

	t.Run("with ttl", func(t *testing.T) {
		err := testProvider.Set(ctx, "ttl-key", []byte("ttl-value"), time.Hour)
		if err != nil {
			t.Fatalf("Set with TTL failed: %v", err)
		}

		ttl, _ := testClient.TTL(ctx, "ttl-key").Result()
		if ttl <= 0 {
			t.Error("TTL not set")
		}
	})

	t.Run("overwrite existing", func(t *testing.T) {
		_ = testProvider.Set(ctx, "overwrite", []byte("v1"), 0)
		err := testProvider.Set(ctx, "overwrite", []byte("v2"), 0)
		if err != nil {
			t.Fatalf("overwrite Set failed: %v", err)
		}

		val, _ := testClient.Get(ctx, "overwrite").Result()
		if val != "v2" {
			t.Errorf("expected 'v2', got %q", val)
		}
	})
}

func TestProvider_Delete(t *testing.T) {
	clearRedis(t)
	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		_ = testClient.Set(ctx, "delete-me", "value", 0).Err()

		err := testProvider.Delete(ctx, "delete-me")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		exists, _ := testClient.Exists(ctx, "delete-me").Result()
		if exists > 0 {
			t.Error("key should have been deleted")
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
	clearRedis(t)
	ctx := context.Background()

	_ = testClient.Set(ctx, "exists", "value", 0).Err()

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
	clearRedis(t)
	ctx := context.Background()

	// Setup test data
	_ = testClient.Set(ctx, "prefix/a", "a", 0).Err()
	_ = testClient.Set(ctx, "prefix/b", "b", 0).Err()
	_ = testClient.Set(ctx, "prefix/c", "c", 0).Err()
	_ = testClient.Set(ctx, "other/x", "x", 0).Err()

	t.Run("with prefix", func(t *testing.T) {
		keys, err := testProvider.List(ctx, "prefix/", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 3 {
			t.Errorf("expected 3 keys, got %d: %v", len(keys), keys)
		}
	})

	t.Run("with limit", func(t *testing.T) {
		keys, err := testProvider.List(ctx, "prefix/", 2)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 2 {
			t.Errorf("expected 2 keys, got %d", len(keys))
		}
	})

	t.Run("empty prefix", func(t *testing.T) {
		keys, err := testProvider.List(ctx, "", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 4 {
			t.Errorf("expected 4 keys, got %d", len(keys))
		}
	})

	t.Run("no matches", func(t *testing.T) {
		keys, err := testProvider.List(ctx, "nonexistent/", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 0 {
			t.Errorf("expected 0 keys, got %d", len(keys))
		}
	})
}

func TestProvider_GetBatch(t *testing.T) {
	clearRedis(t)
	ctx := context.Background()

	// Setup
	_ = testClient.Set(ctx, "batch1", "v1", 0).Err()
	_ = testClient.Set(ctx, "batch2", "v2", 0).Err()

	t.Run("all exist", func(t *testing.T) {
		result, err := testProvider.GetBatch(ctx, []string{"batch1", "batch2"})
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
		result, err := testProvider.GetBatch(ctx, []string{"batch1", "missing"})
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if len(result) != 1 {
			t.Errorf("expected 1 result, got %d", len(result))
		}
	})

	t.Run("none exist", func(t *testing.T) {
		result, err := testProvider.GetBatch(ctx, []string{"x", "y", "z"})
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 results, got %d", len(result))
		}
	})

	t.Run("empty keys", func(t *testing.T) {
		result, err := testProvider.GetBatch(ctx, []string{})
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 results, got %d", len(result))
		}
	})
}

func TestProvider_SetBatch(t *testing.T) {
	clearRedis(t)
	ctx := context.Background()

	t.Run("basic batch", func(t *testing.T) {
		items := map[string][]byte{
			"sb1": []byte("v1"),
			"sb2": []byte("v2"),
		}
		err := testProvider.SetBatch(ctx, items, 0)
		if err != nil {
			t.Fatalf("SetBatch failed: %v", err)
		}

		// Verify
		v1, _ := testClient.Get(ctx, "sb1").Result()
		v2, _ := testClient.Get(ctx, "sb2").Result()
		if v1 != "v1" || v2 != "v2" {
			t.Errorf("batch values not stored correctly")
		}
	})

	t.Run("with ttl", func(t *testing.T) {
		items := map[string][]byte{
			"ttl1": []byte("v1"),
		}
		err := testProvider.SetBatch(ctx, items, time.Hour)
		if err != nil {
			t.Fatalf("SetBatch with TTL failed: %v", err)
		}

		ttl, _ := testClient.TTL(ctx, "ttl1").Result()
		if ttl <= 0 {
			t.Error("TTL not set")
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		err := testProvider.SetBatch(ctx, map[string][]byte{}, 0)
		if err != nil {
			t.Fatalf("SetBatch empty failed: %v", err)
		}
	})
}

func TestProvider_RoundTrip(t *testing.T) {
	clearRedis(t)
	ctx := context.Background()

	original := []byte("hello world")

	if err := testProvider.Set(ctx, "rt", original, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, err := testProvider.Get(ctx, "rt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(original) {
		t.Errorf("mismatch: got %q, want %q", string(retrieved), string(original))
	}
}
