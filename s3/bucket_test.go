package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zoobz-io/grub"
)

var testProvider *Provider
var testS3Client *s3.Client

const testBucket = "test-bucket"

func TestMain(m *testing.M) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:latest",
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES": "s3",
		},
		WaitingFor: wait.ForLog("Ready."),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start localstack container: %v\n", err)
		os.Exit(1)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "4566")

	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load aws config: %v\n", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}

	testS3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	// Create test bucket
	_, err = testS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create bucket: %v\n", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}

	testProvider = New(testS3Client, testBucket)

	code := m.Run()

	_ = container.Terminate(ctx)

	os.Exit(code)
}

func clearBucket(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	output, err := testS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(testBucket),
	})
	if err != nil {
		t.Fatalf("failed to list objects: %v", err)
	}

	for _, obj := range output.Contents {
		_, _ = testS3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(testBucket),
			Key:    obj.Key,
		})
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
		_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String("key1"),
			Body:   bytes.NewReader([]byte("test content")),
		})
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
