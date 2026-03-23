package minio

import (
	"context"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	minioc "github.com/testcontainers/testcontainers-go/modules/minio"
	grubminio "github.com/zoobz-io/grub/minio"
	"github.com/zoobz-io/grub/testing/integration/bucket"
)

var tc *bucket.TestContext

const testBucket = "test-bucket"

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := minioc.Run(ctx, "minio/minio:latest")
	if err != nil {
		panic("failed to start minio container: " + err.Error())
	}

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		panic("failed to get minio endpoint: " + err.Error())
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(container.Username, container.Password, ""),
		Secure: false,
	})
	if err != nil {
		panic("failed to create minio client: " + err.Error())
	}

	err = client.MakeBucket(ctx, testBucket, minio.MakeBucketOptions{})
	if err != nil {
		panic("failed to create test bucket: " + err.Error())
	}

	tc = &bucket.TestContext{
		Provider: grubminio.New(client, testBucket),
		Cleanup: func() {
			for obj := range client.ListObjects(ctx, testBucket, minio.ListObjectsOptions{Recursive: true}) {
				if obj.Err == nil {
					_ = client.RemoveObject(ctx, testBucket, obj.Key, minio.RemoveObjectOptions{})
				}
			}
			_ = client.RemoveBucket(ctx, testBucket)
			_ = container.Terminate(ctx)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func TestMinio_CRUD(t *testing.T) {
	bucket.RunCRUDTests(t, tc)
}

func TestMinio_Metadata(t *testing.T) {
	bucket.RunMetadataTests(t, tc)
}

func TestMinio_Atomic(t *testing.T) {
	bucket.RunAtomicTests(t, tc)
}

func TestMinio_List(t *testing.T) {
	bucket.RunListTests(t, tc)
}

func TestMinio_Hooks(t *testing.T) {
	bucket.RunHookTests(t, tc)
}
