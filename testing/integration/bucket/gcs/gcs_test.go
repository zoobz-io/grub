package gcs

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	grubgcs "github.com/zoobz-io/grub/gcs"
	"github.com/zoobz-io/grub/testing/integration/bucket"
	"google.golang.org/api/option"
)

var tc *bucket.TestContext

const testBucket = "test-bucket"

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start fake-gcs-server container
	req := testcontainers.ContainerRequest{
		Image:        "fsouza/fake-gcs-server:latest",
		ExposedPorts: []string{"4443/tcp"},
		Cmd:          []string{"-scheme", "http", "-public-host", "localhost"},
		WaitingFor: wait.ForHTTP("/storage/v1/b").
			WithPort("4443/tcp").
			WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic("failed to start fake-gcs-server container: " + err.Error())
	}

	// Get the endpoint
	endpoint, err := container.PortEndpoint(ctx, "4443/tcp", "http")
	if err != nil {
		panic("failed to get fake-gcs-server endpoint: " + err.Error())
	}

	// Create GCS client configured for fake-gcs-server
	client, err := storage.NewClient(ctx,
		option.WithEndpoint(endpoint+"/storage/v1/"),
		option.WithoutAuthentication(),
	)
	if err != nil {
		panic("failed to create GCS client: " + err.Error())
	}

	// Create test bucket
	err = client.Bucket(testBucket).Create(ctx, "test-project", nil)
	if err != nil {
		panic("failed to create test bucket: " + err.Error())
	}

	tc = &bucket.TestContext{
		Provider: grubgcs.New(client, testBucket),
		Cleanup: func() {
			// Clean up bucket contents before terminating
			it := client.Bucket(testBucket).Objects(ctx, nil)
			for {
				attrs, err := it.Next()
				if err != nil {
					break
				}
				_ = client.Bucket(testBucket).Object(attrs.Name).Delete(ctx)
			}
			_ = client.Bucket(testBucket).Delete(ctx)
			_ = client.Close()
			_ = container.Terminate(ctx)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func TestGCS_CRUD(t *testing.T) {
	bucket.RunCRUDTests(t, tc)
}

func TestGCS_Metadata(t *testing.T) {
	bucket.RunMetadataTests(t, tc)
}

func TestGCS_Atomic(t *testing.T) {
	bucket.RunAtomicTests(t, tc)
}

func TestGCS_List(t *testing.T) {
	bucket.RunListTests(t, tc)
}

func TestGCS_Hooks(t *testing.T) {
	bucket.RunHookTests(t, tc)
}
