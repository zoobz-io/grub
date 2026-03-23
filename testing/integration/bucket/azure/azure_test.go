package azure

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	grubazure "github.com/zoobz-io/grub/azure"
	"github.com/zoobz-io/grub/testing/integration/bucket"
)

var tc *bucket.TestContext

const (
	testContainer = "test-container"
	// Azurite default credentials.
	accountName = "devstoreaccount1"
	accountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start Azurite container
	req := testcontainers.ContainerRequest{
		Image:        "mcr.microsoft.com/azure-storage/azurite:latest",
		ExposedPorts: []string{"10000/tcp"},
		Cmd:          []string{"azurite-blob", "--blobHost", "0.0.0.0"},
		WaitingFor: wait.ForLog("Azurite Blob service successfully listens").
			WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic("failed to start azurite container: " + err.Error())
	}

	// Get the endpoint
	host, err := container.Host(ctx)
	if err != nil {
		panic("failed to get azurite host: " + err.Error())
	}

	port, err := container.MappedPort(ctx, "10000/tcp")
	if err != nil {
		panic("failed to get azurite port: " + err.Error())
	}

	// Azurite connection string format
	connStr := fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://%s:%s/%s;",
		accountName, accountKey, host, port.Port(), accountName,
	)

	// Create Azure Blob client
	client, err := azblob.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		panic("failed to create Azure Blob client: " + err.Error())
	}

	// Create test container
	_, err = client.CreateContainer(ctx, testContainer, nil)
	if err != nil {
		panic("failed to create test container: " + err.Error())
	}

	tc = &bucket.TestContext{
		Provider: grubazure.New(client, testContainer),
		Cleanup: func() {
			// Clean up container contents before terminating
			pager := client.NewListBlobsFlatPager(testContainer, nil)
			for pager.More() {
				page, err := pager.NextPage(ctx)
				if err != nil {
					break
				}
				for _, blob := range page.Segment.BlobItems {
					_, _ = client.DeleteBlob(ctx, testContainer, *blob.Name, nil)
				}
			}
			_, _ = client.DeleteContainer(ctx, testContainer, nil)
			_ = container.Terminate(ctx)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func TestAzure_CRUD(t *testing.T) {
	bucket.RunCRUDTests(t, tc)
}

func TestAzure_Metadata(t *testing.T) {
	// ContentType works, but custom metadata has known issues with Azurite emulator
	// See: https://github.com/Azure/Azurite/issues/591
	t.Run("ContentType", func(t *testing.T) {
		bucket.RunContentTypeTest(t, tc)
	})
	t.Run("CustomMetadata", func(t *testing.T) {
		t.Skip("Azurite emulator has known issues with custom metadata")
	})
}

func TestAzure_Atomic(t *testing.T) {
	bucket.RunAtomicTests(t, tc)
}

func TestAzure_List(t *testing.T) {
	bucket.RunListTests(t, tc)
}

func TestAzure_Hooks(t *testing.T) {
	bucket.RunHookTests(t, tc)
}
