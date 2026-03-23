package pinecone

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pinecone-io/go-pinecone/v2/pinecone"
	"github.com/testcontainers/testcontainers-go"
	tcpinecone "github.com/testcontainers/testcontainers-go/modules/pinecone"
	"github.com/testcontainers/testcontainers-go/wait"
	grubpinecone "github.com/zoobz-io/grub/pinecone"
	"github.com/zoobz-io/grub/testing/integration/vector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var tc *vector.TestContext

const indexName = "test-vectors"

func TestMain(m *testing.M) {
	ctx := context.Background()

	pineconeContainer, err := tcpinecone.Run(ctx,
		"ghcr.io/pinecone-io/pinecone-local:v0.7.0",
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/indexes").
				WithPort("5080/tcp").
				WithStartupTimeout(120*time.Second),
		),
		// Expose gRPC port for index connections
		testcontainers.WithExposedPorts("5081/tcp"),
	)
	if err != nil {
		panic("failed to start pinecone container: " + err.Error())
	}

	httpEndpoint, err := pineconeContainer.HttpEndpoint()
	if err != nil {
		panic("failed to get http endpoint: " + err.Error())
	}

	// Get the mapped gRPC port
	grpcHost, err := pineconeContainer.Host(ctx)
	if err != nil {
		panic("failed to get container host: " + err.Error())
	}
	grpcPort, err := pineconeContainer.MappedPort(ctx, "5081/tcp")
	if err != nil {
		panic("failed to get gRPC port: " + err.Error())
	}
	// Use http:// prefix for insecure connection (v2 SDK handles this correctly)
	grpcEndpoint := fmt.Sprintf("http://%s:%s", grpcHost, grpcPort.Port())

	// Create Pinecone client
	client, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: "test-api-key", // Any value works for local emulator
		Host:   httpEndpoint,
	})
	if err != nil {
		panic("failed to create pinecone client: " + err.Error())
	}

	// Create serverless index
	_, err = client.CreateServerlessIndex(ctx, &pinecone.CreateServerlessIndexRequest{
		Name:      indexName,
		Dimension: 3, // 3-dimensional vectors for tests
		Metric:    pinecone.Euclidean,
		Cloud:     pinecone.Aws,
		Region:    "us-east-1",
	})
	if err != nil {
		panic("failed to create index: " + err.Error())
	}

	// Connect to the index using the mapped gRPC endpoint with insecure credentials
	indexConn, err := client.Index(pinecone.NewIndexConnParams{
		Host: grpcEndpoint,
	}, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("failed to connect to index: " + err.Error())
	}

	provider := grubpinecone.New(indexConn, grubpinecone.Config{})

	tc = &vector.TestContext{
		Provider: provider,
		Cleanup: func() {
			_ = indexConn.Close()
			_ = client.DeleteIndex(ctx, indexName)
			_ = pineconeContainer.Terminate(ctx)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func TestPinecone_CRUD(t *testing.T) {
	vector.RunCRUDTests(t, tc)
}

func TestPinecone_Search(t *testing.T) {
	vector.RunSearchTests(t, tc)
}

func TestPinecone_Batch(t *testing.T) {
	vector.RunBatchTests(t, tc)
}

func TestPinecone_Atomic(t *testing.T) {
	vector.RunAtomicTests(t, tc)
}

func TestPinecone_Query(t *testing.T) {
	// Pinecone doesn't support range, like, or contains operators
	vector.RunQueryTests(t, tc, vector.QueryOperators{
		Range:    false,
		Like:     false,
		Contains: false,
	})
}

func TestPinecone_Filter(t *testing.T) {
	// Pinecone doesn't support metadata-only filtering
	vector.RunFilterTests(t, tc, false)
}

func TestPinecone_Hooks(t *testing.T) {
	vector.RunHookTests(t, tc)
}
