package qdrant

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/qdrant/go-client/qdrant"
	"github.com/testcontainers/testcontainers-go"
	tcqdrant "github.com/testcontainers/testcontainers-go/modules/qdrant"
	"github.com/testcontainers/testcontainers-go/wait"
	grubqdrant "github.com/zoobz-io/grub/qdrant"
	"github.com/zoobz-io/grub/testing/integration/vector"
)

var tc *vector.TestContext

const collectionName = "test_vectors"

func TestMain(m *testing.M) {
	ctx := context.Background()

	qdrantContainer, err := tcqdrant.Run(ctx,
		"qdrant/qdrant:v1.12.0",
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/readyz").
				WithPort("6333/tcp").
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		panic("failed to start qdrant container: " + err.Error())
	}

	grpcEndpoint, err := qdrantContainer.GRPCEndpoint(ctx)
	if err != nil {
		panic("failed to get grpc endpoint: " + err.Error())
	}

	// Parse host:port from endpoint
	parts := strings.Split(grpcEndpoint, ":")
	host := parts[0]
	port := 6334 // default
	if len(parts) > 1 {
		port, _ = strconv.Atoi(parts[1])
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host: host,
		Port: port,
	})
	if err != nil {
		panic("failed to create qdrant client: " + err.Error())
	}

	// Create test collection
	if err := setupCollection(ctx, client); err != nil {
		panic("failed to setup collection: " + err.Error())
	}

	provider := grubqdrant.New(client, grubqdrant.Config{
		Collection: collectionName,
	})

	tc = &vector.TestContext{
		Provider: provider,
		Cleanup: func() {
			_ = client.Close()
			_ = qdrantContainer.Terminate(ctx)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func setupCollection(ctx context.Context, client *qdrant.Client) error {
	// Delete if exists
	_ = client.DeleteCollection(ctx, collectionName)

	// Create collection with vector config
	return client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: collectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     3, // 3-dimensional vectors for tests
			Distance: qdrant.Distance_Euclid,
		}),
	})
}

func TestQdrant_CRUD(t *testing.T) {
	vector.RunCRUDTests(t, tc)
}

func TestQdrant_Search(t *testing.T) {
	vector.RunSearchTests(t, tc)
}

func TestQdrant_Batch(t *testing.T) {
	vector.RunBatchTests(t, tc)
}

func TestQdrant_Atomic(t *testing.T) {
	vector.RunAtomicTests(t, tc)
}

func TestQdrant_Query(t *testing.T) {
	vector.RunQueryTests(t, tc, vector.QueryOperators{
		Range:    true,
		Like:     false, // qdrant Match_Text is full-text search, not SQL LIKE pattern matching
		Contains: true,
	})
}

func TestQdrant_Filter(t *testing.T) {
	vector.RunFilterTests(t, tc, true)
}

func TestQdrant_Hooks(t *testing.T) {
	vector.RunHookTests(t, tc)
}
