package weaviate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcweaviate "github.com/testcontainers/testcontainers-go/modules/weaviate"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/grpc"
	"github.com/weaviate/weaviate/entities/models"
	grubweaviate "github.com/zoobz-io/grub/weaviate"
	"github.com/zoobz-io/grub/testing/integration/vector"
)

var tc *vector.TestContext

const className = "TestVectors"

func TestMain(m *testing.M) {
	ctx := context.Background()

	weaviateContainer, err := tcweaviate.Run(ctx,
		"semitechnologies/weaviate:1.29.0",
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/v1/.well-known/ready").
				WithPort("8080/tcp").
				WithStartupTimeout(120*time.Second),
		),
	)
	if err != nil {
		panic("failed to start weaviate container: " + err.Error())
	}

	httpScheme, httpHost, err := weaviateContainer.HttpHostAddress(ctx)
	if err != nil {
		panic("failed to get http host: " + err.Error())
	}

	grpcHost, err := weaviateContainer.GrpcHostAddress(ctx)
	if err != nil {
		panic("failed to get grpc host: " + err.Error())
	}

	cfg := weaviate.Config{
		Host:   httpHost,
		Scheme: httpScheme,
		GrpcConfig: &grpc.Config{
			Host: grpcHost,
		},
	}

	client, err := weaviate.NewClient(cfg)
	if err != nil {
		panic("failed to create weaviate client: " + err.Error())
	}

	// Create test class/schema
	if err := setupSchema(ctx, client); err != nil {
		panic("failed to setup schema: " + err.Error())
	}

	provider := grubweaviate.New(client, grubweaviate.Config{
		Class:      className,
		Properties: []string{"category", "score", "tags"},
	})

	tc = &vector.TestContext{
		Provider: provider,
		Cleanup: func() {
			_ = weaviateContainer.Terminate(ctx)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func setupSchema(ctx context.Context, client *weaviate.Client) error {
	// Delete class if exists
	_ = client.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	// Create class with vector config
	classObj := &models.Class{
		Class:       className,
		Description: "Test vectors for integration tests",
		VectorIndexConfig: map[string]interface{}{
			"distance": "l2-squared",
		},
		Properties: []*models.Property{
			{
				Name:        "_grub_id",
				DataType:    []string{"text"},
				Description: "Original string ID",
			},
			{
				Name:        "category",
				DataType:    []string{"text"},
				Description: "Category of the vector",
			},
			{
				Name:        "score",
				DataType:    []string{"number"},
				Description: "Score value",
			},
			{
				Name:        "tags",
				DataType:    []string{"text[]"},
				Description: "Tags for the vector",
			},
		},
	}

	return client.Schema().ClassCreator().WithClass(classObj).Do(ctx)
}

func TestWeaviate_CRUD(t *testing.T) {
	vector.RunCRUDTests(t, tc)
}

func TestWeaviate_Search(t *testing.T) {
	vector.RunSearchTests(t, tc)
}

func TestWeaviate_Batch(t *testing.T) {
	vector.RunBatchTests(t, tc)
}

func TestWeaviate_Atomic(t *testing.T) {
	vector.RunAtomicTests(t, tc)
}

func TestWeaviate_Query(t *testing.T) {
	vector.RunQueryTests(t, tc, vector.QueryOperators{
		Range:    true,
		Like:     true,
		Contains: true,
	})
}

func TestWeaviate_Filter(t *testing.T) {
	vector.RunFilterTests(t, tc, true)
}

func TestWeaviate_Hooks(t *testing.T) {
	vector.RunHookTests(t, tc)
}
