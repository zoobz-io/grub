package milvus

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/testcontainers/testcontainers-go"
	tcmilvus "github.com/testcontainers/testcontainers-go/modules/milvus"
	"github.com/testcontainers/testcontainers-go/wait"
	grubmilvus "github.com/zoobz-io/grub/milvus"
	"github.com/zoobz-io/grub/testing/integration/vector"
)

var tc *vector.TestContext

const collectionName = "test_vectors"

func TestMain(m *testing.M) {
	ctx := context.Background()

	milvusContainer, err := tcmilvus.Run(ctx,
		"milvusdb/milvus:v2.4.0",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Milvus Proxy successfully initialized").
				WithStartupTimeout(180*time.Second),
		),
	)
	if err != nil {
		panic("failed to start milvus container: " + err.Error())
	}

	endpoint, err := milvusContainer.ConnectionString(ctx)
	if err != nil {
		panic("failed to get connection string: " + err.Error())
	}

	milvusClient, err := client.NewClient(ctx, client.Config{
		Address: endpoint,
	})
	if err != nil {
		panic("failed to create milvus client: " + err.Error())
	}

	// Create test collection
	if err := setupCollection(ctx, milvusClient); err != nil {
		panic("failed to setup collection: " + err.Error())
	}

	provider := grubmilvus.New(milvusClient, grubmilvus.Config{
		Collection:    collectionName,
		IDField:       "id",
		VectorField:   "embedding",
		MetadataField: "metadata",
	})

	tc = &vector.TestContext{
		Provider: provider,
		Cleanup: func() {
			_ = milvusClient.Close()
			_ = milvusContainer.Terminate(ctx)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func setupCollection(ctx context.Context, c client.Client) error {
	// Drop if exists
	exists, err := c.HasCollection(ctx, collectionName)
	if err != nil {
		return err
	}
	if exists {
		if err := c.DropCollection(ctx, collectionName); err != nil {
			return err
		}
	}

	// Create schema
	schema := &entity.Schema{
		CollectionName: collectionName,
		Description:    "Test vectors for integration tests",
		Fields: []*entity.Field{
			{
				Name:       "id",
				DataType:   entity.FieldTypeVarChar,
				PrimaryKey: true,
				TypeParams: map[string]string{
					"max_length": "256",
				},
			},
			{
				Name:     "embedding",
				DataType: entity.FieldTypeFloatVector,
				TypeParams: map[string]string{
					"dim": "3",
				},
			},
			{
				Name:     "metadata",
				DataType: entity.FieldTypeJSON,
			},
		},
	}

	if err := c.CreateCollection(ctx, schema, entity.DefaultShardNumber, client.WithConsistencyLevel(entity.ClStrong)); err != nil {
		return err
	}

	// Create index for vector field
	idx, err := entity.NewIndexFlat(entity.L2)
	if err != nil {
		return err
	}

	if err := c.CreateIndex(ctx, collectionName, "embedding", idx, false); err != nil {
		return err
	}

	// Load collection into memory
	return c.LoadCollection(ctx, collectionName, false)
}

func TestMilvus_CRUD(t *testing.T) {
	vector.RunCRUDTests(t, tc)
}

func TestMilvus_Search(t *testing.T) {
	vector.RunSearchTests(t, tc)
}

func TestMilvus_Batch(t *testing.T) {
	vector.RunBatchTests(t, tc)
}

func TestMilvus_Atomic(t *testing.T) {
	vector.RunAtomicTests(t, tc)
}

func TestMilvus_Query(t *testing.T) {
	vector.RunQueryTests(t, tc, vector.QueryOperators{
		Range:    true,
		Like:     true,
		Contains: true,
	})
}

func TestMilvus_Filter(t *testing.T) {
	vector.RunFilterTests(t, tc, true)
}

func TestMilvus_Hooks(t *testing.T) {
	vector.RunHookTests(t, tc)
}
