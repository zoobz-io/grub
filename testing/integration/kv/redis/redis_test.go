package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	grubredis "github.com/zoobz-io/grub/redis"
	"github.com/zoobz-io/grub/testing/integration/kv"
)

var tc *kv.TestContext

func TestMain(m *testing.M) {
	ctx := context.Background()

	redisContainer, err := tcredis.Run(ctx,
		"redis:7-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		panic("failed to start redis container: " + err.Error())
	}

	connStr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		panic("failed to get connection string: " + err.Error())
	}

	opts, err := redis.ParseURL(connStr)
	if err != nil {
		panic("failed to parse redis URL: " + err.Error())
	}

	client := redis.NewClient(opts)

	tc = &kv.TestContext{
		Provider: grubredis.New(client),
		Cleanup: func() {
			_ = client.Close()
			_ = redisContainer.Terminate(ctx)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func TestRedis_CRUD(t *testing.T) {
	kv.RunCRUDTests(t, tc)
}

func TestRedis_Atomic(t *testing.T) {
	kv.RunAtomicTests(t, tc)
}

func TestRedis_TTL(t *testing.T) {
	kv.RunTTLTests(t, tc)
}

func TestRedis_Batch(t *testing.T) {
	kv.RunBatchTests(t, tc)
}

func TestRedis_Hooks(t *testing.T) {
	kv.RunHookTests(t, tc)
}
