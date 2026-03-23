# Integration Tests

Integration tests verify grub providers against real backing services using testcontainers.

## Structure

```
integration/
├── kv/                  # Key-value store tests
│   ├── shared.go        # Shared test suites (CRUD, TTL, Batch)
│   ├── redis/           # Redis via testcontainers
│   ├── badger/          # Badger (embedded)
│   └── bolt/            # BoltDB (embedded)
│
├── bucket/              # Blob storage tests
│   ├── shared.go        # Shared test suites (Put, Get, Delete, List)
│   ├── s3/              # S3 via LocalStack
│   ├── gcs/             # GCS via fake-gcs-server
│   └── azure/           # Azure Blob via Azurite
│
└── database/            # SQL database tests
    ├── shared.go        # Shared test suites (CRUD, Query)
    ├── postgres/        # PostgreSQL via testcontainers
    ├── mariadb/         # MariaDB via testcontainers
    ├── mssql/           # SQL Server via testcontainers
    └── sqlite/          # SQLite (embedded)
```

## Running Tests

```bash
# Run all integration tests
make test-integration

# Run specific provider tests
go test -v ./testing/integration/kv/redis/...
go test -v ./testing/integration/bucket/s3/...
go test -v ./testing/integration/database/postgres/...
```

## Requirements

- Docker (for testcontainers-based tests)
- Sufficient memory for concurrent containers

## Shared Test Suites

Each storage type provides shared test suites that verify consistent behavior across providers.

### Key-Value (`kv/shared.go`)

```go
import "github.com/zoobz-io/grub/testing/integration/kv"

func TestMyProvider(t *testing.T) {
    provider := setupProvider()
    tc := &kv.TestContext{Provider: provider}

    kv.RunCRUDTests(t, tc)    // Get, Set, Delete, Exists
    kv.RunTTLTests(t, tc)     // TTL expiration
    kv.RunBatchTests(t, tc)   // List, GetBatch, SetBatch
    kv.RunAtomicTests(t, tc)  // Atomic view operations
}
```

### Bucket (`bucket/shared.go`)

```go
import "github.com/zoobz-io/grub/testing/integration/bucket"

func TestMyBucket(t *testing.T) {
    provider := setupProvider()
    tc := &bucket.TestContext{Provider: provider, Bucket: "test-bucket"}

    bucket.RunCRUDTests(t, tc)     // Put, Get, Delete
    bucket.RunMetadataTests(t, tc) // ContentType, custom metadata
    bucket.RunListTests(t, tc)     // List with prefix
}
```

### Database (`database/shared.go`)

```go
import "github.com/zoobz-io/grub/testing/integration/database"

func TestMyDatabase(t *testing.T) {
    db := setupDatabase()
    tc := &database.TestContext{DB: db}

    database.RunCRUDTests(t, tc)  // Get, Set, Delete
    database.RunQueryTests(t, tc) // Named queries
}
```

## Writing New Provider Tests

1. Create a directory under the appropriate category
2. Implement provider setup with testcontainers (or embedded)
3. Call the shared test suites
4. Add cleanup in `t.Cleanup()`

Example:

```go
func TestNewProvider(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }

    ctx := context.Background()

    // Start container
    container, err := startContainer(ctx)
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { container.Terminate(ctx) })

    // Create provider
    provider := newprovider.New(container.ConnectionString())

    // Run tests
    tc := &kv.TestContext{Provider: provider}
    kv.RunCRUDTests(t, tc)
}
```

## Skipping Tests

Integration tests are skipped in short mode:

```bash
go test -short ./...  # Skips integration tests
```

Individual tests check `testing.Short()` and skip accordingly.
