# Testing

This directory contains test infrastructure for grub.

## Structure

```
testing/
├── helpers.go           # Shared test utilities
├── helpers_test.go      # Tests for helpers
├── benchmarks/          # Performance benchmarks
│   └── store_test.go
└── integration/         # Integration tests by storage type
    ├── kv/              # Key-value store tests
    ├── bucket/          # Blob storage tests
    └── database/        # SQL database tests
```

## Running Tests

```bash
# Run all tests
make test

# Run unit tests only (short mode)
make test-unit

# Run integration tests
make test-integration

# Run benchmarks
make test-bench
```

## Integration Tests

Integration tests use testcontainers to spin up real instances of backing services. Each provider has its own test file that leverages shared test suites from the parent package.

### Key-Value Stores

- `kv/redis/` — Redis via testcontainers
- `kv/badger/` — Badger (embedded, no container)
- `kv/bolt/` — BoltDB (embedded, no container)

### Blob Storage

- `bucket/s3/` — S3 via LocalStack
- `bucket/gcs/` — GCS via fake-gcs-server
- `bucket/azure/` — Azure Blob via Azurite

### Databases

- `database/postgres/` — PostgreSQL via testcontainers
- `database/mariadb/` — MariaDB via testcontainers
- `database/mssql/` — SQL Server via testcontainers
- `database/sqlite/` — SQLite (embedded, no container)

## Writing Tests

### Using Helpers

```go
import grubtesting "github.com/zoobz-io/grub/testing"

func TestSomething(t *testing.T) {
    ctx := grubtesting.WithTimeout(t, 5*time.Second)

    result, err := doSomething(ctx)
    grubtesting.AssertNoError(t, err)
    grubtesting.AssertEqual(t, result.Name, "expected")
}
```

### Helper Conventions

The helpers in `helpers.go` follow these conventions:

- All helpers call `t.Helper()` for clean stack traces
- All helpers accept `*testing.T` as the first parameter
- Helpers are **generic assertions** rather than domain-specific utilities

**Rationale:** Go's standard `testing` package lacks assertion helpers, leading to repetitive `if err != nil { t.Fatal(err) }` patterns throughout tests. These generic assertion functions (`AssertNoError`, `AssertEqual`, `AssertContains`, etc.) reduce boilerplate without introducing external dependencies like testify.

Domain-specific test logic lives in the shared test suites (`integration/kv/shared.go`, `integration/bucket/shared.go`, `integration/database/shared.go`) rather than in individual helper functions.

### Using Shared Test Suites

Integration tests use shared suites from `integration/kv/` and `integration/bucket/`:

```go
import "github.com/zoobz-io/grub/testing/integration/kv"

func TestRedis(t *testing.T) {
    // Setup provider...

    tc := &kv.TestContext{Provider: provider}
    kv.RunCRUDTests(t, tc)
    kv.RunTTLTests(t, tc)
    kv.RunBatchTests(t, tc)
}
```
