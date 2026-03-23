# grub

[![CI](https://github.com/zoobz-io/grub/actions/workflows/ci.yml/badge.svg)](https://github.com/zoobz-io/grub/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobz-io/grub/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobz-io/grub)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobz-io/grub)](https://goreportcard.com/report/github.com/zoobz-io/grub)
[![CodeQL](https://github.com/zoobz-io/grub/actions/workflows/codeql.yml/badge.svg)](https://github.com/zoobz-io/grub/actions/workflows/codeql.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobz-io/grub.svg)](https://pkg.go.dev/github.com/zoobz-io/grub)
[![License](https://img.shields.io/github/license/zoobz-io/grub)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobz-io/grub)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobz-io/grub)](https://github.com/zoobz-io/grub/releases)

Provider-agnostic storage for Go.

Type-safe CRUD across key-value stores, blob storage, SQL databases, and vector similarity search with a unified interface.

## One Interface, Any Backend

```go
// Same code, different providers
sessions := grub.NewStore[Session](redis.New(client))
sessions := grub.NewStore[Session](badger.New(db))
sessions := grub.NewStore[Session](bolt.New(db, "sessions"))

// Type-safe operations
session, _ := sessions.Get(ctx, "session:abc")
sessions.Set(ctx, "session:xyz", &Session{UserID: "123"}, time.Hour)
```

Swap Redis for BadgerDB. Move from S3 to Azure. Switch databases from SQLite to PostgreSQL. Your business logic stays the same.

```go
// Key-value, blob, SQL, or vector — same patterns
store := grub.NewStore[Config](provider)           // key-value
bucket := grub.NewBucket[Document](provider)       // blob storage
db, _ := grub.NewDatabase[User](conn, "users", renderer)        // SQL
index := grub.NewIndex[Embedding](provider)        // vector search
```

Four storage modes, consistent API, semantic errors across all providers.

## Install

```bash
go get github.com/zoobz-io/grub
```

Requires Go 1.24+.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/zoobz-io/grub"
    "github.com/zoobz-io/grub/redis"
    goredis "github.com/redis/go-redis/v9"
)

type Session struct {
    UserID    string `json:"user_id"`
    Token     string `json:"token"`
    ExpiresAt int64  `json:"expires_at"`
}

func main() {
    ctx := context.Background()

    // Connect to Redis
    client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
    defer client.Close()

    // Create type-safe store
    sessions := grub.NewStore[Session](redis.New(client))

    // Store with TTL
    session := &Session{
        UserID:    "user:123",
        Token:     "abc123",
        ExpiresAt: time.Now().Add(24 * time.Hour).Unix(),
    }
    _ = sessions.Set(ctx, "session:abc123", session, 24*time.Hour)

    // Retrieve
    s, _ := sessions.Get(ctx, "session:abc123")
    fmt.Println(s.UserID) // user:123
}
```

## Capabilities

| Feature         | Description                                        | Docs                                           |
| --------------- | -------------------------------------------------- | ---------------------------------------------- |
| Key-Value Store | Sessions, cache, config with optional TTL          | [Providers](docs/3.guides/1.providers.md)      |
| Blob Storage    | Files and documents with metadata                  | [Lifecycle](docs/3.guides/2.lifecycle.md)      |
| SQL Database    | Structured records with query capabilities         | [Concepts](docs/2.learn/2.concepts.md)         |
| Vector Search   | Similarity search with metadata filtering          | [Providers](docs/3.guides/1.providers.md)      |
| Atomic Views    | Field-level access for encryption pipelines        | [Architecture](docs/2.learn/3.architecture.md) |
| Semantic Errors | `ErrNotFound`, `ErrDuplicate` across all providers | [API Reference](docs/5.reference/1.api.md)     |
| Custom Codecs   | JSON default, Gob available, or bring your own     | [Concepts](docs/2.learn/2.concepts.md)         |

## Why grub?

- **Type-safe** — Generics eliminate runtime type assertions
- **Swap backends** — Change providers without touching business logic
- **Consistent errors** — Same error types whether you're using Redis or S3
- **Atomic views** — Field-level access for framework internals (encryption, pipelines)
- **Isolated dependencies** — Each provider is a separate module; only pull what you use

## Storage Without Coupling

Grub enables a pattern: **define storage once, swap implementations freely**.

Your domain code works with typed stores. Infrastructure decisions — Redis vs embedded, S3 vs local filesystem, PostgreSQL vs SQLite, Pinecone vs Qdrant — become configuration, not architecture.

```go
// Domain code doesn't know or care about the backend
type SessionStore struct {
    store *grub.Store[Session]
}

func (s *SessionStore) Save(ctx context.Context, session *Session) error {
    return s.store.Set(ctx, "session:"+session.Token, session, 24*time.Hour)
}

// Production: Redis
store := grub.NewStore[Session](redis.New(redisClient))

// Development: embedded BadgerDB
store := grub.NewStore[Session](badger.New(localDB))

// Testing: in-memory
store := grub.NewStore[Session](badger.New(memDB))
```

One interface. Any backend. Zero vendor lock-in.

## Documentation

- [Overview](docs/1.overview.md) — Design philosophy and architecture

### Learn

- [Quickstart](docs/2.learn/1.quickstart.md) — Get started in minutes
- [Core Concepts](docs/2.learn/2.concepts.md) — Stores, buckets, databases, codecs
- [Architecture](docs/2.learn/3.architecture.md) — Layer model, atomic views, concurrency

### Guides

- [Providers](docs/3.guides/1.providers.md) — Setup and configuration for all backends
- [Lifecycle](docs/3.guides/2.lifecycle.md) — CRUD operations and batch processing
- [Pagination](docs/3.guides/3.pagination.md) — Listing and iterating large datasets
- [Testing](docs/3.guides/4.testing.md) — Mocks, embedded DBs, testcontainers
- [Best Practices](docs/3.guides/5.best-practices.md) — Key design, error handling, performance

### Cookbook

- [Caching](docs/4.cookbook/1.caching.md) — Cache-aside, read-through, TTL strategies
- [Migrations](docs/4.cookbook/2.migrations.md) — Switching providers without downtime
- [Multi-Tenant](docs/4.cookbook/3.multi-tenant.md) — Tenant isolation patterns

### Reference

- [API](docs/5.reference/1.api.md) — Complete API documentation
- [Providers](docs/5.reference/2.providers.md) — Provider-specific behaviors and limitations

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License — see [LICENSE](LICENSE) for details.
