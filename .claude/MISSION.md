# Mission: grub

Database abstraction layer

## Purpose

Provide a unified interface for database, object, and search storage providers.

## What This Package Contains

- **Store[T]** — Type-safe key-value storage (Redis, BadgerDB, BoltDB)
- **Bucket[T]** — Type-safe blob storage (S3, MinIO, GCS, Azure)
- **Database[T]** — Type-safe SQL operations (PostgreSQL, MariaDB, SQL Server, SQLite)
- **Index[T]** — Type-safe vector similarity search (Pinecone, Qdrant, Weaviate, Milvus)
- **Search[T]** — Type-safe full-text search (OpenSearch, Elasticsearch)
- **Atomic views** — Field-level access for framework operations (encryption, pipelines)
- **Lifecycle hooks** — BeforeSave, AfterSave, AfterLoad, BeforeDelete, AfterDelete
- **Codec system** — JSON default, Gob built-in, custom codecs supported
- **Semantic errors** — 17 consistent error types across all providers

## What This Package Does NOT Contain

- Schema management or migrations
- Multi-tenant isolation
- Distributed transactions across storage types
- Caching layer abstraction
- Replication or failover orchestration
- Authentication or authorization
- Monitoring or metrics instrumentation
- Rate limiting or circuit breakers

## Success Criteria

1. All five storage modes fully functional with type-safe generics
2. Minimum three providers per storage mode
3. Consistent semantic errors mapped correctly by all providers
4. Atomic views working for all storage modes
5. Lifecycle hooks firing in correct order
6. Transaction support on Database[T]
7. Batch operations on all modes
8. Codec round-trip fidelity preserved

## Non-Goals

- ORM complexity beyond simple CRUD
- Schema inference or auto-migration
- Provider feature detection or capability negotiation
- Query optimization or cost estimation
- Support for legacy or obscure databases
