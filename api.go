// Package grub provides a provider-agnostic storage interface.
package grub

import (
	"context"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/zoobz-io/edamame"
	"github.com/zoobz-io/grub/internal/shared"
	"github.com/zoobz-io/lucene"
	"github.com/zoobz-io/vecna"
)

// Semantic errors for storage operations (re-exported from internal/shared).
var (
	ErrNotFound             = shared.ErrNotFound
	ErrDuplicate            = shared.ErrDuplicate
	ErrConflict             = shared.ErrConflict
	ErrConstraint           = shared.ErrConstraint
	ErrInvalidKey           = shared.ErrInvalidKey
	ErrReadOnly             = shared.ErrReadOnly
	ErrTableExists          = shared.ErrTableExists
	ErrTableNotFound        = shared.ErrTableNotFound
	ErrTTLNotSupported      = shared.ErrTTLNotSupported
	ErrDimensionMismatch    = shared.ErrDimensionMismatch
	ErrInvalidVector        = shared.ErrInvalidVector
	ErrIndexNotReady        = shared.ErrIndexNotReady
	ErrInvalidQuery         = shared.ErrInvalidQuery
	ErrOperatorNotSupported = shared.ErrOperatorNotSupported
	ErrFilterNotSupported   = shared.ErrFilterNotSupported
	ErrNoPrimaryKey         = shared.ErrNoPrimaryKey
	ErrMultiplePrimaryKeys  = shared.ErrMultiplePrimaryKeys
)

// StoreProvider defines raw key-value storage operations.
// Implementations (redis, badger, bolt) satisfy this interface.
type StoreProvider interface {
	// Get retrieves the value at key.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores value at key with optional TTL.
	// TTL of 0 means no expiration.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Delete removes the value at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a key exists.
	Exists(ctx context.Context, key string) (bool, error)

	// List returns keys matching the given prefix.
	// Limit of 0 means no limit.
	List(ctx context.Context, prefix string, limit int) ([]string, error)

	// GetBatch retrieves multiple values by key.
	// Missing keys are omitted from the result (no error).
	GetBatch(ctx context.Context, keys []string) (map[string][]byte, error)

	// SetBatch stores multiple key-value pairs with optional TTL.
	// TTL of 0 means no expiration.
	SetBatch(ctx context.Context, items map[string][]byte, ttl time.Duration) error
}

// DatabaseProvider defines raw SQL storage operations.
// Implementations satisfy this interface to provide a mockable database backend.
// Use NewDatabaseProvider to create a provider backed by a sqlx.DB connection.
type DatabaseProvider interface {
	// Get retrieves the record at key.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores value at key (insert or update).
	Set(ctx context.Context, key string, value []byte) error

	// Delete removes the record at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a record exists at key.
	Exists(ctx context.Context, key string) (bool, error)

	// ExecQuery executes a named query and returns multiple records as raw bytes.
	ExecQuery(ctx context.Context, stmt edamame.QueryStatement, params map[string]any) ([][]byte, error)

	// ExecSelect executes a named select and returns a single record as raw bytes.
	ExecSelect(ctx context.Context, stmt edamame.SelectStatement, params map[string]any) ([]byte, error)

	// ExecUpdate executes a named update and returns the affected record as raw bytes.
	ExecUpdate(ctx context.Context, stmt edamame.UpdateStatement, params map[string]any) ([]byte, error)

	// ExecAggregate executes an aggregate and returns a scalar.
	ExecAggregate(ctx context.Context, stmt edamame.AggregateStatement, params map[string]any) (float64, error)
}

// BucketProvider defines raw blob storage operations.
// Implementations (s3, gcs, azure) satisfy this interface.
type BucketProvider interface {
	// Get retrieves the blob at key.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, *ObjectInfo, error)

	// Put stores data at key with associated metadata.
	Put(ctx context.Context, key string, data []byte, info *ObjectInfo) error

	// Delete removes the blob at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a key exists.
	Exists(ctx context.Context, key string) (bool, error)

	// List returns object info for keys matching the given prefix.
	// Limit of 0 means no limit.
	List(ctx context.Context, prefix string, limit int) ([]ObjectInfo, error)

	// Stat returns the metadata of the object at key, without transferring the data.
	// Returns ErrNotFound if the key does not exist.
	Stat(ctx context.Context, key string) (*ObjectInfo, error)

	// ListPage returns one page of object info for keys matching prefix.
	// cursor is an opaque provider-defined token; pass "" for the first page.
	// An empty next cursor means no more pages. Limit of 0 uses the provider default page size.
	ListPage(ctx context.Context, prefix, cursor string, limit int) (infos []ObjectInfo, next string, err error)

	// ListLevel returns the objects and common prefixes directly under prefix,
	// where delimiter separates the levels. cursor pages through large levels;
	// pass "" for the first page. Limit of 0 uses the provider default page size.
	ListLevel(ctx context.Context, prefix, delimiter, cursor string, limit int) (*Level, error)

	// GetStream returns a reader over the object at key, without buffering it.
	// The caller must close the reader. Returns ErrNotFound if the key does not exist.
	GetStream(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error)

	// PutStream stores data from r at key. info.Size may be set if known;
	// providers that need a length for unknown-size streams handle it internally.
	PutStream(ctx context.Context, key string, r io.Reader, info *ObjectInfo) error
}

// VectorInfo is re-exported from internal/shared for the public API.
type VectorInfo = shared.VectorInfo

// VectorRecord is re-exported from internal/shared for the public API.
type VectorRecord = shared.VectorRecord

// VectorResult is re-exported from internal/shared for the public API.
type VectorResult = shared.VectorResult

// DistanceMetric is re-exported from internal/shared for the public API.
type DistanceMetric = shared.DistanceMetric

// Distance metric constants.
const (
	DistanceL2           = shared.DistanceL2
	DistanceCosine       = shared.DistanceCosine
	DistanceInnerProduct = shared.DistanceInnerProduct
)

// VectorProvider defines raw vector storage operations.
// Implementations (pinecone, weaviate, milvus, qdrant) satisfy this interface.
type VectorProvider interface {
	// Upsert stores or updates a vector with associated metadata.
	// If the ID exists, the vector and metadata are replaced.
	Upsert(ctx context.Context, id uuid.UUID, vector []float32, metadata []byte) error

	// UpsertBatch stores or updates multiple vectors.
	UpsertBatch(ctx context.Context, vectors []VectorRecord) error

	// Get retrieves a vector by ID.
	// Returns ErrNotFound if the ID does not exist.
	Get(ctx context.Context, id uuid.UUID) ([]float32, *VectorInfo, error)

	// Delete removes a vector by ID.
	// Returns ErrNotFound if the ID does not exist.
	Delete(ctx context.Context, id uuid.UUID) error

	// DeleteBatch removes multiple vectors by ID.
	// Non-existent IDs are silently ignored.
	DeleteBatch(ctx context.Context, ids []uuid.UUID) error

	// Search performs similarity search and returns the k nearest neighbors.
	// filter is optional metadata filtering (nil means no filter).
	Search(ctx context.Context, vector []float32, k int, filter map[string]any) ([]VectorResult, error)

	// Query performs similarity search with vecna filter support.
	// Returns ErrInvalidQuery if the filter contains validation errors.
	// Returns ErrOperatorNotSupported if the provider doesn't support an operator.
	Query(ctx context.Context, vector []float32, k int, filter *vecna.Filter) ([]VectorResult, error)

	// Filter returns vectors matching the metadata filter without similarity search.
	// Result ordering is provider-dependent and not guaranteed by the interface.
	// Limit of 0 returns all matching vectors.
	// Returns ErrFilterNotSupported if the provider cannot perform metadata-only filtering.
	Filter(ctx context.Context, filter *vecna.Filter, limit int) ([]VectorResult, error)

	// List returns vector IDs.
	// Limit of 0 means no limit.
	List(ctx context.Context, limit int) ([]uuid.UUID, error)

	// Exists checks whether a vector ID exists.
	Exists(ctx context.Context, id uuid.UUID) (bool, error)
}

// AggResult is a single typed aggregation result.
type AggResult = shared.AggResult

// AggBucket is a single bucket in a bucket aggregation result.
type AggBucket = shared.AggBucket

// AggStats holds the result of a stats or extended_stats aggregation.
type AggStats = shared.AggStats

// SearchHit represents a single search result.
type SearchHit = shared.SearchHit

// SearchResponse represents the response from a search operation.
type SearchResponse = shared.SearchResponse

// SearchProvider defines raw search/document storage operations.
// Implementations (opensearch, elasticsearch) satisfy this interface.
type SearchProvider interface {
	// Index stores a document with the given ID.
	// If the ID exists, the document is replaced.
	Index(ctx context.Context, index, id string, doc []byte) error

	// IndexBatch stores multiple documents.
	IndexBatch(ctx context.Context, index string, docs map[string][]byte) error

	// Get retrieves a document by ID.
	// Returns ErrNotFound if the ID does not exist.
	Get(ctx context.Context, index, id string) ([]byte, error)

	// Delete removes a document by ID.
	// Returns ErrNotFound if the ID does not exist.
	Delete(ctx context.Context, index, id string) error

	// DeleteBatch removes multiple documents by ID.
	// Non-existent IDs are silently ignored.
	DeleteBatch(ctx context.Context, index string, ids []string) error

	// Exists checks whether a document ID exists.
	Exists(ctx context.Context, index, id string) (bool, error)

	// Search performs a search using the provided search request.
	Search(ctx context.Context, index string, search *lucene.Search) (*SearchResponse, error)

	// Count returns the number of documents matching the query.
	Count(ctx context.Context, index string, query lucene.Query) (int64, error)

	// Refresh makes recent operations visible for search.
	Refresh(ctx context.Context, index string) error
}
