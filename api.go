// Package grub provides a provider-agnostic storage interface.
// Atoms serve as the type-agnostic API boundary; providers store data
// in its intended structure, not as atoms.
package grub

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/zoobz-io/atom"
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

// AtomicStore defines atom-based key-value storage operations.
// atomix.Store[T] satisfies this interface, enabling type-agnostic access
// for framework internals (field-level encryption, pipelines, etc.).
type AtomicStore interface {
	// Spec returns the atom spec describing the stored type's structure.
	Spec() atom.Spec

	// Get retrieves the value at key as an Atom.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) (*atom.Atom, error)

	// Set stores an Atom at key with optional TTL.
	// TTL of 0 means no expiration.
	Set(ctx context.Context, key string, data *atom.Atom, ttl time.Duration) error

	// Delete removes the value at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a key exists.
	Exists(ctx context.Context, key string) (bool, error)
}

// AtomicDatabase defines atom-based storage operations for a single table.
// atomix.Database[T] satisfies this interface, enabling type-agnostic access
// for framework internals (field-level encryption, pipelines, etc.).
type AtomicDatabase interface {
	// Table returns the table name this provider manages.
	Table() string

	// Spec returns the atom spec describing the table's structure.
	Spec() atom.Spec

	// Get retrieves the record at key as an Atom.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) (*atom.Atom, error)

	// Set stores an Atom at key (insert or update).
	Set(ctx context.Context, key string, data *atom.Atom) error

	// Delete removes the record at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a record exists at key.
	Exists(ctx context.Context, key string) (bool, error)

	// ExecQuery executes a query statement and returns atoms.
	ExecQuery(ctx context.Context, stmt edamame.QueryStatement, params map[string]any) ([]*atom.Atom, error)

	// ExecSelect executes a select statement and returns an atom.
	ExecSelect(ctx context.Context, stmt edamame.SelectStatement, params map[string]any) (*atom.Atom, error)
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
}

// AtomicObject holds blob metadata with an atomized payload.
// Used by AtomicBucket for type-agnostic access to blob data.
type AtomicObject = shared.AtomicObject

// AtomicBucket defines atom-based blob storage operations.
// atomix.Bucket[T] satisfies this interface, enabling type-agnostic access
// for framework internals (field-level encryption, pipelines, etc.).
type AtomicBucket interface {
	// Spec returns the atom spec describing the payload T structure.
	Spec() atom.Spec

	// Get retrieves the blob at key with atomized payload.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) (*AtomicObject, error)

	// Put stores an object with atomized payload at key.
	Put(ctx context.Context, key string, obj *AtomicObject) error

	// Delete removes the blob at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a key exists.
	Exists(ctx context.Context, key string) (bool, error)
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

// AtomicVector holds vector data with an atomized metadata payload.
// Used by AtomicIndex for type-agnostic access to vector data.
type AtomicVector = shared.AtomicVector

// AtomicIndex defines atom-based vector storage operations.
// atomix.Index[T] satisfies this interface, enabling type-agnostic access
// for framework internals (field-level encryption, pipelines, etc.).
type AtomicIndex interface {
	// Spec returns the atom spec describing the metadata type's structure.
	Spec() atom.Spec

	// Get retrieves the vector at ID with atomized metadata.
	// Returns ErrNotFound if the ID does not exist.
	Get(ctx context.Context, id uuid.UUID) (*AtomicVector, error)

	// Upsert stores a vector with atomized metadata.
	Upsert(ctx context.Context, id uuid.UUID, vector []float32, metadata *atom.Atom) error

	// Delete removes the vector at ID.
	// Returns ErrNotFound if the ID does not exist.
	Delete(ctx context.Context, id uuid.UUID) error

	// Exists checks whether an ID exists.
	Exists(ctx context.Context, id uuid.UUID) (bool, error)

	// Search performs similarity search returning atomized results.
	Search(ctx context.Context, vector []float32, k int, filter *atom.Atom) ([]AtomicVector, error)

	// Query performs similarity search with vecna filter support.
	Query(ctx context.Context, vector []float32, k int, filter *vecna.Filter) ([]AtomicVector, error)

	// Filter returns vectors matching the metadata filter without similarity search.
	Filter(ctx context.Context, filter *vecna.Filter, limit int) ([]AtomicVector, error)
}

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

// AtomicDocument holds a search document with atomized content.
// Used by AtomicSearch for type-agnostic access to document data.
type AtomicDocument = shared.AtomicDocument

// AtomicSearch defines atom-based search operations.
// atomix.Search[T] satisfies this interface, enabling type-agnostic access
// for framework internals (field-level encryption, pipelines, etc.).
type AtomicSearch interface {
	// Index returns the index name this provider manages.
	Index() string

	// Spec returns the atom spec describing the document type's structure.
	Spec() atom.Spec

	// Get retrieves the document at ID with atomized content.
	// Returns ErrNotFound if the ID does not exist.
	Get(ctx context.Context, id string) (*AtomicDocument, error)

	// Index stores a document with atomized content.
	IndexDoc(ctx context.Context, id string, doc *atom.Atom) error

	// Delete removes the document at ID.
	// Returns ErrNotFound if the ID does not exist.
	Delete(ctx context.Context, id string) error

	// Exists checks whether a document ID exists.
	Exists(ctx context.Context, id string) (bool, error)

	// Search performs a search returning atomized results.
	Search(ctx context.Context, search *lucene.Search) ([]AtomicDocument, error)
}
