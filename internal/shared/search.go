// Package shared contains canonical type definitions shared across grub.
package shared //nolint:revive // internal shared package is intentional

// SearchHit represents a single search result from a provider.
type SearchHit struct {
	// ID is the document identifier.
	ID string

	// Source is the raw document content.
	Source []byte

	// Score is the relevance score.
	Score float64
}

// SearchResponse represents the response from a search operation.
type SearchResponse struct {
	// Hits contains the matching documents.
	Hits []SearchHit

	// Total is the total number of matching documents.
	Total int64

	// MaxScore is the maximum score across all hits.
	MaxScore float64

	// Aggregations contains aggregation results as raw JSON.
	Aggregations map[string]any

	// TypedAggs contains typed aggregation results parsed using
	// the aggregation definitions from the search request.
	// Nil when no aggregations were requested or parsed.
	TypedAggs []AggResult
}
