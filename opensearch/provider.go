// Package opensearch provides a grub SearchProvider implementation for OpenSearch.
package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/lucene"
	osrenderer "github.com/zoobz-io/lucene/opensearch"
)

// Config holds configuration for the OpenSearch provider.
type Config struct {
	// Version is the OpenSearch version to target.
	Version osrenderer.Version

	// RefreshOnWrite forces a refresh after each write operation.
	// Useful for testing but impacts performance.
	RefreshOnWrite bool
}

// Provider implements grub.SearchProvider for OpenSearch.
type Provider struct {
	client   *opensearch.Client
	renderer *osrenderer.Renderer
	config   Config
}

// New creates an OpenSearch provider with the given client and config.
func New(client *opensearch.Client, config Config) *Provider {
	return &Provider{
		client:   client,
		renderer: osrenderer.NewRenderer(config.Version),
		config:   config,
	}
}

// Index stores a document with the given ID.
func (p *Provider) Index(ctx context.Context, index, id string, doc []byte) error {
	req := opensearchapi.IndexReq{
		Index:      index,
		DocumentID: id,
		Body:       bytes.NewReader(doc),
	}
	if p.config.RefreshOnWrite {
		req.Params.Refresh = "true"
	}

	var resp opensearchapi.IndexResp
	httpResp, err := p.client.Do(ctx, req, &resp)
	if err != nil {
		return err
	}
	if httpResp.StatusCode >= 400 {
		return fmt.Errorf("opensearch: index failed with status %d", httpResp.StatusCode)
	}
	return nil
}

// IndexBatch stores multiple documents using bulk API.
func (p *Provider) IndexBatch(ctx context.Context, index string, docs map[string][]byte) error {
	if len(docs) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for id, doc := range docs {
		// Action line
		action := map[string]any{
			"index": map[string]any{
				"_index": index,
				"_id":    id,
			},
		}
		actionBytes, err := json.Marshal(action)
		if err != nil {
			return err
		}
		buf.Write(actionBytes)
		buf.WriteByte('\n')

		// Document line
		buf.Write(doc)
		buf.WriteByte('\n')
	}

	req := opensearchapi.BulkReq{
		Body: &buf,
	}
	if p.config.RefreshOnWrite {
		req.Params.Refresh = "true"
	}

	var resp opensearchapi.BulkResp
	httpResp, err := p.client.Do(ctx, req, &resp)
	if err != nil {
		return err
	}
	if httpResp.StatusCode >= 400 {
		return fmt.Errorf("opensearch: bulk index failed with status %d", httpResp.StatusCode)
	}
	if resp.Errors {
		return fmt.Errorf("opensearch: bulk index had errors")
	}
	return nil
}

// Get retrieves a document by ID.
func (p *Provider) Get(ctx context.Context, index, id string) ([]byte, error) {
	req := opensearchapi.DocumentGetReq{
		Index:      index,
		DocumentID: id,
	}

	var resp opensearchapi.DocumentGetResp
	httpResp, err := p.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode == http.StatusNotFound {
		return nil, grub.ErrNotFound
	}
	if httpResp.StatusCode >= 400 {
		return nil, fmt.Errorf("opensearch: get failed with status %d", httpResp.StatusCode)
	}

	if !resp.Found {
		return nil, grub.ErrNotFound
	}

	return resp.Source, nil
}

// Delete removes a document by ID.
func (p *Provider) Delete(ctx context.Context, index, id string) error {
	req := opensearchapi.DocumentDeleteReq{
		Index:      index,
		DocumentID: id,
	}
	if p.config.RefreshOnWrite {
		req.Params.Refresh = "true"
	}

	var resp opensearchapi.DocumentDeleteResp
	httpResp, err := p.client.Do(ctx, req, &resp)
	if err != nil {
		return err
	}

	if httpResp.StatusCode == http.StatusNotFound {
		return grub.ErrNotFound
	}
	if httpResp.StatusCode >= 400 {
		return fmt.Errorf("opensearch: delete failed with status %d", httpResp.StatusCode)
	}

	if resp.Result == "not_found" {
		return grub.ErrNotFound
	}
	return nil
}

// DeleteBatch removes multiple documents by ID using bulk API.
func (p *Provider) DeleteBatch(ctx context.Context, index string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, id := range ids {
		action := map[string]any{
			"delete": map[string]any{
				"_index": index,
				"_id":    id,
			},
		}
		actionBytes, err := json.Marshal(action)
		if err != nil {
			return err
		}
		buf.Write(actionBytes)
		buf.WriteByte('\n')
	}

	req := opensearchapi.BulkReq{
		Body: &buf,
	}
	if p.config.RefreshOnWrite {
		req.Params.Refresh = "true"
	}

	var resp opensearchapi.BulkResp
	httpResp, err := p.client.Do(ctx, req, &resp)
	if err != nil {
		return err
	}
	if httpResp.StatusCode >= 400 {
		return fmt.Errorf("opensearch: bulk delete failed with status %d", httpResp.StatusCode)
	}
	// Silently ignore not_found errors for batch delete
	return nil
}

// Exists checks whether a document ID exists.
func (p *Provider) Exists(ctx context.Context, index, id string) (bool, error) {
	req := opensearchapi.DocumentExistsReq{
		Index:      index,
		DocumentID: id,
	}

	httpResp, err := p.client.Do(ctx, req, nil)
	if err != nil {
		return false, err
	}

	if httpResp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if httpResp.StatusCode == http.StatusOK {
		return true, nil
	}
	return false, fmt.Errorf("opensearch: exists check failed with status %d", httpResp.StatusCode)
}

// Search performs a search using the provided search request.
func (p *Provider) Search(ctx context.Context, index string, search *lucene.Search) (*grub.SearchResponse, error) {
	body, err := p.renderer.Render(search)
	if err != nil {
		return nil, fmt.Errorf("opensearch: failed to render search: %w", err)
	}

	req := opensearchapi.SearchReq{
		Indices: []string{index},
		Body:    bytes.NewReader(body),
	}

	var resp opensearchapi.SearchResp
	httpResp, err := p.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode >= 400 {
		// Try to extract error message
		if httpResp.Body != nil {
			defer httpResp.Body.Close()
			errBody, _ := io.ReadAll(httpResp.Body)
			return nil, fmt.Errorf("opensearch: search failed with status %d: %s", httpResp.StatusCode, string(errBody))
		}
		return nil, fmt.Errorf("opensearch: search failed with status %d", httpResp.StatusCode)
	}

	return p.parseSearchResponse(&resp)
}

// Count returns the number of documents matching the query.
func (p *Provider) Count(ctx context.Context, index string, query lucene.Query) (int64, error) {
	var body []byte
	var err error

	if query != nil {
		body, err = p.renderer.RenderQuery(query)
		if err != nil {
			return 0, fmt.Errorf("opensearch: failed to render query: %w", err)
		}
		// Wrap in {"query": ...}
		body = []byte(fmt.Sprintf(`{"query":%s}`, string(body)))
	}

	req := opensearchapi.IndicesCountReq{
		Indices: []string{index},
	}
	if body != nil {
		req.Body = bytes.NewReader(body)
	}

	var resp opensearchapi.IndicesCountResp
	httpResp, err := p.client.Do(ctx, req, &resp)
	if err != nil {
		return 0, err
	}

	if httpResp.StatusCode >= 400 {
		return 0, fmt.Errorf("opensearch: count failed with status %d", httpResp.StatusCode)
	}

	return int64(resp.Count), nil
}

// Refresh makes recent operations visible for search.
func (p *Provider) Refresh(ctx context.Context, index string) error {
	req := opensearchapi.IndicesRefreshReq{
		Indices: []string{index},
	}

	var resp opensearchapi.IndicesRefreshResp
	httpResp, err := p.client.Do(ctx, req, &resp)
	if err != nil {
		return err
	}

	if httpResp.StatusCode >= 400 {
		return fmt.Errorf("opensearch: refresh failed with status %d", httpResp.StatusCode)
	}
	return nil
}

// parseSearchResponse converts OpenSearch response to grub.SearchResponse.
func (p *Provider) parseSearchResponse(resp *opensearchapi.SearchResp) (*grub.SearchResponse, error) {
	result := &grub.SearchResponse{
		Hits:     make([]grub.SearchHit, 0, len(resp.Hits.Hits)),
		Total:    int64(resp.Hits.Total.Value),
		MaxScore: float64(resp.Hits.MaxScore),
	}

	for _, hit := range resp.Hits.Hits {
		grubHit := grub.SearchHit{
			ID:     hit.ID,
			Source: hit.Source,
			Score:  float64(hit.Score),
		}
		result.Hits = append(result.Hits, grubHit)
	}

	// Parse aggregations if present
	if len(resp.Aggregations) > 0 {
		result.Aggregations = make(map[string]any)
		var parsed any
		if err := json.Unmarshal(resp.Aggregations, &parsed); err != nil {
			// Use raw JSON as fallback
			result.Aggregations["_raw"] = string(resp.Aggregations)
		} else if m, ok := parsed.(map[string]any); ok {
			result.Aggregations = m
		}
	}

	return result, nil
}

// CreateIndex creates an index with optional settings and mappings.
func (p *Provider) CreateIndex(ctx context.Context, index string, settings map[string]any) error {
	var body io.Reader
	if settings != nil {
		data, err := json.Marshal(settings)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}

	req := opensearchapi.IndicesCreateReq{
		Index: index,
		Body:  body,
	}

	var resp opensearchapi.IndicesCreateResp
	httpResp, err := p.client.Do(ctx, req, &resp)
	if err != nil {
		// Check if error contains "already exists"
		if strings.Contains(err.Error(), "resource_already_exists") {
			return nil
		}
		return err
	}

	if httpResp.StatusCode >= 400 {
		// Read error body
		if httpResp.Body != nil {
			defer httpResp.Body.Close()
			errBody, _ := io.ReadAll(httpResp.Body)
			if strings.Contains(string(errBody), "resource_already_exists") {
				return nil // Idempotent create
			}
			return fmt.Errorf("opensearch: create index failed with status %d: %s", httpResp.StatusCode, string(errBody))
		}
		return fmt.Errorf("opensearch: create index failed with status %d", httpResp.StatusCode)
	}
	return nil
}

// DeleteIndex deletes an index.
func (p *Provider) DeleteIndex(ctx context.Context, index string) error {
	req := opensearchapi.IndicesDeleteReq{
		Indices: []string{index},
	}

	var resp opensearchapi.IndicesDeleteResp
	httpResp, err := p.client.Do(ctx, req, &resp)
	if err != nil {
		return err
	}

	if httpResp.StatusCode == http.StatusNotFound {
		return nil // Idempotent delete
	}
	if httpResp.StatusCode >= 400 {
		return fmt.Errorf("opensearch: delete index failed with status %d", httpResp.StatusCode)
	}
	return nil
}
