// Package elasticsearch provides a grub SearchProvider implementation for Elasticsearch.
package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/lucene"
	esrenderer "github.com/zoobz-io/lucene/elasticsearch"
)

// Config holds configuration for the Elasticsearch provider.
type Config struct {
	// Version is the Elasticsearch version to target.
	Version esrenderer.Version

	// RefreshOnWrite forces a refresh after each write operation.
	// Useful for testing but impacts performance.
	RefreshOnWrite bool
}

// Provider implements grub.SearchProvider for Elasticsearch.
type Provider struct {
	client   *elasticsearch.Client
	renderer *esrenderer.Renderer
	config   Config
}

// New creates an Elasticsearch provider with the given client and config.
func New(client *elasticsearch.Client, config Config) *Provider {
	return &Provider{
		client:   client,
		renderer: esrenderer.NewRenderer(config.Version),
		config:   config,
	}
}

// Index stores a document with the given ID.
func (p *Provider) Index(ctx context.Context, index, id string, doc []byte) error {
	opts := []func(*esapi.IndexRequest){
		p.client.Index.WithContext(ctx),
		p.client.Index.WithDocumentID(id),
	}
	if p.config.RefreshOnWrite {
		opts = append(opts, p.client.Index.WithRefresh("true"))
	}

	resp, err := p.client.Index(index, bytes.NewReader(doc), opts...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return fmt.Errorf("elasticsearch: index failed with status %d", resp.StatusCode)
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

	opts := []func(*esapi.BulkRequest){
		p.client.Bulk.WithContext(ctx),
	}
	if p.config.RefreshOnWrite {
		opts = append(opts, p.client.Bulk.WithRefresh("true"))
	}

	resp, err := p.client.Bulk(&buf, opts...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return fmt.Errorf("elasticsearch: bulk index failed with status %d", resp.StatusCode)
	}

	// Parse response to check for item errors
	var bulkResp bulkResponse
	if err := json.NewDecoder(resp.Body).Decode(&bulkResp); err != nil {
		return err
	}
	if bulkResp.Errors {
		return fmt.Errorf("elasticsearch: bulk index had errors")
	}
	return nil
}

// Get retrieves a document by ID.
func (p *Provider) Get(ctx context.Context, index, id string) ([]byte, error) {
	resp, err := p.client.Get(index, id, p.client.Get.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, grub.ErrNotFound
	}
	if resp.IsError() {
		return nil, fmt.Errorf("elasticsearch: get failed with status %d", resp.StatusCode)
	}

	var getResp getResponse
	if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
		return nil, err
	}

	if !getResp.Found {
		return nil, grub.ErrNotFound
	}

	return getResp.Source, nil
}

// Delete removes a document by ID.
func (p *Provider) Delete(ctx context.Context, index, id string) error {
	opts := []func(*esapi.DeleteRequest){
		p.client.Delete.WithContext(ctx),
	}
	if p.config.RefreshOnWrite {
		opts = append(opts, p.client.Delete.WithRefresh("true"))
	}

	resp, err := p.client.Delete(index, id, opts...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return grub.ErrNotFound
	}
	if resp.IsError() {
		return fmt.Errorf("elasticsearch: delete failed with status %d", resp.StatusCode)
	}

	var delResp deleteResponse
	if err := json.NewDecoder(resp.Body).Decode(&delResp); err != nil {
		return err
	}

	if delResp.Result == "not_found" {
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

	opts := []func(*esapi.BulkRequest){
		p.client.Bulk.WithContext(ctx),
	}
	if p.config.RefreshOnWrite {
		opts = append(opts, p.client.Bulk.WithRefresh("true"))
	}

	resp, err := p.client.Bulk(&buf, opts...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return fmt.Errorf("elasticsearch: bulk delete failed with status %d", resp.StatusCode)
	}
	// Silently ignore not_found errors for batch delete
	return nil
}

// Exists checks whether a document ID exists.
func (p *Provider) Exists(ctx context.Context, index, id string) (bool, error) {
	resp, err := p.client.Exists(index, id, p.client.Exists.WithContext(ctx))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	return false, fmt.Errorf("elasticsearch: exists check failed with status %d", resp.StatusCode)
}

// Search performs a search using the provided search request.
func (p *Provider) Search(ctx context.Context, index string, search *lucene.Search) (*grub.SearchResponse, error) {
	body, err := p.renderer.Render(search)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch: failed to render search: %w", err)
	}

	resp, err := p.client.Search(
		p.client.Search.WithContext(ctx),
		p.client.Search.WithIndex(index),
		p.client.Search.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		errBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("elasticsearch: search failed with status %d: %s", resp.StatusCode, string(errBody))
	}

	return p.parseSearchResponse(resp.Body)
}

// Count returns the number of documents matching the query.
func (p *Provider) Count(ctx context.Context, index string, query lucene.Query) (int64, error) {
	opts := []func(*esapi.CountRequest){
		p.client.Count.WithContext(ctx),
		p.client.Count.WithIndex(index),
	}

	if query != nil {
		body, err := p.renderer.RenderQuery(query)
		if err != nil {
			return 0, fmt.Errorf("elasticsearch: failed to render query: %w", err)
		}
		// Wrap in {"query": ...}
		wrapped := []byte(fmt.Sprintf(`{"query":%s}`, string(body)))
		opts = append(opts, p.client.Count.WithBody(bytes.NewReader(wrapped)))
	}

	resp, err := p.client.Count(opts...)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return 0, fmt.Errorf("elasticsearch: count failed with status %d", resp.StatusCode)
	}

	var countResp countResponse
	if err := json.NewDecoder(resp.Body).Decode(&countResp); err != nil {
		return 0, err
	}

	return countResp.Count, nil
}

// Refresh makes recent operations visible for search.
func (p *Provider) Refresh(ctx context.Context, index string) error {
	resp, err := p.client.Indices.Refresh(
		p.client.Indices.Refresh.WithContext(ctx),
		p.client.Indices.Refresh.WithIndex(index),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return fmt.Errorf("elasticsearch: refresh failed with status %d", resp.StatusCode)
	}
	return nil
}

// CreateIndex creates an index with optional settings and mappings.
func (p *Provider) CreateIndex(ctx context.Context, index string, settings map[string]any) error {
	opts := []func(*esapi.IndicesCreateRequest){
		p.client.Indices.Create.WithContext(ctx),
	}

	if settings != nil {
		data, err := json.Marshal(settings)
		if err != nil {
			return err
		}
		opts = append(opts, p.client.Indices.Create.WithBody(bytes.NewReader(data)))
	}

	resp, err := p.client.Indices.Create(index, opts...)
	if err != nil {
		if strings.Contains(err.Error(), "resource_already_exists") {
			return nil
		}
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		errBody, _ := io.ReadAll(resp.Body)
		if strings.Contains(string(errBody), "resource_already_exists") {
			return nil // Idempotent create
		}
		return fmt.Errorf("elasticsearch: create index failed with status %d: %s", resp.StatusCode, string(errBody))
	}
	return nil
}

// DeleteIndex deletes an index.
func (p *Provider) DeleteIndex(ctx context.Context, index string) error {
	resp, err := p.client.Indices.Delete(
		[]string{index},
		p.client.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil // Idempotent delete
	}
	if resp.IsError() {
		return fmt.Errorf("elasticsearch: delete index failed with status %d", resp.StatusCode)
	}
	return nil
}

// parseSearchResponse converts Elasticsearch response to grub.SearchResponse.
func (p *Provider) parseSearchResponse(body io.Reader) (*grub.SearchResponse, error) {
	var resp searchResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}

	result := &grub.SearchResponse{
		Hits:     make([]grub.SearchHit, 0, len(resp.Hits.Hits)),
		Total:    resp.Hits.Total.Value,
		MaxScore: resp.Hits.MaxScore,
	}

	for _, hit := range resp.Hits.Hits {
		grubHit := grub.SearchHit{
			ID:     hit.ID,
			Source: hit.Source,
			Score:  hit.Score,
		}
		result.Hits = append(result.Hits, grubHit)
	}

	// Parse aggregations if present
	if len(resp.Aggregations) > 0 {
		result.Aggregations = make(map[string]any)
		var parsed any
		if err := json.Unmarshal(resp.Aggregations, &parsed); err != nil {
			result.Aggregations["_raw"] = string(resp.Aggregations)
		} else if m, ok := parsed.(map[string]any); ok {
			result.Aggregations = m
		}
	}

	return result, nil
}

// Response types for JSON parsing.

type bulkResponse struct {
	Errors bool `json:"errors"`
}

type getResponse struct {
	Found  bool            `json:"found"`
	Source json.RawMessage `json:"_source"`
}

type deleteResponse struct {
	Result string `json:"result"`
}

type countResponse struct {
	Count int64 `json:"count"`
}

type searchResponse struct {
	Hits struct {
		Total struct {
			Value int64 `json:"value"`
		} `json:"total"`
		MaxScore float64     `json:"max_score"`
		Hits     []searchHit `json:"hits"`
	} `json:"hits"`
	Aggregations json.RawMessage `json:"aggregations,omitempty"`
}

type searchHit struct {
	ID     string          `json:"_id"`
	Score  float64         `json:"_score"`
	Source json.RawMessage `json:"_source"`
}
