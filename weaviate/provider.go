// Package weaviate provides a grub VectorProvider implementation for Weaviate.
package weaviate

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/vecna"
)

// isNotFoundError checks if the error indicates a not found condition.
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "404") || strings.Contains(errStr, "not found")
}

// Config holds configuration for the Weaviate provider.
type Config struct {
	// Class is the Weaviate class name for vector storage.
	Class string

	// Properties is the list of metadata property names to retrieve in searches.
	// These must match the property names defined in your Weaviate schema.
	// If empty, no metadata properties will be returned (only ID, vector, and score).
	Properties []string
}

// Provider implements grub.VectorProvider for Weaviate.
type Provider struct {
	client *weaviate.Client
	config Config
}

// New creates a Weaviate provider with the given client and config.
func New(client *weaviate.Client, config Config) *Provider {
	return &Provider{
		client: client,
		config: config,
	}
}

// buildSearchFields constructs the GraphQL field list for vector search queries.
// Includes distance which is only valid for near* queries.
func (p *Provider) buildSearchFields() []graphql.Field {
	fields := make([]graphql.Field, 0, len(p.config.Properties)+1)

	for _, prop := range p.config.Properties {
		fields = append(fields, graphql.Field{Name: prop})
	}

	fields = append(fields, graphql.Field{
		Name: "_additional",
		Fields: []graphql.Field{
			{Name: "id"},
			{Name: "vector"},
			{Name: "distance"},
		},
	})

	return fields
}

// buildGetFields constructs the GraphQL field list for non-vector queries.
// Omits distance which is only valid for near* queries.
func (p *Provider) buildGetFields() []graphql.Field {
	fields := make([]graphql.Field, 0, len(p.config.Properties)+1)

	for _, prop := range p.config.Properties {
		fields = append(fields, graphql.Field{Name: prop})
	}

	fields = append(fields, graphql.Field{
		Name: "_additional",
		Fields: []graphql.Field{
			{Name: "id"},
			{Name: "vector"},
		},
	})

	return fields
}

// Upsert stores or updates a vector with associated metadata.
func (p *Provider) Upsert(ctx context.Context, id uuid.UUID, vector []float32, metadata []byte) error {
	props, err := bytesToProperties(metadata)
	if err != nil {
		return err
	}

	// Try to update first, create if not exists
	exists, err := p.Exists(ctx, id)
	if err != nil {
		return err
	}

	if exists {
		return p.client.Data().Updater().
			WithClassName(p.config.Class).
			WithID(id.String()).
			WithProperties(props).
			WithVector(vector).
			Do(ctx)
	}

	_, err = p.client.Data().Creator().
		WithClassName(p.config.Class).
		WithID(id.String()).
		WithProperties(props).
		WithVector(vector).
		Do(ctx)
	return err
}

// UpsertBatch stores or updates multiple vectors.
func (p *Provider) UpsertBatch(ctx context.Context, vectors []grub.VectorRecord) error {
	if len(vectors) == 0 {
		return nil
	}

	objects := make([]*models.Object, len(vectors))
	for i, v := range vectors {
		props, err := bytesToProperties(v.Metadata)
		if err != nil {
			return err
		}

		objects[i] = &models.Object{
			Class:      p.config.Class,
			ID:         strfmt.UUID(v.ID.String()),
			Properties: props,
			Vector:     v.Vector,
		}
	}

	_, err := p.client.Batch().ObjectsBatcher().
		WithObjects(objects...).
		Do(ctx)
	return err
}

// Get retrieves a vector by ID.
func (p *Provider) Get(ctx context.Context, id uuid.UUID) ([]float32, *grub.VectorInfo, error) {
	objs, err := p.client.Data().ObjectsGetter().
		WithClassName(p.config.Class).
		WithID(id.String()).
		WithVector().
		Do(ctx)
	if err != nil {
		// Check if it's a not found error
		if isNotFoundError(err) {
			return nil, nil, grub.ErrNotFound
		}
		return nil, nil, err
	}

	if len(objs) == 0 {
		return nil, nil, grub.ErrNotFound
	}

	obj := objs[0]
	vector := obj.Vector

	props, _ := obj.Properties.(map[string]any)
	metadata, err := propertiesToBytes(props)
	if err != nil {
		return nil, nil, err
	}

	return vector, &grub.VectorInfo{
		ID:        id,
		Dimension: len(vector),
		Metadata:  metadata,
	}, nil
}

// Delete removes a vector by ID.
func (p *Provider) Delete(ctx context.Context, id uuid.UUID) error {
	exists, err := p.Exists(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return grub.ErrNotFound
	}

	return p.client.Data().Deleter().
		WithClassName(p.config.Class).
		WithID(id.String()).
		Do(ctx)
}

// DeleteBatch removes multiple vectors by ID.
// Non-existent IDs are silently ignored, but other errors are returned.
func (p *Provider) DeleteBatch(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	for _, id := range ids {
		err := p.client.Data().Deleter().
			WithClassName(p.config.Class).
			WithID(id.String()).
			Do(ctx)
		if err != nil && !isNotFoundError(err) {
			return err
		}
	}
	return nil
}

// Search performs similarity search and returns the k nearest neighbors.
func (p *Provider) Search(ctx context.Context, vector []float32, k int, filter map[string]any) ([]grub.VectorResult, error) {
	nearVector := p.client.GraphQL().NearVectorArgBuilder().
		WithVector(vector)

	query := p.client.GraphQL().Get().
		WithClassName(p.config.Class).
		WithNearVector(nearVector).
		WithLimit(k).
		WithFields(p.buildSearchFields()...)

	if len(filter) > 0 {
		where := buildWhereFilter(filter)
		query = query.WithWhere(where)
	}

	resp, err := query.Do(ctx)
	if err != nil {
		return nil, err
	}

	return parseSearchResults(resp, p.config.Class)
}

// Query performs similarity search with vecna filter support.
func (p *Provider) Query(ctx context.Context, vector []float32, k int, filter *vecna.Filter) ([]grub.VectorResult, error) {
	nearVector := p.client.GraphQL().NearVectorArgBuilder().
		WithVector(vector)

	query := p.client.GraphQL().Get().
		WithClassName(p.config.Class).
		WithNearVector(nearVector).
		WithLimit(k).
		WithFields(p.buildSearchFields()...)

	if filter != nil {
		where, err := translateFilter(filter)
		if err != nil {
			return nil, err
		}
		if where != nil {
			query = query.WithWhere(where)
		}
	}

	resp, err := query.Do(ctx)
	if err != nil {
		return nil, err
	}

	return parseSearchResults(resp, p.config.Class)
}

// Filter returns vectors matching the metadata filter without similarity search.
// Uses GraphQL Get with Where filter. Paginates when limit=0.
func (p *Provider) Filter(ctx context.Context, filter *vecna.Filter, limit int) ([]grub.VectorResult, error) {
	var where *filters.WhereBuilder
	if filter != nil {
		var err error
		where, err = translateFilter(filter)
		if err != nil {
			return nil, err
		}
	}

	const pageSize = 100
	var allResults []grub.VectorResult
	offset := 0

	for {
		fetchLimit := pageSize
		if limit > 0 {
			remaining := limit - len(allResults)
			if remaining <= 0 {
				break
			}
			if remaining < fetchLimit {
				fetchLimit = remaining
			}
		}

		query := p.client.GraphQL().Get().
			WithClassName(p.config.Class).
			WithLimit(fetchLimit).
			WithOffset(offset).
			WithFields(p.buildGetFields()...)

		if where != nil {
			query = query.WithWhere(where)
		}

		resp, err := query.Do(ctx)
		if err != nil {
			return nil, err
		}

		batch, err := parseSearchResults(resp, p.config.Class)
		if err != nil {
			return nil, err
		}

		if len(batch) == 0 {
			break
		}

		allResults = append(allResults, batch...)
		offset += len(batch)

		if len(batch) < fetchLimit {
			break
		}

		if limit > 0 && len(allResults) >= limit {
			break
		}
	}

	return allResults, nil
}

// List returns vector IDs.
func (p *Provider) List(ctx context.Context, limit int) ([]uuid.UUID, error) {
	fetchLimit := 100
	if limit > 0 {
		fetchLimit = limit
	}

	query := p.client.GraphQL().Get().
		WithClassName(p.config.Class).
		WithLimit(fetchLimit).
		WithFields(graphql.Field{
			Name: "_additional",
			Fields: []graphql.Field{
				{Name: "id"},
			},
		})

	resp, err := query.Do(ctx)
	if err != nil {
		return nil, err
	}

	return parseIDs(resp, p.config.Class, limit)
}

// Exists checks whether a vector ID exists.
func (p *Provider) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	return p.client.Data().Checker().
		WithClassName(p.config.Class).
		WithID(id.String()).
		Do(ctx)
}

// buildWhereFilter builds a Weaviate where filter from map.
func buildWhereFilter(m map[string]any) *filters.WhereBuilder {
	if len(m) == 0 {
		return nil
	}

	var clauses []*filters.WhereBuilder
	for k, v := range m {
		clause := filters.Where().WithPath([]string{k})
		switch val := v.(type) {
		case string:
			clause = clause.WithOperator(filters.Equal).WithValueText(val)
		case int:
			clause = clause.WithOperator(filters.Equal).WithValueInt(int64(val))
		case int64:
			clause = clause.WithOperator(filters.Equal).WithValueInt(val)
		case float64:
			clause = clause.WithOperator(filters.Equal).WithValueNumber(val)
		case bool:
			clause = clause.WithOperator(filters.Equal).WithValueBoolean(val)
		default:
			continue
		}
		clauses = append(clauses, clause)
	}

	if len(clauses) == 1 {
		return clauses[0]
	}

	return filters.Where().
		WithOperator(filters.And).
		WithOperands(clauses)
}

// parseSearchResults parses GraphQL response to VectorResult slice.
func parseSearchResults(resp *models.GraphQLResponse, class string) ([]grub.VectorResult, error) {
	if resp.Errors != nil && len(resp.Errors) > 0 {
		return nil, fmt.Errorf("weaviate: %s", resp.Errors[0].Message)
	}

	data, ok := resp.Data["Get"].(map[string]any)
	if !ok {
		return nil, nil
	}

	items, ok := data[class].([]any)
	if !ok {
		return nil, nil
	}

	results := make([]grub.VectorResult, 0, len(items))
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}

		additional, _ := obj["_additional"].(map[string]any)
		idStr, _ := additional["id"].(string)
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, err
		}
		distance, _ := additional["distance"].(float64)

		var vector []float32
		if vec, ok := additional["vector"].([]any); ok {
			vector = make([]float32, len(vec))
			for i, v := range vec {
				if f, ok := v.(float64); ok {
					vector[i] = float32(f)
				}
			}
		}

		// Extract metadata (everything except _additional)
		metaMap := make(map[string]any)
		for k, v := range obj {
			if k != "_additional" {
				metaMap[k] = v
			}
		}
		metadata, err := propertiesToBytes(metaMap)
		if err != nil {
			return nil, err
		}

		results = append(results, grub.VectorResult{
			ID:       id,
			Vector:   vector,
			Metadata: metadata,
			Score:    float32(distance),
		})
	}

	return results, nil
}

// parseIDs parses GraphQL response to ID slice.
func parseIDs(resp *models.GraphQLResponse, class string, limit int) ([]uuid.UUID, error) {
	if resp.Errors != nil && len(resp.Errors) > 0 {
		return nil, fmt.Errorf("weaviate: %s", resp.Errors[0].Message)
	}

	data, ok := resp.Data["Get"].(map[string]any)
	if !ok {
		return nil, nil
	}

	items, ok := data[class].([]any)
	if !ok {
		return nil, nil
	}

	ids := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		additional, _ := obj["_additional"].(map[string]any)
		idStr, ok := additional["id"].(string)
		if !ok || idStr == "" {
			continue
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
		if limit > 0 && len(ids) >= limit {
			break
		}
	}

	return ids, nil
}

// bytesToProperties converts []byte to map[string]any properties.
func bytesToProperties(data []byte) (map[string]any, error) {
	if data == nil {
		return make(map[string]any), nil
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// propertiesToBytes converts map[string]any properties to []byte.
func propertiesToBytes(props map[string]any) ([]byte, error) {
	if props == nil || len(props) == 0 {
		return nil, nil
	}
	return json.Marshal(props)
}
