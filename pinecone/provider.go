// Package pinecone provides a grub VectorProvider implementation for Pinecone.
package pinecone

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v2/pinecone"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/vecna"
)

// Config holds configuration for the Pinecone provider.
type Config struct {
	// Namespace is the Pinecone namespace for vector operations.
	Namespace string
}

// Provider implements grub.VectorProvider for Pinecone.
type Provider struct {
	index  *pinecone.IndexConnection
	config Config
}

// New creates a Pinecone provider with the given index connection and config.
func New(index *pinecone.IndexConnection, config Config) *Provider {
	return &Provider{
		index:  index,
		config: config,
	}
}

// Upsert stores or updates a vector with associated metadata.
func (p *Provider) Upsert(ctx context.Context, id uuid.UUID, vector []float32, metadata []byte) error {
	metaStruct, err := bytesToStruct(metadata)
	if err != nil {
		return err
	}

	vectors := []*pinecone.Vector{
		{
			Id:       id.String(),
			Values:   vector,
			Metadata: metaStruct,
		},
	}

	_, err = p.index.UpsertVectors(ctx, vectors)
	return err
}

// UpsertBatch stores or updates multiple vectors.
func (p *Provider) UpsertBatch(ctx context.Context, vectors []grub.VectorRecord) error {
	if len(vectors) == 0 {
		return nil
	}

	pcVectors := make([]*pinecone.Vector, len(vectors))
	for i, v := range vectors {
		metaStruct, err := bytesToStruct(v.Metadata)
		if err != nil {
			return err
		}
		pcVectors[i] = &pinecone.Vector{
			Id:       v.ID.String(),
			Values:   v.Vector,
			Metadata: metaStruct,
		}
	}

	_, err := p.index.UpsertVectors(ctx, pcVectors)
	return err
}

// Get retrieves a vector by ID.
func (p *Provider) Get(ctx context.Context, id uuid.UUID) ([]float32, *grub.VectorInfo, error) {
	idStr := id.String()
	resp, err := p.index.FetchVectors(ctx, []string{idStr})
	if err != nil {
		return nil, nil, err
	}

	vec, ok := resp.Vectors[idStr]
	if !ok {
		return nil, nil, grub.ErrNotFound
	}

	metadata, err := structToBytes(vec.Metadata)
	if err != nil {
		return nil, nil, err
	}

	return vec.Values, &grub.VectorInfo{
		ID:        id,
		Dimension: len(vec.Values),
		Metadata:  metadata,
	}, nil
}

// Delete removes a vector by ID.
func (p *Provider) Delete(ctx context.Context, id uuid.UUID) error {
	idStr := id.String()
	// Pinecone doesn't return error if ID doesn't exist, so we check first
	resp, err := p.index.FetchVectors(ctx, []string{idStr})
	if err != nil {
		return err
	}
	if _, ok := resp.Vectors[idStr]; !ok {
		return grub.ErrNotFound
	}

	return p.index.DeleteVectorsById(ctx, []string{idStr})
}

// DeleteBatch removes multiple vectors by ID.
func (p *Provider) DeleteBatch(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	return p.index.DeleteVectorsById(ctx, strs)
}

// Search performs similarity search and returns the k nearest neighbors.
func (p *Provider) Search(ctx context.Context, vector []float32, k int, filter map[string]any) ([]grub.VectorResult, error) {
	req := &pinecone.QueryByVectorValuesRequest{
		Vector:          vector,
		TopK:            uint32(k),
		IncludeValues:   true,
		IncludeMetadata: true,
	}

	if len(filter) > 0 {
		filterStruct, err := toStruct(filter)
		if err != nil {
			return nil, err
		}
		req.MetadataFilter = filterStruct
	}

	resp, err := p.index.QueryByVectorValues(ctx, req)
	if err != nil {
		return nil, err
	}

	results := make([]grub.VectorResult, len(resp.Matches))
	for i, match := range resp.Matches {
		metadata, err := structToBytes(match.Vector.Metadata)
		if err != nil {
			return nil, err
		}
		id, err := uuid.Parse(match.Vector.Id)
		if err != nil {
			return nil, err
		}
		results[i] = grub.VectorResult{
			ID:       id,
			Vector:   match.Vector.Values,
			Metadata: metadata,
			Score:    match.Score,
		}
	}

	return results, nil
}

// Query performs similarity search with vecna filter support.
func (p *Provider) Query(ctx context.Context, vector []float32, k int, filter *vecna.Filter) ([]grub.VectorResult, error) {
	req := &pinecone.QueryByVectorValuesRequest{
		Vector:          vector,
		TopK:            uint32(k),
		IncludeValues:   true,
		IncludeMetadata: true,
	}

	if filter != nil {
		filterStruct, err := translateFilter(filter)
		if err != nil {
			return nil, err
		}
		req.MetadataFilter = filterStruct
	}

	resp, err := p.index.QueryByVectorValues(ctx, req)
	if err != nil {
		return nil, err
	}

	results := make([]grub.VectorResult, len(resp.Matches))
	for i, match := range resp.Matches {
		metadata, err := structToBytes(match.Vector.Metadata)
		if err != nil {
			return nil, err
		}
		id, err := uuid.Parse(match.Vector.Id)
		if err != nil {
			return nil, err
		}
		results[i] = grub.VectorResult{
			ID:       id,
			Vector:   match.Vector.Values,
			Metadata: metadata,
			Score:    match.Score,
		}
	}

	return results, nil
}

// Filter returns ErrFilterNotSupported as Pinecone does not support metadata-only filtering.
func (p *Provider) Filter(_ context.Context, _ *vecna.Filter, _ int) ([]grub.VectorResult, error) {
	return nil, grub.ErrFilterNotSupported
}

// List returns vector IDs.
func (p *Provider) List(ctx context.Context, limit int) ([]uuid.UUID, error) {
	req := &pinecone.ListVectorsRequest{}
	if limit > 0 {
		l := uint32(limit)
		req.Limit = &l
	}

	resp, err := p.index.ListVectors(ctx, req)
	if err != nil {
		return nil, err
	}

	ids := make([]uuid.UUID, len(resp.VectorIds))
	for i, idPtr := range resp.VectorIds {
		id, err := uuid.Parse(*idPtr)
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

// Exists checks whether a vector ID exists.
func (p *Provider) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	idStr := id.String()
	resp, err := p.index.FetchVectors(ctx, []string{idStr})
	if err != nil {
		return false, err
	}
	_, ok := resp.Vectors[idStr]
	return ok, nil
}

// bytesToStruct converts []byte to *pinecone.Metadata.
func bytesToStruct(data []byte) (*pinecone.Metadata, error) {
	if data == nil {
		return nil, nil
	}
	var meta pinecone.Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// structToBytes converts *pinecone.Metadata to []byte.
func structToBytes(s *pinecone.Metadata) ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	return json.Marshal(s)
}
