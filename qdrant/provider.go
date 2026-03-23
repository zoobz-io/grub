// Package qdrant provides a grub VectorProvider implementation for Qdrant.
package qdrant

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/vecna"
)

// uuidToPointID converts a uuid.UUID to a qdrant PointId.
func uuidToPointID(id uuid.UUID) *qdrant.PointId {
	return qdrant.NewID(id.String())
}

// Config holds configuration for the Qdrant provider.
type Config struct {
	// Collection is the name of the Qdrant collection.
	Collection string
}

// Provider implements grub.VectorProvider for Qdrant.
type Provider struct {
	client *qdrant.Client
	config Config
}

// New creates a Qdrant provider with the given client and config.
func New(client *qdrant.Client, config Config) *Provider {
	return &Provider{
		client: client,
		config: config,
	}
}

// Upsert stores or updates a vector with associated metadata.
func (p *Provider) Upsert(ctx context.Context, id uuid.UUID, vector []float32, metadata []byte) error {
	payload, err := bytesToPayload(metadata)
	if err != nil {
		return err
	}

	points := []*qdrant.PointStruct{
		{
			Id:      uuidToPointID(id),
			Vectors: qdrant.NewVectors(vector...),
			Payload: payload,
		},
	}

	_, err = p.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: p.config.Collection,
		Points:         points,
		Wait:           qdrant.PtrOf(true),
	})
	return err
}

// UpsertBatch stores or updates multiple vectors.
func (p *Provider) UpsertBatch(ctx context.Context, vectors []grub.VectorRecord) error {
	if len(vectors) == 0 {
		return nil
	}

	points := make([]*qdrant.PointStruct, len(vectors))
	for i, v := range vectors {
		payload, err := bytesToPayload(v.Metadata)
		if err != nil {
			return err
		}
		points[i] = &qdrant.PointStruct{
			Id:      uuidToPointID(v.ID),
			Vectors: qdrant.NewVectors(v.Vector...),
			Payload: payload,
		}
	}

	_, err := p.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: p.config.Collection,
		Points:         points,
		Wait:           qdrant.PtrOf(true),
	})
	return err
}

// Get retrieves a vector by ID.
func (p *Provider) Get(ctx context.Context, id uuid.UUID) ([]float32, *grub.VectorInfo, error) {
	resp, err := p.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: p.config.Collection,
		Ids:            []*qdrant.PointId{uuidToPointID(id)},
		WithVectors:    qdrant.NewWithVectors(true),
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, nil, err
	}

	if len(resp) == 0 {
		return nil, nil, grub.ErrNotFound
	}

	point := resp[0]
	vector := point.Vectors.GetVector().Data
	metadata, err := payloadToBytes(point.Payload)
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
	// Check if exists first
	exists, err := p.Exists(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return grub.ErrNotFound
	}

	_, err = p.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: p.config.Collection,
		Points:         qdrant.NewPointsSelector(uuidToPointID(id)),
	})
	return err
}

// DeleteBatch removes multiple vectors by ID.
func (p *Provider) DeleteBatch(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	pointIds := make([]*qdrant.PointId, len(ids))
	for i, id := range ids {
		pointIds[i] = uuidToPointID(id)
	}

	_, err := p.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: p.config.Collection,
		Points: &qdrant.PointsSelector{
			PointsSelectorOneOf: &qdrant.PointsSelector_Points{
				Points: &qdrant.PointsIdsList{
					Ids: pointIds,
				},
			},
		},
	})
	return err
}

// Search performs similarity search and returns the k nearest neighbors.
func (p *Provider) Search(ctx context.Context, vector []float32, k int, filter map[string]any) ([]grub.VectorResult, error) {
	req := &qdrant.QueryPoints{
		CollectionName: p.config.Collection,
		Query:          qdrant.NewQuery(vector...),
		Limit:          qdrant.PtrOf(uint64(k)),
		WithVectors:    qdrant.NewWithVectors(true),
		WithPayload:    qdrant.NewWithPayload(true),
	}

	if len(filter) > 0 {
		req.Filter = buildFilter(filter)
	}

	resp, err := p.client.Query(ctx, req)
	if err != nil {
		return nil, err
	}

	results := make([]grub.VectorResult, len(resp))
	for i, scored := range resp {
		id, err := uuid.Parse(scored.Id.GetUuid())
		if err != nil {
			return nil, err
		}
		metadata, err := payloadToBytes(scored.Payload)
		if err != nil {
			return nil, err
		}
		var vec []float32
		if scored.Vectors != nil {
			vec = scored.Vectors.GetVector().Data
		}
		results[i] = grub.VectorResult{
			ID:       id,
			Vector:   vec,
			Metadata: metadata,
			Score:    scored.Score,
		}
	}

	return results, nil
}

// Query performs similarity search with vecna filter support.
func (p *Provider) Query(ctx context.Context, vector []float32, k int, filter *vecna.Filter) ([]grub.VectorResult, error) {
	req := &qdrant.QueryPoints{
		CollectionName: p.config.Collection,
		Query:          qdrant.NewQuery(vector...),
		Limit:          qdrant.PtrOf(uint64(k)),
		WithVectors:    qdrant.NewWithVectors(true),
		WithPayload:    qdrant.NewWithPayload(true),
	}

	if filter != nil {
		translated, err := translateFilter(filter)
		if err != nil {
			return nil, err
		}
		req.Filter = translated
	}

	resp, err := p.client.Query(ctx, req)
	if err != nil {
		return nil, err
	}

	results := make([]grub.VectorResult, len(resp))
	for i, scored := range resp {
		id, err := uuid.Parse(scored.Id.GetUuid())
		if err != nil {
			return nil, err
		}
		metadata, err := payloadToBytes(scored.Payload)
		if err != nil {
			return nil, err
		}
		var vec []float32
		if scored.Vectors != nil {
			vec = scored.Vectors.GetVector().Data
		}
		results[i] = grub.VectorResult{
			ID:       id,
			Vector:   vec,
			Metadata: metadata,
			Score:    scored.Score,
		}
	}

	return results, nil
}

// Filter returns vectors matching the metadata filter without similarity search.
// Uses Scroll API with filter, returning results in storage order.
func (p *Provider) Filter(ctx context.Context, filter *vecna.Filter, limit int) ([]grub.VectorResult, error) {
	var pageLimit uint32 = 100
	if limit > 0 && limit < 100 {
		pageLimit = uint32(limit)
	}

	var qdrantFilter *qdrant.Filter
	if filter != nil {
		translated, err := translateFilter(filter)
		if err != nil {
			return nil, err
		}
		qdrantFilter = translated
	}

	results := make([]grub.VectorResult, 0)
	var offset *qdrant.PointId

	for {
		req := &qdrant.ScrollPoints{
			CollectionName: p.config.Collection,
			Limit:          qdrant.PtrOf(pageLimit),
			WithVectors:    qdrant.NewWithVectors(true),
			WithPayload:    qdrant.NewWithPayload(true),
			Offset:         offset,
			Filter:         qdrantFilter,
		}

		resp, err := p.client.Scroll(ctx, req)
		if err != nil {
			return nil, err
		}

		if len(resp) == 0 {
			break
		}

		for _, point := range resp {
			id, err := uuid.Parse(point.Id.GetUuid())
			if err != nil {
				return nil, err
			}
			metadata, err := payloadToBytes(point.Payload)
			if err != nil {
				return nil, err
			}
			var vec []float32
			if point.Vectors != nil {
				vec = point.Vectors.GetVector().Data
			}
			results = append(results, grub.VectorResult{
				ID:       id,
				Vector:   vec,
				Metadata: metadata,
			})
			if limit > 0 && len(results) >= limit {
				return results, nil
			}
			offset = point.Id
		}

		// If we got fewer records than requested, we've reached the end.
		if len(resp) < int(pageLimit) {
			break
		}
	}

	return results, nil
}

// List returns vector IDs.
func (p *Provider) List(ctx context.Context, limit int) ([]uuid.UUID, error) {
	var pageLimit uint32 = 100
	if limit > 0 && limit < 100 {
		pageLimit = uint32(limit)
	}

	ids := make([]uuid.UUID, 0)
	var offset *qdrant.PointId

	for {
		req := &qdrant.ScrollPoints{
			CollectionName: p.config.Collection,
			Limit:          qdrant.PtrOf(pageLimit),
			WithVectors:    qdrant.NewWithVectors(false),
			WithPayload:    qdrant.NewWithPayload(false),
			Offset:         offset,
		}

		resp, err := p.client.Scroll(ctx, req)
		if err != nil {
			return nil, err
		}

		if len(resp) == 0 {
			break
		}

		for _, point := range resp {
			id, err := uuid.Parse(point.Id.GetUuid())
			if err != nil {
				return nil, err
			}
			ids = append(ids, id)
			if limit > 0 && len(ids) >= limit {
				return ids, nil
			}
			offset = point.Id
		}

		// If we got fewer records than requested, we've reached the end
		if len(resp) < int(pageLimit) {
			break
		}
	}

	return ids, nil
}

// Exists checks whether a vector ID exists.
func (p *Provider) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	resp, err := p.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: p.config.Collection,
		Ids:            []*qdrant.PointId{uuidToPointID(id)},
		WithVectors:    qdrant.NewWithVectors(false),
		WithPayload:    qdrant.NewWithPayload(false),
	})
	if err != nil {
		return false, err
	}
	return len(resp) > 0, nil
}

// toPayload converts map[string]any to qdrant payload.
func toPayload(m map[string]any) map[string]*qdrant.Value {
	payload := make(map[string]*qdrant.Value, len(m))
	for k, v := range m {
		payload[k] = toValue(v)
	}
	return payload
}

// toValue converts any to *qdrant.Value.
func toValue(v any) *qdrant.Value {
	switch val := v.(type) {
	case string:
		return qdrant.NewValueString(val)
	case float64:
		return qdrant.NewValueDouble(val)
	case int:
		return qdrant.NewValueInt(int64(val))
	case int64:
		return qdrant.NewValueInt(val)
	case bool:
		return qdrant.NewValueBool(val)
	case []string:
		values := make([]*qdrant.Value, len(val))
		for i, s := range val {
			values[i] = qdrant.NewValueString(s)
		}
		return qdrant.NewValueList(&qdrant.ListValue{Values: values})
	case []any:
		values := make([]*qdrant.Value, len(val))
		for i, item := range val {
			values[i] = toValue(item)
		}
		return qdrant.NewValueList(&qdrant.ListValue{Values: values})
	default:
		// Fall back to JSON string
		data, _ := json.Marshal(v)
		return qdrant.NewValueString(string(data))
	}
}

// fromPayload converts qdrant payload to map[string]any.
func fromPayload(payload map[string]*qdrant.Value) map[string]any {
	if payload == nil {
		return nil
	}
	m := make(map[string]any, len(payload))
	for k, v := range payload {
		m[k] = fromValue(v)
	}
	return m
}

// bytesToPayload converts []byte to qdrant payload.
func bytesToPayload(data []byte) (map[string]*qdrant.Value, error) {
	if data == nil {
		return make(map[string]*qdrant.Value), nil
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return toPayload(m), nil
}

// payloadToBytes converts qdrant payload to []byte.
func payloadToBytes(payload map[string]*qdrant.Value) ([]byte, error) {
	if payload == nil || len(payload) == 0 {
		return nil, nil
	}
	m := fromPayload(payload)
	return json.Marshal(m)
}

// fromValue converts *qdrant.Value to any.
func fromValue(v *qdrant.Value) any {
	switch v.Kind.(type) {
	case *qdrant.Value_StringValue:
		return v.GetStringValue()
	case *qdrant.Value_DoubleValue:
		return v.GetDoubleValue()
	case *qdrant.Value_IntegerValue:
		return v.GetIntegerValue()
	case *qdrant.Value_BoolValue:
		return v.GetBoolValue()
	case *qdrant.Value_ListValue:
		list := v.GetListValue()
		if list == nil {
			return nil
		}
		result := make([]any, len(list.Values))
		for i, item := range list.Values {
			result[i] = fromValue(item)
		}
		return result
	default:
		return nil
	}
}

// buildFilter converts map[string]any to qdrant filter.
func buildFilter(m map[string]any) *qdrant.Filter {
	if len(m) == 0 {
		return nil
	}
	conditions := make([]*qdrant.Condition, 0, len(m))
	for k, v := range m {
		switch val := v.(type) {
		case string:
			conditions = append(conditions, qdrant.NewMatchKeyword(k, val))
		case int:
			conditions = append(conditions, qdrant.NewMatchInt(k, int64(val)))
		case int64:
			conditions = append(conditions, qdrant.NewMatchInt(k, val))
		case float64:
			// Qdrant doesn't have direct float match, use range
			conditions = append(conditions, &qdrant.Condition{
				ConditionOneOf: &qdrant.Condition_Field{
					Field: &qdrant.FieldCondition{
						Key: k,
						Range: &qdrant.Range{
							Gte: &val,
							Lte: &val,
						},
					},
				},
			})
		default:
			_ = val // Skip unsupported types
		}
	}
	return &qdrant.Filter{Must: conditions}
}
