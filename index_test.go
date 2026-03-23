package grub

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/zoobz-io/vecna"
)

// mockVectorProvider implements VectorProvider for testing.
type mockVectorProvider struct {
	vectors   map[uuid.UUID]vectorEntry
	upsertErr error
	getErr    error
	deleteErr error
	searchErr error
	queryErr  error
	filterErr error
	listErr   error
	existsErr error
}

type vectorEntry struct {
	vector   []float32
	metadata []byte
}

func newMockVectorProvider() *mockVectorProvider {
	return &mockVectorProvider{
		vectors: make(map[uuid.UUID]vectorEntry),
	}
}

func (m *mockVectorProvider) Upsert(_ context.Context, id uuid.UUID, vector []float32, metadata []byte) error {
	if m.upsertErr != nil {
		return m.upsertErr
	}
	m.vectors[id] = vectorEntry{vector: vector, metadata: metadata}
	return nil
}

func (m *mockVectorProvider) UpsertBatch(_ context.Context, vectors []VectorRecord) error {
	if m.upsertErr != nil {
		return m.upsertErr
	}
	for _, v := range vectors {
		m.vectors[v.ID] = vectorEntry{vector: v.Vector, metadata: v.Metadata}
	}
	return nil
}

func (m *mockVectorProvider) Get(_ context.Context, id uuid.UUID) (vector []float32, info *VectorInfo, err error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	entry, ok := m.vectors[id]
	if !ok {
		return nil, nil, ErrNotFound
	}
	return entry.vector, &VectorInfo{
		ID:        id,
		Dimension: len(entry.vector),
		Metadata:  entry.metadata,
	}, nil
}

func (m *mockVectorProvider) Delete(_ context.Context, id uuid.UUID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	if _, ok := m.vectors[id]; !ok {
		return ErrNotFound
	}
	delete(m.vectors, id)
	return nil
}

func (m *mockVectorProvider) DeleteBatch(_ context.Context, ids []uuid.UUID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	for _, id := range ids {
		delete(m.vectors, id)
	}
	return nil
}

func (m *mockVectorProvider) Search(_ context.Context, vector []float32, k int, filter map[string]any) ([]VectorResult, error) {
	if m.searchErr != nil {
		return nil, m.searchErr
	}
	// Simple L2 distance search
	type scored struct {
		id       uuid.UUID
		entry    vectorEntry
		distance float32
	}
	items := make([]scored, 0, len(m.vectors))
	for id, entry := range m.vectors {
		// Check filter (simple equality check) - unmarshal entry metadata to map for comparison
		var entryMeta map[string]any
		if entry.metadata != nil {
			_ = json.Unmarshal(entry.metadata, &entryMeta)
		}
		if !matchesFilter(entryMeta, filter) {
			continue
		}
		dist := l2Distance(vector, entry.vector)
		items = append(items, scored{id: id, entry: entry, distance: dist})
	}
	// Sort by distance
	sort.Slice(items, func(i, j int) bool {
		return items[i].distance < items[j].distance
	})
	// Take top k
	if k > 0 && len(items) > k {
		items = items[:k]
	}
	results := make([]VectorResult, len(items))
	for i, s := range items {
		results[i] = VectorResult{
			ID:       s.id,
			Vector:   s.entry.vector,
			Metadata: s.entry.metadata,
			Score:    s.distance,
		}
	}
	return results, nil
}

func (m *mockVectorProvider) Query(_ context.Context, vector []float32, k int, _ *vecna.Filter) ([]VectorResult, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	// For testing, just do a simple search without filter evaluation
	type scored struct {
		id       uuid.UUID
		entry    vectorEntry
		distance float32
	}
	items := make([]scored, 0, len(m.vectors))
	for id, entry := range m.vectors {
		dist := l2Distance(vector, entry.vector)
		items = append(items, scored{id: id, entry: entry, distance: dist})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].distance < items[j].distance
	})
	if k > 0 && len(items) > k {
		items = items[:k]
	}
	results := make([]VectorResult, len(items))
	for i, s := range items {
		results[i] = VectorResult{
			ID:       s.id,
			Vector:   s.entry.vector,
			Metadata: s.entry.metadata,
			Score:    s.distance,
		}
	}
	return results, nil
}

func (m *mockVectorProvider) List(_ context.Context, limit int) ([]uuid.UUID, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	n := len(m.vectors)
	if limit > 0 && limit < n {
		n = limit
	}
	ids := make([]uuid.UUID, 0, n)
	for id := range m.vectors {
		ids = append(ids, id)
		if limit > 0 && len(ids) >= limit {
			break
		}
	}
	return ids, nil
}

func (m *mockVectorProvider) Exists(_ context.Context, id uuid.UUID) (bool, error) {
	if m.existsErr != nil {
		return false, m.existsErr
	}
	_, ok := m.vectors[id]
	return ok, nil
}

func (m *mockVectorProvider) Filter(_ context.Context, _ *vecna.Filter, limit int) ([]VectorResult, error) {
	if m.filterErr != nil {
		return nil, m.filterErr
	}
	// For testing, just return all vectors (no filter evaluation).
	results := make([]VectorResult, 0, len(m.vectors))
	for id, entry := range m.vectors {
		results = append(results, VectorResult{
			ID:       id,
			Vector:   entry.vector,
			Metadata: entry.metadata,
		})
		if limit > 0 && len(results) >= limit {
			break
		}
	}
	return results, nil
}

func matchesFilter(metadata, filter map[string]any) bool {
	if filter == nil {
		return true
	}
	for k, v := range filter {
		if metadata[k] != v {
			return false
		}
	}
	return true
}

func l2Distance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.MaxFloat32)
	}
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

type testMetadata struct {
	Category string `json:"category" atom:"category"`
	Score    int    `json:"score" atom:"score"`
}

func TestNewIndex(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)

	if index == nil {
		t.Fatal("NewIndex returned nil")
	}
	if index.provider != provider {
		t.Error("provider not set correctly")
	}
	if index.codec == nil {
		t.Error("codec should default to JSONCodec")
	}
}

func TestNewIndexWithCodec(t *testing.T) {
	provider := newMockVectorProvider()
	codec := GobCodec{}
	index := NewIndexWithCodec[testMetadata](provider, codec)

	if index == nil {
		t.Fatal("NewIndexWithCodec returned nil")
	}
}

func TestIndex_Upsert(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	t.Run("basic upsert", func(t *testing.T) {
		id := uuid.New()
		metadata := &testMetadata{Category: "test", Score: 42}
		err := index.Upsert(ctx, id, []float32{1.0, 2.0, 3.0}, metadata)
		if err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}

		if _, ok := provider.vectors[id]; !ok {
			t.Error("vector not stored in provider")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.upsertErr = errors.New("upsert error")
		defer func() { provider.upsertErr = nil }()

		metadata := &testMetadata{Category: "fail", Score: 0}
		err := index.Upsert(ctx, uuid.New(), []float32{1.0}, metadata)
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestIndex_UpsertBatch(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	t.Run("basic batch", func(t *testing.T) {
		vectors := []Vector[testMetadata]{
			{ID: uuid.New(), Vector: []float32{1.0, 0.0}, Metadata: testMetadata{Category: "a", Score: 1}},
			{ID: uuid.New(), Vector: []float32{0.0, 1.0}, Metadata: testMetadata{Category: "b", Score: 2}},
		}
		err := index.UpsertBatch(ctx, vectors)
		if err != nil {
			t.Fatalf("UpsertBatch failed: %v", err)
		}

		if len(provider.vectors) != 2 {
			t.Errorf("expected 2 vectors, got %d", len(provider.vectors))
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.upsertErr = errors.New("batch error")
		defer func() { provider.upsertErr = nil }()

		vectors := []Vector[testMetadata]{
			{ID: uuid.New(), Vector: []float32{1.0}, Metadata: testMetadata{}},
		}
		err := index.UpsertBatch(ctx, vectors)
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestIndex_Get(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	t.Run("existing id", func(t *testing.T) {
		id := uuid.New()
		provider.vectors[id] = vectorEntry{
			vector:   []float32{1.0, 2.0, 3.0},
			metadata: []byte(`{"category": "test", "score": 42}`),
		}

		vec, err := index.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if vec.ID != id {
			t.Errorf("expected ID '%s', got '%s'", id, vec.ID)
		}
		if len(vec.Vector) != 3 {
			t.Errorf("expected 3-dim vector, got %d", len(vec.Vector))
		}
		if vec.Metadata.Category != "test" {
			t.Errorf("expected category 'test', got %q", vec.Metadata.Category)
		}
	})

	t.Run("missing id", func(t *testing.T) {
		_, err := index.Get(ctx, uuid.New())
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("provider error", func(t *testing.T) {
		id := uuid.New()
		provider.vectors[id] = vectorEntry{vector: []float32{1.0}}
		provider.getErr = errors.New("get error")
		defer func() { provider.getErr = nil }()

		_, err := index.Get(ctx, id)
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestIndex_Delete(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	t.Run("existing id", func(t *testing.T) {
		id := uuid.New()
		provider.vectors[id] = vectorEntry{vector: []float32{1.0}}

		err := index.Delete(ctx, id)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if _, ok := provider.vectors[id]; ok {
			t.Error("vector should have been deleted")
		}
	})

	t.Run("missing id", func(t *testing.T) {
		err := index.Delete(ctx, uuid.New())
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestIndex_DeleteBatch(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()
	provider.vectors[id1] = vectorEntry{vector: []float32{1.0}}
	provider.vectors[id2] = vectorEntry{vector: []float32{2.0}}
	provider.vectors[id3] = vectorEntry{vector: []float32{3.0}}

	err := index.DeleteBatch(ctx, []uuid.UUID{id1, id2, uuid.New()})
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}

	if _, ok := provider.vectors[id1]; ok {
		t.Error("id1 should have been deleted")
	}
	if _, ok := provider.vectors[id2]; ok {
		t.Error("id2 should have been deleted")
	}
	if _, ok := provider.vectors[id3]; !ok {
		t.Error("id3 should still exist")
	}
}

func TestIndex_Search(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	// Setup test vectors
	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()
	provider.vectors[id1] = vectorEntry{
		vector:   []float32{1.0, 0.0, 0.0},
		metadata: []byte(`{"category": "a", "score": 1}`),
	}
	provider.vectors[id2] = vectorEntry{
		vector:   []float32{0.0, 1.0, 0.0},
		metadata: []byte(`{"category": "b", "score": 2}`),
	}
	provider.vectors[id3] = vectorEntry{
		vector:   []float32{0.0, 0.0, 1.0},
		metadata: []byte(`{"category": "a", "score": 3}`),
	}

	t.Run("basic search", func(t *testing.T) {
		query := []float32{1.0, 0.0, 0.0}
		results, err := index.Search(ctx, query, 2, nil)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
		// First result should be id1 (exact match)
		if results[0].ID != id1 {
			t.Errorf("expected first result to be %s, got %s", id1, results[0].ID)
		}
	})

	t.Run("search with filter", func(t *testing.T) {
		query := []float32{0.5, 0.5, 0.0}
		filter := &testMetadata{Category: "a"}
		results, err := index.Search(ctx, query, 10, filter)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		// Should only return category "a" vectors (id1 and id3)
		for _, r := range results {
			if r.Metadata.Category != "a" {
				t.Errorf("expected category 'a', got %q", r.Metadata.Category)
			}
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.searchErr = errors.New("search error")
		defer func() { provider.searchErr = nil }()

		_, err := index.Search(ctx, []float32{1.0}, 1, nil)
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestIndex_Query(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	// Setup test vectors
	id1, id2 := uuid.New(), uuid.New()
	provider.vectors[id1] = vectorEntry{
		vector:   []float32{1.0, 0.0},
		metadata: []byte(`{"category":"a","score":10}`),
	}
	provider.vectors[id2] = vectorEntry{
		vector:   []float32{0.0, 1.0},
		metadata: []byte(`{"category":"b","score":20}`),
	}

	t.Run("nil filter", func(t *testing.T) {
		results, err := index.Query(ctx, []float32{1.0, 0.0}, 10, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
	})

	t.Run("with limit", func(t *testing.T) {
		results, err := index.Query(ctx, []float32{1.0, 0.0}, 1, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result, got %d", len(results))
		}
	})

	t.Run("metadata decoded", func(t *testing.T) {
		results, err := index.Query(ctx, []float32{1.0, 0.0}, 10, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		for _, r := range results {
			if r.Metadata.Category == "" {
				t.Error("expected category to be decoded")
			}
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.queryErr = errors.New("query error")
		defer func() { provider.queryErr = nil }()

		_, err := index.Query(ctx, []float32{1.0, 0.0}, 1, nil)
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestIndex_List(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	provider.vectors[uuid.New()] = vectorEntry{vector: []float32{1.0}}
	provider.vectors[uuid.New()] = vectorEntry{vector: []float32{2.0}}
	provider.vectors[uuid.New()] = vectorEntry{vector: []float32{3.0}}

	t.Run("list all", func(t *testing.T) {
		ids, err := index.List(ctx, 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(ids) != 3 {
			t.Errorf("expected 3 ids, got %d", len(ids))
		}
	})

	t.Run("with limit", func(t *testing.T) {
		ids, err := index.List(ctx, 1)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(ids) != 1 {
			t.Errorf("expected 1 id, got %d", len(ids))
		}
	})
}

func TestIndex_Exists(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	id := uuid.New()
	provider.vectors[id] = vectorEntry{vector: []float32{1.0}}

	t.Run("existing id", func(t *testing.T) {
		exists, err := index.Exists(ctx, id)
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("expected id to exist")
		}
	})

	t.Run("missing id", func(t *testing.T) {
		exists, err := index.Exists(ctx, uuid.New())
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected id to not exist")
		}
	})
}

func TestIndex_Filter(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	// Insert test data
	id1, id2 := uuid.New(), uuid.New()
	provider.vectors[id1] = vectorEntry{
		vector:   []float32{1.0, 0.0},
		metadata: []byte(`{"category":"a","score":10}`),
	}
	provider.vectors[id2] = vectorEntry{
		vector:   []float32{0.0, 1.0},
		metadata: []byte(`{"category":"b","score":20}`),
	}

	t.Run("nil filter returns all", func(t *testing.T) {
		results, err := index.Filter(ctx, nil, 0)
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
	})

	t.Run("with limit", func(t *testing.T) {
		results, err := index.Filter(ctx, nil, 1)
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result, got %d", len(results))
		}
	})

	t.Run("metadata decoded", func(t *testing.T) {
		results, err := index.Filter(ctx, nil, 0)
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}
		for _, r := range results {
			if r.Metadata.Category == "" {
				t.Error("expected metadata to be decoded")
			}
		}
	})
}

func TestIndex_FilterProviderError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	provider.filterErr = errors.New("filter error")
	defer func() { provider.filterErr = nil }()

	_, err := index.Filter(ctx, nil, 0)
	if err == nil {
		t.Error("expected provider error")
	}
}

func TestIndex_NilMetadata(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	t.Run("Upsert with nil metadata", func(t *testing.T) {
		id := uuid.New()
		err := index.Upsert(ctx, id, []float32{1.0}, nil)
		if err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
		// Verify it was stored with nil metadata
		if provider.vectors[id].metadata != nil {
			t.Error("expected nil metadata to be stored")
		}
	})

	t.Run("Get with nil metadata", func(t *testing.T) {
		id := uuid.New()
		provider.vectors[id] = vectorEntry{
			vector:   []float32{1.0},
			metadata: nil,
		}
		result, err := index.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		// Zero value metadata should be returned
		if result.Metadata.Category != "" || result.Metadata.Score != 0 {
			t.Error("expected zero value metadata")
		}
	})

	t.Run("Search with nil metadata in results", func(t *testing.T) {
		id := uuid.New()
		provider.vectors[id] = vectorEntry{
			vector:   []float32{1.0, 0.0},
			metadata: nil,
		}
		results, err := index.Search(ctx, []float32{1.0, 0.0}, 10, nil)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(results) == 0 {
			t.Error("expected results")
		}
	})

	t.Run("Query with nil metadata in results", func(t *testing.T) {
		id := uuid.New()
		provider.vectors[id] = vectorEntry{
			vector:   []float32{1.0, 0.0},
			metadata: nil,
		}
		results, err := index.Query(ctx, []float32{1.0, 0.0}, 10, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) == 0 {
			t.Error("expected results")
		}
	})
}

func TestIndex_RoundTrip(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	id := uuid.New()
	original := &testMetadata{Category: "roundtrip", Score: 42}
	vector := []float32{1.0, 2.0, 3.0}

	if err := index.Upsert(ctx, id, vector, original); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	retrieved, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Metadata.Category != original.Category {
		t.Errorf("Category mismatch: got %q, want %q", retrieved.Metadata.Category, original.Category)
	}
	if retrieved.Metadata.Score != original.Score {
		t.Errorf("Score mismatch: got %d, want %d", retrieved.Metadata.Score, original.Score)
	}
	if len(retrieved.Vector) != len(vector) {
		t.Errorf("Vector length mismatch: got %d, want %d", len(retrieved.Vector), len(vector))
	}
}

func TestIndex_DecodeErrors(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	// Store invalid JSON that can't be decoded to testMetadata
	badID := uuid.New()
	provider.vectors[badID] = vectorEntry{
		vector:   []float32{1.0, 2.0},
		metadata: []byte(`{invalid json`),
	}

	t.Run("Get decode error", func(t *testing.T) {
		_, err := index.Get(ctx, badID)
		if err == nil {
			t.Error("expected decode error")
		}
	})

	t.Run("Search decode error", func(t *testing.T) {
		_, err := index.Search(ctx, []float32{1.0, 2.0}, 10, nil)
		if err == nil {
			t.Error("expected decode error")
		}
	})

	t.Run("Query decode error", func(t *testing.T) {
		_, err := index.Query(ctx, []float32{1.0, 2.0}, 10, nil)
		if err == nil {
			t.Error("expected decode error")
		}
	})

	t.Run("Filter decode error", func(t *testing.T) {
		_, err := index.Filter(ctx, nil, 0)
		if err == nil {
			t.Error("expected decode error")
		}
	})
}

// errorCodec is a codec that can be configured to fail.
type errorCodec struct {
	encodeErr error
	decodeErr error
}

func (f errorCodec) Encode(v any) ([]byte, error) {
	if f.encodeErr != nil {
		return nil, f.encodeErr
	}
	return json.Marshal(v)
}

func (f errorCodec) Decode(data []byte, v any) error {
	if f.decodeErr != nil {
		return f.decodeErr
	}
	return json.Unmarshal(data, v)
}

func TestIndex_EncodeErrors(t *testing.T) {
	provider := newMockVectorProvider()
	codec := errorCodec{encodeErr: errors.New("encode failed")}
	index := NewIndexWithCodec[testMetadata](provider, codec)
	ctx := context.Background()

	t.Run("Upsert encode error", func(t *testing.T) {
		err := index.Upsert(ctx, uuid.New(), []float32{1.0}, &testMetadata{})
		if err == nil {
			t.Error("expected encode error")
		}
	})

	t.Run("UpsertBatch encode error", func(t *testing.T) {
		vectors := []Vector[testMetadata]{
			{ID: uuid.New(), Vector: []float32{1.0}, Metadata: testMetadata{}},
		}
		err := index.UpsertBatch(ctx, vectors)
		if err == nil {
			t.Error("expected encode error")
		}
	})

	t.Run("Search encodeFilter error", func(t *testing.T) {
		filter := &testMetadata{Category: "test"}
		_, err := index.Search(ctx, []float32{1.0}, 10, filter)
		if err == nil {
			t.Error("expected encode error")
		}
	})
}

func TestIndex_EncodeFilterDecodeError(t *testing.T) {
	provider := newMockVectorProvider()
	// Codec that encodes fine but fails on decode (for encodeFilter's second step)
	codec := errorCodec{decodeErr: errors.New("decode failed")}
	index := NewIndexWithCodec[testMetadata](provider, codec)
	ctx := context.Background()

	filter := &testMetadata{Category: "test"}
	_, err := index.Search(ctx, []float32{1.0}, 10, filter)
	if err == nil {
		t.Error("expected decode error in encodeFilter")
	}
}

func TestIndex_Atomic(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testMetadata](provider)
	ctx := context.Background()

	id := uuid.New()
	provider.vectors[id] = vectorEntry{
		vector:   []float32{1.0, 2.0},
		metadata: []byte(`{"category": "atomic", "score": 99}`),
	}

	atomic := index.Atomic()
	if atomic == nil {
		t.Fatal("Atomic returned nil")
	}

	// Verify it returns the same instance
	atomic2 := index.Atomic()
	if atomic != atomic2 {
		t.Error("Atomic should return cached instance")
	}

	// Test that atomic view works
	a, err := atomic.Get(ctx, id)
	if err != nil {
		t.Fatalf("Atomic Get failed: %v", err)
	}
	if a.Metadata.Strings["Category"] != "atomic" {
		t.Errorf("unexpected Category: %q", a.Metadata.Strings["Category"])
	}
}
