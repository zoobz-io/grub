package atomix

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/zoobzio/atom"
	"github.com/zoobzio/grub/internal/shared"
	"github.com/zoobzio/vecna"
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

var errVectorNotFound = errors.New("not found")

func (m *mockVectorProvider) Upsert(_ context.Context, id uuid.UUID, vector []float32, metadata []byte) error {
	if m.upsertErr != nil {
		return m.upsertErr
	}
	m.vectors[id] = vectorEntry{vector: vector, metadata: metadata}
	return nil
}

func (m *mockVectorProvider) UpsertBatch(_ context.Context, vectors []shared.VectorRecord) error {
	if m.upsertErr != nil {
		return m.upsertErr
	}
	for _, v := range vectors {
		m.vectors[v.ID] = vectorEntry{vector: v.Vector, metadata: v.Metadata}
	}
	return nil
}

func (m *mockVectorProvider) Get(_ context.Context, id uuid.UUID) ([]float32, *shared.VectorInfo, error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	entry, ok := m.vectors[id]
	if !ok {
		return nil, nil, errVectorNotFound
	}
	return entry.vector, &shared.VectorInfo{
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
		return errVectorNotFound
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

func (m *mockVectorProvider) Search(_ context.Context, vector []float32, k int, filter map[string]any) ([]shared.VectorResult, error) {
	if m.searchErr != nil {
		return nil, m.searchErr
	}
	type scored struct {
		id       uuid.UUID
		entry    vectorEntry
		distance float32
	}
	scoredItems := make([]scored, 0, len(m.vectors))
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
		scoredItems = append(scoredItems, scored{id: id, entry: entry, distance: dist})
	}
	sort.Slice(scoredItems, func(i, j int) bool {
		return scoredItems[i].distance < scoredItems[j].distance
	})
	if k > 0 && len(scoredItems) > k {
		scoredItems = scoredItems[:k]
	}
	results := make([]shared.VectorResult, len(scoredItems))
	for i, s := range scoredItems {
		results[i] = shared.VectorResult{
			ID:       s.id,
			Vector:   s.entry.vector,
			Metadata: s.entry.metadata,
			Score:    s.distance,
		}
	}
	return results, nil
}

func (m *mockVectorProvider) Query(_ context.Context, vector []float32, k int, _ *vecna.Filter) ([]shared.VectorResult, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	// For testing, just do a simple search without filter evaluation
	type scored struct {
		id       uuid.UUID
		entry    vectorEntry
		distance float32
	}
	scoredItems := make([]scored, 0, len(m.vectors))
	for id, entry := range m.vectors {
		dist := l2Distance(vector, entry.vector)
		scoredItems = append(scoredItems, scored{id: id, entry: entry, distance: dist})
	}
	sort.Slice(scoredItems, func(i, j int) bool {
		return scoredItems[i].distance < scoredItems[j].distance
	})
	if k > 0 && len(scoredItems) > k {
		scoredItems = scoredItems[:k]
	}
	results := make([]shared.VectorResult, len(scoredItems))
	for i, s := range scoredItems {
		results[i] = shared.VectorResult{
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

func (m *mockVectorProvider) Filter(_ context.Context, _ *vecna.Filter, limit int) ([]shared.VectorResult, error) {
	if m.filterErr != nil {
		return nil, m.filterErr
	}
	// For testing, just return all vectors (no filter evaluation).
	results := make([]shared.VectorResult, 0, len(m.vectors))
	for id, entry := range m.vectors {
		results = append(results, shared.VectorResult{
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

type testVectorMetadata struct {
	Category string `json:"category" atom:"category"`
	Score    int64  `json:"score" atom:"score"`
}

// vectorJSONCodec implements Codec for testing.
type vectorJSONCodec struct{}

func (vectorJSONCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (vectorJSONCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func TestNewIndex(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()

	index := NewIndex[testVectorMetadata](provider, codec, spec)

	if index == nil {
		t.Fatal("NewIndex returned nil")
	}
	if index.provider != provider {
		t.Error("provider not set correctly")
	}
}

func TestIndex_Spec(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()

	index := NewIndex[testVectorMetadata](provider, codec, spec)

	returnedSpec := index.Spec()
	if returnedSpec.TypeName != spec.TypeName {
		t.Error("Spec returned incorrect value")
	}
}

func TestIndex_Get(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
	ctx := context.Background()

	t.Run("existing id", func(t *testing.T) {
		id := uuid.New()
		provider.vectors[id] = vectorEntry{
			vector:   []float32{1.0, 2.0, 3.0},
			metadata: []byte(`{"category": "test", "score": 42}`),
		}

		result, err := index.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if result == nil {
			t.Fatal("Get returned nil")
		}
		if result.ID != id {
			t.Errorf("unexpected ID: %s", result.ID)
		}
		if len(result.Vector) != 3 {
			t.Errorf("unexpected vector length: %d", len(result.Vector))
		}
		if result.Metadata.Strings["Category"] != "test" {
			t.Errorf("unexpected category: %v", result.Metadata.Strings["Category"])
		}
	})

	t.Run("missing id", func(t *testing.T) {
		_, err := index.Get(ctx, uuid.New())
		if err == nil {
			t.Error("expected error for missing id")
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

func TestIndex_Upsert(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
	ctx := context.Background()

	t.Run("basic upsert", func(t *testing.T) {
		id := uuid.New()
		metadata := testVectorMetadata{Category: "test", Score: 42}
		a := atomizer.Atomize(&metadata)

		err := index.Upsert(ctx, id, []float32{1.0, 2.0}, a)
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

		metadata := testVectorMetadata{Category: "fail", Score: 0}
		a := atomizer.Atomize(&metadata)

		err := index.Upsert(ctx, uuid.New(), []float32{1.0}, a)
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestIndex_Delete(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
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
		if err == nil {
			t.Error("expected error for missing id")
		}
	})
}

func TestIndex_Exists(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
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

func TestIndex_Search(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
	ctx := context.Background()

	// Setup test vectors
	id1 := uuid.New()
	id2 := uuid.New()
	provider.vectors[id1] = vectorEntry{
		vector:   []float32{1.0, 0.0, 0.0},
		metadata: []byte(`{"category": "a", "score": 1}`),
	}
	provider.vectors[id2] = vectorEntry{
		vector:   []float32{0.0, 1.0, 0.0},
		metadata: []byte(`{"category": "b", "score": 2}`),
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
	})

	t.Run("search with filter", func(t *testing.T) {
		query := []float32{0.5, 0.5, 0.0}
		filter := testVectorMetadata{Category: "a"}
		a := atomizer.Atomize(&filter)

		results, err := index.Search(ctx, query, 10, a)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		// Should only return category "a" vectors
		for _, r := range results {
			if r.Metadata.Strings["Category"] != "a" {
				t.Errorf("expected category 'a', got %q", r.Metadata.Strings["Category"])
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
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
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

	t.Run("metadata atomized", func(t *testing.T) {
		results, err := index.Query(ctx, []float32{1.0, 0.0}, 10, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		for _, r := range results {
			if r.Metadata == nil {
				t.Error("expected metadata to be atomized")
			}
			if r.Metadata.Strings["Category"] == "" {
				t.Error("expected Category to be populated")
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

func TestIndex_RoundTrip(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
	ctx := context.Background()

	id := uuid.New()
	original := testVectorMetadata{Category: "roundtrip", Score: 42}
	a := atomizer.Atomize(&original)
	vector := []float32{1.0, 2.0, 3.0}

	if err := index.Upsert(ctx, id, vector, a); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	retrieved, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Metadata.Strings["Category"] != original.Category {
		t.Errorf("Category mismatch: got %v, want %s", retrieved.Metadata.Strings["Category"], original.Category)
	}
	if retrieved.Metadata.Ints["Score"] != original.Score {
		t.Errorf("Score mismatch: got %v, want %d", retrieved.Metadata.Ints["Score"], original.Score)
	}
}

func TestIndex_Filter(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
	ctx := context.Background()

	// Insert test data.
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

	t.Run("metadata atomized", func(t *testing.T) {
		results, err := index.Filter(ctx, nil, 0)
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}
		for _, r := range results {
			if r.Metadata == nil {
				t.Error("expected metadata to be atomized")
			}
			if r.Metadata.Strings["Category"] == "" {
				t.Error("expected Category to be populated")
			}
		}
	})
}

func TestIndex_DecodeErrors(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
	ctx := context.Background()

	// Store invalid JSON that can't be decoded
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

// errorTestCodec is a codec that can be configured to fail.
type errorTestCodec struct {
	encodeErr error
	decodeErr error
}

func (f errorTestCodec) Encode(v any) ([]byte, error) {
	if f.encodeErr != nil {
		return nil, f.encodeErr
	}
	return json.Marshal(v)
}

func (f errorTestCodec) Decode(data []byte, v any) error {
	if f.decodeErr != nil {
		return f.decodeErr
	}
	return json.Unmarshal(data, v)
}

func TestIndex_EncodeErrors(t *testing.T) {
	provider := newMockVectorProvider()
	codec := errorTestCodec{encodeErr: errors.New("encode failed")}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
	ctx := context.Background()

	t.Run("Upsert encode error", func(t *testing.T) {
		metadata := testVectorMetadata{Category: "test", Score: 42}
		a := atomizer.Atomize(&metadata)
		err := index.Upsert(ctx, uuid.New(), []float32{1.0}, a)
		if err == nil {
			t.Error("expected encode error")
		}
	})

	t.Run("Search atomToFilter encode error", func(t *testing.T) {
		filter := testVectorMetadata{Category: "test"}
		a := atomizer.Atomize(&filter)
		_, err := index.Search(ctx, []float32{1.0}, 10, a)
		if err == nil {
			t.Error("expected encode error")
		}
	})
}

func TestIndex_AtomToFilterDecodeError(t *testing.T) {
	provider := newMockVectorProvider()
	// Codec that encodes fine but fails on decode (for atomToFilter's second step)
	codec := errorTestCodec{decodeErr: errors.New("decode failed")}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
	ctx := context.Background()

	filter := testVectorMetadata{Category: "test"}
	a := atomizer.Atomize(&filter)
	_, err := index.Search(ctx, []float32{1.0}, 10, a)
	if err == nil {
		t.Error("expected decode error in atomToFilter")
	}
}

func TestIndex_FilterProviderError(t *testing.T) {
	provider := newMockVectorProvider()
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
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
	codec := vectorJSONCodec{}
	atomizer, _ := atom.Use[testVectorMetadata]()
	spec := atomizer.Spec()
	index := NewIndex[testVectorMetadata](provider, codec, spec)
	ctx := context.Background()

	t.Run("Upsert with nil atom", func(t *testing.T) {
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
		// nil metadata should return nil atom
		if result.Metadata != nil {
			t.Error("expected nil metadata atom")
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

	t.Run("Filter with nil metadata in results", func(t *testing.T) {
		id := uuid.New()
		provider.vectors[id] = vectorEntry{
			vector:   []float32{1.0},
			metadata: nil,
		}
		results, err := index.Filter(ctx, nil, 0)
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}
		if len(results) == 0 {
			t.Error("expected results")
		}
	})

	t.Run("Search with nil filter atom", func(t *testing.T) {
		id := uuid.New()
		provider.vectors[id] = vectorEntry{
			vector:   []float32{1.0, 0.0},
			metadata: []byte(`{"category":"test","score":1}`),
		}
		results, err := index.Search(ctx, []float32{1.0, 0.0}, 10, nil)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(results) == 0 {
			t.Error("expected results")
		}
	})
}
