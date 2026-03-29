package grub

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/zoobz-io/lucene"
)

// mockSearchProvider implements SearchProvider for testing.
type mockSearchProvider struct {
	docs        map[string]map[string][]byte // index -> id -> doc
	indexErr    error
	getErr      error
	deleteErr   error
	existsErr   error
	searchErr   error
	countErr    error
	refreshErr  error
	searchResp  *SearchResponse
	countResult int64
}

func newMockSearchProvider() *mockSearchProvider {
	return &mockSearchProvider{
		docs: make(map[string]map[string][]byte),
	}
}

func (m *mockSearchProvider) ensureIndex(index string) {
	if m.docs[index] == nil {
		m.docs[index] = make(map[string][]byte)
	}
}

func (m *mockSearchProvider) Index(_ context.Context, index, id string, doc []byte) error {
	if m.indexErr != nil {
		return m.indexErr
	}
	m.ensureIndex(index)
	m.docs[index][id] = doc
	return nil
}

func (m *mockSearchProvider) IndexBatch(_ context.Context, index string, docs map[string][]byte) error {
	if m.indexErr != nil {
		return m.indexErr
	}
	m.ensureIndex(index)
	for id, doc := range docs {
		m.docs[index][id] = doc
	}
	return nil
}

func (m *mockSearchProvider) Get(_ context.Context, index, id string) ([]byte, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.ensureIndex(index)
	doc, ok := m.docs[index][id]
	if !ok {
		return nil, ErrNotFound
	}
	return doc, nil
}

func (m *mockSearchProvider) Delete(_ context.Context, index, id string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	m.ensureIndex(index)
	if _, ok := m.docs[index][id]; !ok {
		return ErrNotFound
	}
	delete(m.docs[index], id)
	return nil
}

func (m *mockSearchProvider) DeleteBatch(_ context.Context, index string, ids []string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	m.ensureIndex(index)
	for _, id := range ids {
		delete(m.docs[index], id)
	}
	return nil
}

func (m *mockSearchProvider) Exists(_ context.Context, index, id string) (bool, error) {
	if m.existsErr != nil {
		return false, m.existsErr
	}
	m.ensureIndex(index)
	_, ok := m.docs[index][id]
	return ok, nil
}

func (m *mockSearchProvider) Search(_ context.Context, index string, _ *lucene.Search) (*SearchResponse, error) {
	if m.searchErr != nil {
		return nil, m.searchErr
	}
	if m.searchResp != nil {
		return m.searchResp, nil
	}
	// Return all docs in the index as search results
	m.ensureIndex(index)
	hits := make([]SearchHit, 0, len(m.docs[index]))
	for id, doc := range m.docs[index] {
		hits = append(hits, SearchHit{
			ID:     id,
			Source: doc,
			Score:  1.0,
		})
	}
	return &SearchResponse{
		Hits:     hits,
		Total:    int64(len(hits)),
		MaxScore: 1.0,
	}, nil
}

func (m *mockSearchProvider) Count(_ context.Context, index string, _ lucene.Query) (int64, error) {
	if m.countErr != nil {
		return 0, m.countErr
	}
	if m.countResult > 0 {
		return m.countResult, nil
	}
	m.ensureIndex(index)
	return int64(len(m.docs[index])), nil
}

func (m *mockSearchProvider) Refresh(_ context.Context, _ string) error {
	return m.refreshErr
}

type testProduct struct {
	Title    string  `json:"title" atom:"title"`
	Price    float64 `json:"price" atom:"price"`
	Category string  `json:"category" atom:"category"`
}

func TestNewSearch(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")

	if search == nil {
		t.Fatal("NewSearch returned nil")
	}
	if search.provider != provider {
		t.Error("provider not set correctly")
	}
	if search.index != "products" {
		t.Errorf("index not set correctly: got %q", search.index)
	}
	if search.codec == nil {
		t.Error("codec should default to JSONCodec")
	}
	if search.builder == nil {
		t.Error("builder should be initialized")
	}
}

func TestNewSearchWithCodec(t *testing.T) {
	provider := newMockSearchProvider()
	codec := GobCodec{}
	search := NewSearchWithCodec[testProduct](provider, "products", codec)

	if search == nil {
		t.Fatal("NewSearchWithCodec returned nil")
	}
}

func TestSearch_Index(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	t.Run("basic index", func(t *testing.T) {
		doc := &testProduct{Title: "Running Shoes", Price: 99.99, Category: "footwear"}
		err := search.Index(ctx, "prod-1", doc)
		if err != nil {
			t.Fatalf("Index failed: %v", err)
		}

		if _, ok := provider.docs["products"]["prod-1"]; !ok {
			t.Error("document not stored in provider")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.indexErr = errors.New("index error")
		defer func() { provider.indexErr = nil }()

		doc := &testProduct{Title: "Fail", Price: 0}
		err := search.Index(ctx, "fail-1", doc)
		if err == nil {
			t.Error("expected provider error")
		}
	})

	t.Run("encode error", func(t *testing.T) {
		codec := &failingCodec{encodeErr: errors.New("encode failed")}
		s := NewSearchWithCodec[testProduct](provider, "products", codec)

		doc := &testProduct{Title: "Encode Fail"}
		err := s.Index(ctx, "enc-fail", doc)
		if err == nil {
			t.Error("expected encode error")
		}
	})
}

func TestSearch_IndexBatch(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	t.Run("basic batch", func(t *testing.T) {
		docs := map[string]*testProduct{
			"b1": {Title: "Product 1", Price: 10.0},
			"b2": {Title: "Product 2", Price: 20.0},
		}
		err := search.IndexBatch(ctx, docs)
		if err != nil {
			t.Fatalf("IndexBatch failed: %v", err)
		}

		if len(provider.docs["products"]) < 2 {
			t.Error("batch items not stored")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.indexErr = errors.New("batch error")
		defer func() { provider.indexErr = nil }()

		docs := map[string]*testProduct{
			"fail": {Title: "Fail"},
		}
		err := search.IndexBatch(ctx, docs)
		if err == nil {
			t.Error("expected provider error")
		}
	})

	t.Run("encode error", func(t *testing.T) {
		codec := &failingCodec{encodeErr: errors.New("encode failed")}
		s := NewSearchWithCodec[testProduct](provider, "products", codec)

		docs := map[string]*testProduct{
			"fail": {Title: "Encode Fail"},
		}
		err := s.IndexBatch(ctx, docs)
		if err == nil {
			t.Error("expected encode error")
		}
	})
}

func TestSearch_Get(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	t.Run("existing id", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["get-1"] = []byte(`{"title":"Test Product","price":49.99,"category":"test"}`)

		doc, err := search.Get(ctx, "get-1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if doc.ID != "get-1" {
			t.Errorf("expected ID 'get-1', got %q", doc.ID)
		}
		if doc.Content.Title != "Test Product" {
			t.Errorf("expected Title 'Test Product', got %q", doc.Content.Title)
		}
		if doc.Content.Price != 49.99 {
			t.Errorf("expected Price 49.99, got %f", doc.Content.Price)
		}
	})

	t.Run("missing id", func(t *testing.T) {
		_, err := search.Get(ctx, "nonexistent")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["bad"] = []byte(`{invalid}`)

		_, err := search.Get(ctx, "bad")
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.getErr = errors.New("get error")
		defer func() { provider.getErr = nil }()

		_, err := search.Get(ctx, "get-1")
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestSearch_Delete(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	t.Run("existing id", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["delete-me"] = []byte(`{}`)

		err := search.Delete(ctx, "delete-me")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if _, ok := provider.docs["products"]["delete-me"]; ok {
			t.Error("document should have been deleted")
		}
	})

	t.Run("missing id", func(t *testing.T) {
		err := search.Delete(ctx, "nonexistent")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestSearch_DeleteBatch(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	provider.ensureIndex("products")
	provider.docs["products"]["d1"] = []byte(`{}`)
	provider.docs["products"]["d2"] = []byte(`{}`)
	provider.docs["products"]["d3"] = []byte(`{}`)

	err := search.DeleteBatch(ctx, []string{"d1", "d2", "nonexistent"})
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}

	if _, ok := provider.docs["products"]["d1"]; ok {
		t.Error("d1 should have been deleted")
	}
	if _, ok := provider.docs["products"]["d2"]; ok {
		t.Error("d2 should have been deleted")
	}
	if _, ok := provider.docs["products"]["d3"]; !ok {
		t.Error("d3 should still exist")
	}
}

func TestSearch_Exists(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	provider.ensureIndex("products")
	provider.docs["products"]["exists"] = []byte(`{}`)

	t.Run("existing id", func(t *testing.T) {
		exists, err := search.Exists(ctx, "exists")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("expected document to exist")
		}
	})

	t.Run("missing id", func(t *testing.T) {
		exists, err := search.Exists(ctx, "missing")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected document to not exist")
		}
	})
}

func TestSearch_Query(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")

	builder := search.Query()
	if builder == nil {
		t.Fatal("Query() returned nil builder")
	}

	// Verify the builder can create queries
	query := builder.Match("title", "shoes")
	if query == nil {
		t.Fatal("builder.Match returned nil")
	}
	if query.Err() != nil {
		t.Errorf("query has error: %v", query.Err())
	}
}

func TestSearch_Execute(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	t.Run("basic search", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["s1"] = []byte(`{"title":"Shoes","price":50.0,"category":"footwear"}`)
		provider.docs["products"]["s2"] = []byte(`{"title":"Shirt","price":30.0,"category":"apparel"}`)

		s := lucene.NewSearch().Size(10)
		result, err := search.Execute(ctx, s)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected 2 total hits, got %d", result.Total)
		}
		if len(result.Hits) != 2 {
			t.Errorf("expected 2 hits, got %d", len(result.Hits))
		}
	})

	t.Run("with query", func(t *testing.T) {
		provider.searchResp = &SearchResponse{
			Hits: []SearchHit{
				{ID: "s1", Source: []byte(`{"title":"Shoes","price":50.0,"category":"footwear"}`), Score: 1.5},
			},
			Total:    1,
			MaxScore: 1.5,
		}
		defer func() { provider.searchResp = nil }()

		q := search.Query()
		s := lucene.NewSearch().Query(q.Match("title", "shoes"))
		result, err := search.Execute(ctx, s)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
		if result.Total != 1 {
			t.Errorf("expected 1 hit, got %d", result.Total)
		}
		if result.Hits[0].Content.Title != "Shoes" {
			t.Errorf("expected title 'Shoes', got %q", result.Hits[0].Content.Title)
		}
		if result.Hits[0].Score != 1.5 {
			t.Errorf("expected score 1.5, got %f", result.Hits[0].Score)
		}
	})

	t.Run("with aggregations", func(t *testing.T) {
		provider.searchResp = &SearchResponse{
			Hits:     []SearchHit{},
			Total:    0,
			MaxScore: 0,
			Aggregations: map[string]any{
				"categories": map[string]any{
					"buckets": []any{
						map[string]any{"key": "footwear", "doc_count": 10},
						map[string]any{"key": "apparel", "doc_count": 5},
					},
				},
			},
			TypedAggs: []AggResult{
				{
					Name: "categories",
					Type: lucene.AggTerms,
					Buckets: []AggBucket{
						{Key: "footwear", DocCount: 10},
						{Key: "apparel", DocCount: 5},
					},
					Raw: map[string]any{
						"buckets": []any{
							map[string]any{"key": "footwear", "doc_count": 10},
							map[string]any{"key": "apparel", "doc_count": 5},
						},
					},
				},
			},
		}
		defer func() { provider.searchResp = nil }()

		q := search.Query()
		s := lucene.NewSearch().
			Query(q.MatchAll()).
			Aggs(q.TermsAgg("categories", "category"))

		result, err := search.Execute(ctx, s)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
		if result.Aggregations == nil {
			t.Error("expected aggregations")
		}
		if _, ok := result.Aggregations["categories"]; !ok {
			t.Error("expected categories aggregation")
		}

		// Verify typed aggregations pass through
		if result.TypedAggs == nil {
			t.Fatal("expected TypedAggs to be populated")
		}
		agg := result.Agg("categories")
		if agg == nil {
			t.Fatal("expected Agg('categories') to return result")
		}
		if len(agg.Buckets) != 2 {
			t.Fatalf("expected 2 buckets, got %d", len(agg.Buckets))
		}
		if agg.Buckets[0].Key != "footwear" {
			t.Errorf("expected key 'footwear', got %q", agg.Buckets[0].Key)
		}
		if agg.Buckets[0].DocCount != 10 {
			t.Errorf("expected doc_count 10, got %d", agg.Buckets[0].DocCount)
		}

		// Verify Agg() returns nil for non-existent name
		if result.Agg("nonexistent") != nil {
			t.Error("expected Agg('nonexistent') to return nil")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.searchErr = errors.New("search error")
		defer func() { provider.searchErr = nil }()

		s := lucene.NewSearch()
		_, err := search.Execute(ctx, s)
		if err == nil {
			t.Error("expected provider error")
		}
	})

	t.Run("decode error", func(t *testing.T) {
		provider.searchResp = &SearchResponse{
			Hits: []SearchHit{
				{ID: "bad", Source: []byte(`{invalid json`), Score: 1.0},
			},
			Total: 1,
		}
		defer func() { provider.searchResp = nil }()

		s := lucene.NewSearch()
		_, err := search.Execute(ctx, s)
		if err == nil {
			t.Error("expected decode error")
		}
	})
}

func TestSearch_Count(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	t.Run("count all", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["c1"] = []byte(`{}`)
		provider.docs["products"]["c2"] = []byte(`{}`)
		provider.docs["products"]["c3"] = []byte(`{}`)

		count, err := search.Count(ctx, nil)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 3 {
			t.Errorf("expected count 3, got %d", count)
		}
	})

	t.Run("count with query", func(t *testing.T) {
		provider.countResult = 5
		defer func() { provider.countResult = 0 }()

		q := search.Query()
		count, err := search.Count(ctx, q.Match("title", "shoes"))
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 5 {
			t.Errorf("expected count 5, got %d", count)
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.countErr = errors.New("count error")
		defer func() { provider.countErr = nil }()

		_, err := search.Count(ctx, nil)
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestSearch_Refresh(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		err := search.Refresh(ctx)
		if err != nil {
			t.Fatalf("Refresh failed: %v", err)
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.refreshErr = errors.New("refresh error")
		defer func() { provider.refreshErr = nil }()

		err := search.Refresh(ctx)
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestSearch_RoundTrip(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	original := &testProduct{
		Title:    "Round Trip Product",
		Price:    123.45,
		Category: "test",
	}

	if err := search.Index(ctx, "rt-1", original); err != nil {
		t.Fatalf("Index failed: %v", err)
	}

	retrieved, err := search.Get(ctx, "rt-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Content.Title != original.Title {
		t.Errorf("Title mismatch: got %q, want %q", retrieved.Content.Title, original.Title)
	}
	if retrieved.Content.Price != original.Price {
		t.Errorf("Price mismatch: got %f, want %f", retrieved.Content.Price, original.Price)
	}
	if retrieved.Content.Category != original.Category {
		t.Errorf("Category mismatch: got %q, want %q", retrieved.Content.Category, original.Category)
	}
}

func TestSearch_Atomic(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	provider.ensureIndex("products")
	provider.docs["products"]["atomic-1"] = []byte(`{"title":"Atomic Product","price":42.0,"category":"atomic"}`)

	atomic := search.Atomic()
	if atomic == nil {
		t.Fatal("Atomic returned nil")
	}

	// Verify it returns the same instance
	atomic2 := search.Atomic()
	if atomic != atomic2 {
		t.Error("Atomic should return cached instance")
	}

	// Test that atomic view works
	doc, err := atomic.Get(ctx, "atomic-1")
	if err != nil {
		t.Fatalf("Atomic Get failed: %v", err)
	}
	if doc.Content.Strings["Title"] != "Atomic Product" {
		t.Errorf("unexpected Title: %q", doc.Content.Strings["Title"])
	}
}

// hookedProduct tests lifecycle hooks.
type hookedProduct struct {
	Title         string `json:"title"`
	beforeSaveCnt int
	afterSaveCnt  int
	afterLoadCnt  int
}

func (h *hookedProduct) BeforeSave(_ context.Context) error {
	h.beforeSaveCnt++
	return nil
}

func (h *hookedProduct) AfterSave(_ context.Context) error {
	h.afterSaveCnt++
	return nil
}

func (h *hookedProduct) AfterLoad(_ context.Context) error {
	h.afterLoadCnt++
	return nil
}

func TestSearch_Hooks(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[hookedProduct](provider, "products")
	ctx := context.Background()

	t.Run("Index calls hooks", func(t *testing.T) {
		doc := &hookedProduct{Title: "Hooked"}
		err := search.Index(ctx, "hook-1", doc)
		if err != nil {
			t.Fatalf("Index failed: %v", err)
		}
		if doc.beforeSaveCnt != 1 {
			t.Errorf("expected BeforeSave called once, got %d", doc.beforeSaveCnt)
		}
		if doc.afterSaveCnt != 1 {
			t.Errorf("expected AfterSave called once, got %d", doc.afterSaveCnt)
		}
	})

	t.Run("Get calls AfterLoad", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["hook-get"] = []byte(`{"title":"Load Hook"}`)

		doc, err := search.Get(ctx, "hook-get")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if doc.Content.afterLoadCnt != 1 {
			t.Errorf("expected AfterLoad called once, got %d", doc.Content.afterLoadCnt)
		}
	})

	t.Run("Execute calls AfterLoad for each hit", func(t *testing.T) {
		provider.searchResp = &SearchResponse{
			Hits: []SearchHit{
				{ID: "h1", Source: []byte(`{"title":"Hit 1"}`), Score: 1.0},
				{ID: "h2", Source: []byte(`{"title":"Hit 2"}`), Score: 0.9},
			},
			Total: 2,
		}
		defer func() { provider.searchResp = nil }()

		s := lucene.NewSearch()
		result, err := search.Execute(ctx, s)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
		for i, hit := range result.Hits {
			if hit.Content.afterLoadCnt != 1 {
				t.Errorf("hit %d: expected AfterLoad called once, got %d", i, hit.Content.afterLoadCnt)
			}
		}
	})
}

// failingHookProduct tests BeforeSave hook failure.
type failingHookProduct struct {
	Title string `json:"title"`
}

func (*failingHookProduct) BeforeSave(_ context.Context) error {
	return errors.New("before save failed")
}

// failingAfterSaveProduct tests AfterSave hook failure.
type failingAfterSaveProduct struct {
	Title string `json:"title" atom:"title"`
}

func (*failingAfterSaveProduct) AfterSave(_ context.Context) error {
	return errors.New("after save failed")
}

// failingAfterLoadProduct tests AfterLoad hook failure.
type failingAfterLoadProduct struct {
	Title string `json:"title" atom:"title"`
}

func (*failingAfterLoadProduct) AfterLoad(_ context.Context) error {
	return errors.New("after load failed")
}

// failingBeforeDeleteProduct tests BeforeDelete hook failure.
type failingBeforeDeleteProduct struct {
	Title string `json:"title" atom:"title"`
}

func (*failingBeforeDeleteProduct) BeforeDelete(_ context.Context) error {
	return errors.New("before delete failed")
}

// failingAfterDeleteProduct tests AfterDelete hook failure.
type failingAfterDeleteProduct struct {
	Title string `json:"title" atom:"title"`
}

func (*failingAfterDeleteProduct) AfterDelete(_ context.Context) error {
	return errors.New("after delete failed")
}

func TestSearch_FailingBeforeSaveHook(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[failingHookProduct](provider, "products")
	ctx := context.Background()

	t.Run("Index BeforeSave error", func(t *testing.T) {
		doc := &failingHookProduct{Title: "Fail"}
		err := search.Index(ctx, "fail-1", doc)
		if err == nil {
			t.Error("expected BeforeSave error")
		}

		// Verify document was not stored
		if _, ok := provider.docs["products"]["fail-1"]; ok {
			t.Error("document should not have been stored after BeforeSave failure")
		}
	})

	t.Run("IndexBatch BeforeSave error", func(t *testing.T) {
		docs := map[string]*failingHookProduct{
			"batch-fail": {Title: "Batch Fail"},
		}
		err := search.IndexBatch(ctx, docs)
		if err == nil {
			t.Error("expected BeforeSave error in batch")
		}
	})
}

func TestSearch_FailingAfterSaveHook(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[failingAfterSaveProduct](provider, "products")
	ctx := context.Background()

	t.Run("Index AfterSave error", func(t *testing.T) {
		doc := &failingAfterSaveProduct{Title: "Fail"}
		err := search.Index(ctx, "fail-after", doc)
		if err == nil {
			t.Error("expected AfterSave error")
		}
	})

	t.Run("IndexBatch AfterSave error", func(t *testing.T) {
		docs := map[string]*failingAfterSaveProduct{
			"batch-1": {Title: "Batch Fail"},
		}
		err := search.IndexBatch(ctx, docs)
		if err == nil {
			t.Error("expected AfterSave error in batch")
		}
	})
}

func TestSearch_FailingAfterLoadHook(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[failingAfterLoadProduct](provider, "products")
	ctx := context.Background()

	t.Run("Get AfterLoad error", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["load-fail"] = []byte(`{"title":"Load Fail"}`)

		_, err := search.Get(ctx, "load-fail")
		if err == nil {
			t.Error("expected AfterLoad error")
		}
	})

	t.Run("Execute AfterLoad error", func(t *testing.T) {
		provider.searchResp = &SearchResponse{
			Hits: []SearchHit{
				{ID: "exec-fail", Source: []byte(`{"title":"Execute Fail"}`), Score: 1.0},
			},
			Total: 1,
		}
		defer func() { provider.searchResp = nil }()

		s := lucene.NewSearch()
		_, err := search.Execute(ctx, s)
		if err == nil {
			t.Error("expected AfterLoad error in Execute")
		}
	})
}

func TestSearch_FailingBeforeDeleteHook(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[failingBeforeDeleteProduct](provider, "products")
	ctx := context.Background()

	provider.ensureIndex("products")
	provider.docs["products"]["del-1"] = []byte(`{}`)

	t.Run("Delete BeforeDelete error", func(t *testing.T) {
		err := search.Delete(ctx, "del-1")
		if err == nil {
			t.Error("expected BeforeDelete error")
		}
		// Document should still exist
		if _, ok := provider.docs["products"]["del-1"]; !ok {
			t.Error("document should not have been deleted after BeforeDelete failure")
		}
	})

	t.Run("DeleteBatch BeforeDelete error", func(t *testing.T) {
		err := search.DeleteBatch(ctx, []string{"del-1"})
		if err == nil {
			t.Error("expected BeforeDelete error in batch")
		}
	})
}

func TestSearch_FailingAfterDeleteHook(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[failingAfterDeleteProduct](provider, "products")
	ctx := context.Background()

	t.Run("Delete AfterDelete error", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["del-after-1"] = []byte(`{}`)

		err := search.Delete(ctx, "del-after-1")
		if err == nil {
			t.Error("expected AfterDelete error")
		}
	})

	t.Run("DeleteBatch AfterDelete error", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["del-after-2"] = []byte(`{}`)

		err := search.DeleteBatch(ctx, []string{"del-after-2"})
		if err == nil {
			t.Error("expected AfterDelete error in batch")
		}
	})
}

func TestSearch_DeleteBatch_ProviderError(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	provider.deleteErr = errors.New("batch delete error")
	defer func() { provider.deleteErr = nil }()

	err := search.DeleteBatch(ctx, []string{"d1", "d2"})
	if err == nil {
		t.Error("expected provider error")
	}
}

func TestSearch_Count_QueryError(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	// Create a query with an error
	q := search.Query()
	invalidQuery := q.Match("nonexistent_field", "value")

	_, err := search.Count(ctx, invalidQuery)
	if err == nil {
		t.Error("expected query error for invalid field")
	}
}

// TestSearch_ExecuteQueryError tests that query errors propagate.
func TestSearch_ExecuteQueryError(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	// Create a query with an error (invalid field).
	q := search.Query()
	invalidQuery := q.Match("nonexistent_field", "value")
	s := lucene.NewSearch().Query(invalidQuery)

	// The query should have an error due to invalid field.
	_, err := search.Execute(ctx, s)
	if err == nil {
		t.Error("expected error for invalid field query")
	}
}

func TestSearch_IndexBatchEmpty(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	// Empty batch should succeed
	err := search.IndexBatch(ctx, map[string]*testProduct{})
	if err != nil {
		t.Errorf("empty IndexBatch should succeed, got: %v", err)
	}
}

func TestSearch_DeleteBatchEmpty(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	// Empty batch should succeed
	err := search.DeleteBatch(ctx, []string{})
	if err != nil {
		t.Errorf("empty DeleteBatch should succeed, got: %v", err)
	}
}

func TestSearch_ExecuteNilSearch(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	// Test with minimal search request
	s := lucene.NewSearch()
	provider.searchResp = &SearchResponse{
		Hits:  []SearchHit{},
		Total: 0,
	}

	result, err := search.Execute(ctx, s)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

// TestSearch_JSONRoundTrip verifies JSON encoding preserves data.
func TestSearch_JSONRoundTrip(t *testing.T) {
	provider := newMockSearchProvider()
	search := NewSearch[testProduct](provider, "products")
	ctx := context.Background()

	original := &testProduct{
		Title:    "Special \"Quoted\" Product",
		Price:    999.99,
		Category: "unicode: こんにちは",
	}

	if err := search.Index(ctx, "json-rt", original); err != nil {
		t.Fatalf("Index failed: %v", err)
	}

	// Verify stored JSON is valid
	stored := provider.docs["products"]["json-rt"]
	var parsed testProduct
	if err := json.Unmarshal(stored, &parsed); err != nil {
		t.Fatalf("stored JSON invalid: %v", err)
	}

	if parsed.Title != original.Title {
		t.Errorf("Title mismatch: %q vs %q", parsed.Title, original.Title)
	}
	if parsed.Category != original.Category {
		t.Errorf("Category mismatch: %q vs %q", parsed.Category, original.Category)
	}
}

func TestNewSearch_BuilderPanic(t *testing.T) {
	provider := newMockSearchProvider()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for non-struct type")
		}
	}()
	NewSearch[int](provider, "test")
}

func TestNewSearchWithCodec_BuilderPanic(t *testing.T) {
	provider := newMockSearchProvider()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for non-struct type")
		}
	}()
	NewSearchWithCodec[int](provider, "test", JSONCodec{})
}
