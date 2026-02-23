package atomic

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/zoobzio/atom"
	"github.com/zoobzio/grub/internal/shared"
	"github.com/zoobzio/lucene"
)

// mockSearchProvider implements SearchProvider for testing.
type mockSearchProvider struct {
	docs       map[string]map[string][]byte // index -> id -> doc
	indexErr   error
	getErr     error
	deleteErr  error
	existsErr  error
	searchErr  error
	countErr   error
	refreshErr error
	searchResp *shared.SearchResponse
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

var errSearchNotFound = errors.New("not found")

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
		return nil, errSearchNotFound
	}
	return doc, nil
}

func (m *mockSearchProvider) Delete(_ context.Context, index, id string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	m.ensureIndex(index)
	if _, ok := m.docs[index][id]; !ok {
		return errSearchNotFound
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

func (m *mockSearchProvider) Search(_ context.Context, index string, _ *lucene.Search) (*shared.SearchResponse, error) {
	if m.searchErr != nil {
		return nil, m.searchErr
	}
	if m.searchResp != nil {
		return m.searchResp, nil
	}
	m.ensureIndex(index)
	hits := make([]shared.SearchHit, 0, len(m.docs[index]))
	for id, doc := range m.docs[index] {
		hits = append(hits, shared.SearchHit{
			ID:     id,
			Source: doc,
			Score:  1.0,
		})
	}
	return &shared.SearchResponse{
		Hits:     hits,
		Total:    int64(len(hits)),
		MaxScore: 1.0,
	}, nil
}

func (m *mockSearchProvider) Count(_ context.Context, index string, _ lucene.Query) (int64, error) {
	if m.countErr != nil {
		return 0, m.countErr
	}
	m.ensureIndex(index)
	return int64(len(m.docs[index])), nil
}

func (m *mockSearchProvider) Refresh(_ context.Context, _ string) error {
	return m.refreshErr
}

type testDocument struct {
	Title    string  `json:"title" atom:"title"`
	Price    float64 `json:"price" atom:"price"`
	Category string  `json:"category" atom:"category"`
}

func TestNewSearch(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, err := atom.Use[testDocument]()
	if err != nil {
		t.Fatalf("failed to create atomizer: %v", err)
	}

	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())

	if search == nil {
		t.Fatal("NewSearch returned nil")
	}
	if search.provider != provider {
		t.Error("provider not set")
	}
	if search.indexName != "products" {
		t.Errorf("indexName: got %q, want %q", search.indexName, "products")
	}
}

func TestSearch_Index(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())

	if search.Index() != "products" {
		t.Errorf("Index(): got %q, want %q", search.Index(), "products")
	}
}

func TestSearch_Spec(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())

	spec := search.Spec()
	if spec.TypeName != "testDocument" {
		t.Errorf("Spec().TypeName: got %q, want %q", spec.TypeName, "testDocument")
	}
}

func TestSearch_Get(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())
	ctx := context.Background()

	t.Run("existing document", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["doc-1"] = []byte(`{"title":"Test Product","price":99.99,"category":"test"}`)

		doc, err := search.Get(ctx, "doc-1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if doc.ID != "doc-1" {
			t.Errorf("ID: got %q, want %q", doc.ID, "doc-1")
		}
		if doc.Content == nil {
			t.Fatal("Content is nil")
		}
		if doc.Content.Strings["Title"] != "Test Product" {
			t.Errorf("Title: got %q, want %q", doc.Content.Strings["Title"], "Test Product")
		}
		if doc.Content.Floats["Price"] != 99.99 {
			t.Errorf("Price: got %f, want %f", doc.Content.Floats["Price"], 99.99)
		}
	})

	t.Run("missing document", func(t *testing.T) {
		_, err := search.Get(ctx, "nonexistent")
		if err == nil {
			t.Error("expected error for missing document")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.getErr = errors.New("get error")
		defer func() { provider.getErr = nil }()

		_, err := search.Get(ctx, "doc-1")
		if err == nil {
			t.Error("expected provider error")
		}
	})

	t.Run("decode error", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["bad-json"] = []byte(`{invalid}`)

		_, err := search.Get(ctx, "bad-json")
		if err == nil {
			t.Error("expected decode error")
		}
	})
}

func TestSearch_IndexDoc(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		doc := &testDocument{Title: "New Product", Price: 49.99, Category: "new"}
		a := atomizer.Atomize(doc)

		err := search.IndexDoc(ctx, "new-doc", a)
		if err != nil {
			t.Fatalf("IndexDoc failed: %v", err)
		}

		// Verify stored
		if _, ok := provider.docs["products"]["new-doc"]; !ok {
			t.Error("document not stored")
		}
	})

	t.Run("nil atom", func(t *testing.T) {
		err := search.IndexDoc(ctx, "nil-doc", nil)
		if err != nil {
			t.Fatalf("IndexDoc with nil should succeed: %v", err)
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.indexErr = errors.New("index error")
		defer func() { provider.indexErr = nil }()

		doc := &testDocument{Title: "Fail"}
		a := atomizer.Atomize(doc)

		err := search.IndexDoc(ctx, "fail-doc", a)
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestSearch_Delete(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())
	ctx := context.Background()

	t.Run("existing document", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["del-1"] = []byte(`{}`)

		err := search.Delete(ctx, "del-1")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if _, ok := provider.docs["products"]["del-1"]; ok {
			t.Error("document should have been deleted")
		}
	})

	t.Run("missing document", func(t *testing.T) {
		err := search.Delete(ctx, "nonexistent")
		if err == nil {
			t.Error("expected error for missing document")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.deleteErr = errors.New("delete error")
		defer func() { provider.deleteErr = nil }()

		provider.ensureIndex("products")
		provider.docs["products"]["err-doc"] = []byte(`{}`)

		err := search.Delete(ctx, "err-doc")
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestSearch_Exists(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())
	ctx := context.Background()

	provider.ensureIndex("products")
	provider.docs["products"]["exists-1"] = []byte(`{}`)

	t.Run("existing document", func(t *testing.T) {
		exists, err := search.Exists(ctx, "exists-1")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("expected document to exist")
		}
	})

	t.Run("missing document", func(t *testing.T) {
		exists, err := search.Exists(ctx, "missing")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected document to not exist")
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.existsErr = errors.New("exists error")
		defer func() { provider.existsErr = nil }()

		_, err := search.Exists(ctx, "exists-1")
		if err == nil {
			t.Error("expected provider error")
		}
	})
}

func TestSearch_Search(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())
	ctx := context.Background()

	t.Run("basic search", func(t *testing.T) {
		provider.ensureIndex("products")
		provider.docs["products"]["s1"] = []byte(`{"title":"Shoes","price":50.0,"category":"footwear"}`)
		provider.docs["products"]["s2"] = []byte(`{"title":"Shirt","price":30.0,"category":"apparel"}`)

		s := lucene.NewSearch().Size(10)
		results, err := search.Search(ctx, s)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}

		// Verify atomization
		for _, doc := range results {
			if doc.Content == nil {
				t.Error("Content is nil")
			}
			if doc.Score != 1.0 {
				t.Errorf("expected score 1.0, got %f", doc.Score)
			}
		}
	})

	t.Run("provider error", func(t *testing.T) {
		provider.searchErr = errors.New("search error")
		defer func() { provider.searchErr = nil }()

		s := lucene.NewSearch()
		_, err := search.Search(ctx, s)
		if err == nil {
			t.Error("expected provider error")
		}
	})

	t.Run("decode error in results", func(t *testing.T) {
		provider.searchResp = &shared.SearchResponse{
			Hits: []shared.SearchHit{
				{ID: "bad", Source: []byte(`{invalid json`), Score: 1.0},
			},
			Total: 1,
		}
		defer func() { provider.searchResp = nil }()

		s := lucene.NewSearch()
		_, err := search.Search(ctx, s)
		if err == nil {
			t.Error("expected decode error")
		}
	})
}

func TestSearch_RoundTrip(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())
	ctx := context.Background()

	original := &testDocument{
		Title:    "Round Trip Product",
		Price:    123.45,
		Category: "test",
	}
	a := atomizer.Atomize(original)

	// Index
	if err := search.IndexDoc(ctx, "rt-1", a); err != nil {
		t.Fatalf("IndexDoc failed: %v", err)
	}

	// Retrieve
	doc, err := search.Get(ctx, "rt-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Verify
	if doc.Content.Strings["Title"] != original.Title {
		t.Errorf("Title mismatch: got %q, want %q", doc.Content.Strings["Title"], original.Title)
	}
	if doc.Content.Floats["Price"] != original.Price {
		t.Errorf("Price mismatch: got %f, want %f", doc.Content.Floats["Price"], original.Price)
	}
	if doc.Content.Strings["Category"] != original.Category {
		t.Errorf("Category mismatch: got %q, want %q", doc.Content.Strings["Category"], original.Category)
	}
}

func TestSearch_ContentToAtom_NilData(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())

	result, err := search.contentToAtom(nil)
	if err != nil {
		t.Fatalf("contentToAtom(nil) failed: %v", err)
	}
	if result != nil {
		t.Error("expected nil result for nil data")
	}
}

func TestSearch_AtomToContent_NilAtom(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())

	result, err := search.atomToContent(nil)
	if err != nil {
		t.Fatalf("atomToContent(nil) failed: %v", err)
	}
	if result != nil {
		t.Error("expected nil result for nil atom")
	}
}

func TestSearch_FailingCodec(t *testing.T) {
	provider := newMockSearchProvider()
	atomizer, _ := atom.Use[testDocument]()
	ctx := context.Background()

	t.Run("encode error on IndexDoc", func(t *testing.T) {
		codec := &failingCodec{encodeErr: errors.New("encode failed")}
		search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())

		doc := &testDocument{Title: "Test"}
		a := atomizer.Atomize(doc)

		err := search.IndexDoc(ctx, "enc-fail", a)
		if err == nil {
			t.Error("expected encode error")
		}
	})

	t.Run("decode error on Get", func(t *testing.T) {
		codec := &failingCodec{decodeErr: errors.New("decode failed")}
		search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())

		provider.ensureIndex("products")
		provider.docs["products"]["dec-fail"] = []byte(`{"title":"Test"}`)

		_, err := search.Get(ctx, "dec-fail")
		if err == nil {
			t.Error("expected decode error")
		}
	})
}

// Verify JSON encoding preserves field values correctly.
func TestSearch_JSONRoundTrip(t *testing.T) {
	provider := newMockSearchProvider()
	codec := jsonCodec{}
	atomizer, _ := atom.Use[testDocument]()
	search := NewSearch[testDocument](provider, "products", codec, atomizer.Spec())
	ctx := context.Background()

	original := &testDocument{
		Title:    "Special \"Quoted\" Product",
		Price:    999.99,
		Category: "unicode: こんにちは",
	}
	a := atomizer.Atomize(original)

	if err := search.IndexDoc(ctx, "json-rt", a); err != nil {
		t.Fatalf("IndexDoc failed: %v", err)
	}

	// Verify stored JSON is valid
	stored := provider.docs["products"]["json-rt"]
	var parsed testDocument
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
