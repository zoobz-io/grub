package elasticsearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/lucene"
	esrenderer "github.com/zoobz-io/lucene/elasticsearch"
)

// setupMockServer creates a test server with configurable responses.
func setupMockServer(t *testing.T, handler http.HandlerFunc) (*elasticsearch.Client, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{server.URL},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return client, server.Close
}

func TestNew(t *testing.T) {
	client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer cleanup()

	config := Config{
		Version:        esrenderer.V8,
		RefreshOnWrite: true,
	}
	provider := New(client, config)

	if provider == nil {
		t.Fatal("New returned nil")
	}
	if provider.client != client {
		t.Error("client not set correctly")
	}
	if provider.renderer == nil {
		t.Error("renderer not initialized")
	}
	if provider.config.Version != esrenderer.V8 {
		t.Error("config not set correctly")
	}
}

func TestProvider_Index(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PUT" && r.Method != "POST" {
				t.Errorf("expected PUT or POST, got %s", r.Method)
			}
			if !strings.Contains(r.URL.Path, "/products/_doc/") {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"_index":"products","_id":"doc-1","result":"created"}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		doc := []byte(`{"title":"Test Product","price":99.99}`)
		err := provider.Index(ctx, "products", "doc-1", doc)
		if err != nil {
			t.Fatalf("Index failed: %v", err)
		}
	})

	t.Run("error response", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"type":"mapper_parsing_exception"}}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		err := provider.Index(ctx, "products", "doc-1", []byte(`{}`))
		if err == nil {
			t.Error("expected error for bad request")
		}
	})
}

func TestProvider_IndexBatch(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected POST, got %s", r.Method)
			}
			if !strings.Contains(r.URL.Path, "_bulk") {
				t.Errorf("expected bulk path, got: %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"took":10,"errors":false,"items":[{"index":{"_id":"d1","status":201}},{"index":{"_id":"d2","status":201}}]}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		docs := map[string][]byte{
			"d1": []byte(`{"title":"Doc 1"}`),
			"d2": []byte(`{"title":"Doc 2"}`),
		}
		err := provider.IndexBatch(ctx, "products", docs)
		if err != nil {
			t.Fatalf("IndexBatch failed: %v", err)
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(_ http.ResponseWriter, _ *http.Request) {
			t.Error("should not make request for empty batch")
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		err := provider.IndexBatch(ctx, "products", map[string][]byte{})
		if err != nil {
			t.Errorf("empty batch should succeed: %v", err)
		}
	})

	t.Run("with errors", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"took":10,"errors":true,"items":[{"index":{"_id":"d1","status":400,"error":{"type":"mapper_parsing_exception"}}}]}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		docs := map[string][]byte{
			"d1": []byte(`{"title":"Doc 1"}`),
		}
		err := provider.IndexBatch(ctx, "products", docs)
		if err == nil {
			t.Error("expected error when bulk has errors")
		}
	})
}

func TestProvider_Get(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected GET, got %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"_index":"products","_id":"doc-1","found":true,"_source":{"title":"Test","price":50}}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		data, err := provider.Get(ctx, "products", "doc-1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		var doc map[string]any
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatalf("invalid JSON returned: %v", err)
		}
		if doc["title"] != "Test" {
			t.Errorf("expected title 'Test', got %v", doc["title"])
		}
	})

	t.Run("not found", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"_index":"products","_id":"doc-1","found":false}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		_, err := provider.Get(ctx, "products", "missing")
		if err != grub.ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestProvider_Delete(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("expected DELETE, got %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"_index":"products","_id":"doc-1","result":"deleted"}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		err := provider.Delete(ctx, "products", "doc-1")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"_index":"products","_id":"doc-1","result":"not_found"}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		err := provider.Delete(ctx, "products", "missing")
		if err != grub.ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestProvider_DeleteBatch(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected POST, got %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"took":5,"errors":false,"items":[{"delete":{"_id":"d1","status":200}},{"delete":{"_id":"d2","status":404}}]}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		err := provider.DeleteBatch(ctx, "products", []string{"d1", "d2"})
		if err != nil {
			t.Fatalf("DeleteBatch failed: %v", err)
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(_ http.ResponseWriter, _ *http.Request) {
			t.Error("should not make request for empty batch")
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		err := provider.DeleteBatch(ctx, "products", []string{})
		if err != nil {
			t.Errorf("empty batch should succeed: %v", err)
		}
	})
}

func TestProvider_Exists(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "HEAD" {
				t.Errorf("expected HEAD, got %s", r.Method)
			}
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		exists, err := provider.Exists(ctx, "products", "doc-1")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("expected document to exist")
		}
	})

	t.Run("not exists", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusNotFound)
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		exists, err := provider.Exists(ctx, "products", "missing")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected document to not exist")
		}
	})
}

func TestProvider_Search(t *testing.T) {
	t.Run("basic search", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" && r.Method != "GET" {
				t.Errorf("expected POST or GET, got %s", r.Method)
			}
			if !strings.Contains(r.URL.Path, "_search") {
				t.Errorf("expected search path, got: %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"took": 5,
				"hits": {
					"total": {"value": 2, "relation": "eq"},
					"max_score": 1.5,
					"hits": [
						{"_id": "doc-1", "_score": 1.5, "_source": {"title": "First"}},
						{"_id": "doc-2", "_score": 1.2, "_source": {"title": "Second"}}
					]
				}
			}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		search := lucene.NewSearch().Size(10)
		result, err := provider.Search(ctx, "products", search)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		if result.Total != 2 {
			t.Errorf("expected total 2, got %d", result.Total)
		}
		if result.MaxScore != 1.5 {
			t.Errorf("expected max_score 1.5, got %f", result.MaxScore)
		}
		if len(result.Hits) != 2 {
			t.Errorf("expected 2 hits, got %d", len(result.Hits))
		}
		if result.Hits[0].ID != "doc-1" {
			t.Errorf("expected first hit ID 'doc-1', got %q", result.Hits[0].ID)
		}
		if result.Hits[0].Score != 1.5 {
			t.Errorf("expected first hit score 1.5, got %f", result.Hits[0].Score)
		}
	})

	t.Run("with aggregations", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"took": 3,
				"hits": {"total": {"value": 0}, "hits": []},
				"aggregations": {
					"categories": {
						"buckets": [
							{"key": "electronics", "doc_count": 50},
							{"key": "clothing", "doc_count": 30}
						]
					}
				}
			}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		search := lucene.NewSearch().Size(0)

		result, err := provider.Search(ctx, "products", search)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		if result.Aggregations == nil {
			t.Error("expected aggregations")
		}
		if _, ok := result.Aggregations["categories"]; !ok {
			t.Error("expected categories aggregation")
		}
	})

	t.Run("error response", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"type":"parsing_exception","reason":"Unknown key"}}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		search := lucene.NewSearch()
		_, err := provider.Search(ctx, "products", search)
		if err == nil {
			t.Error("expected error for bad request")
		}
	})
}

func TestProvider_Count(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if !strings.Contains(r.URL.Path, "_count") {
				t.Errorf("expected count path, got: %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"count": 42}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		count, err := provider.Count(ctx, "products", nil)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 42 {
			t.Errorf("expected count 42, got %d", count)
		}
	})

	t.Run("with query", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"count": 10}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		b := lucene.New[struct{}]()
		query := b.MatchAll()

		count, err := provider.Count(ctx, "products", query)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 10 {
			t.Errorf("expected count 10, got %d", count)
		}
	})
}

func TestProvider_Refresh(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" && r.Method != "GET" {
				t.Errorf("unexpected method: %s", r.Method)
			}
			if !strings.Contains(r.URL.Path, "_refresh") {
				t.Errorf("expected refresh path, got: %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"_shards":{"total":2,"successful":2,"failed":0}}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		err := provider.Refresh(ctx, "products")
		if err != nil {
			t.Fatalf("Refresh failed: %v", err)
		}
	})
}

func TestProvider_CreateIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PUT" {
				t.Errorf("expected PUT, got %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true,"shards_acknowledged":true,"index":"products"}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		settings := map[string]any{
			"settings": map[string]any{
				"number_of_shards":   1,
				"number_of_replicas": 0,
			},
		}
		err := provider.CreateIndex(ctx, "products", settings)
		if err != nil {
			t.Fatalf("CreateIndex failed: %v", err)
		}
	})

	t.Run("already exists", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"type":"resource_already_exists_exception","reason":"index already exists"}}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		err := provider.CreateIndex(ctx, "products", nil)
		// Should be idempotent - no error for already exists.
		if err != nil {
			t.Errorf("CreateIndex should be idempotent: %v", err)
		}
	})
}

func TestProvider_DeleteIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("expected DELETE, got %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		err := provider.DeleteIndex(ctx, "products")
		if err != nil {
			t.Fatalf("DeleteIndex failed: %v", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		client, cleanup := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"type":"index_not_found_exception"}}`))
		})
		defer cleanup()

		provider := New(client, Config{Version: esrenderer.V8})
		ctx := context.Background()

		err := provider.DeleteIndex(ctx, "missing")
		// Should be idempotent - no error for not found.
		if err != nil {
			t.Errorf("DeleteIndex should be idempotent: %v", err)
		}
	})
}

func TestProvider_RefreshOnWrite(t *testing.T) {
	var refreshParam string

	client, cleanup := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
		refreshParam = r.URL.Query().Get("refresh")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"_index":"products","_id":"doc-1","result":"created"}`))
	})
	defer cleanup()

	provider := New(client, Config{
		Version:        esrenderer.V8,
		RefreshOnWrite: true,
	})
	ctx := context.Background()

	_ = provider.Index(ctx, "products", "doc-1", []byte(`{}`))

	if refreshParam != "true" {
		t.Errorf("expected refresh=true, got %q", refreshParam)
	}
}
