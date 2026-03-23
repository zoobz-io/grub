package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/sentinel"
)

func init() {
	sentinel.Tag("json")
}

// BenchValue is the model used for benchmarks.
type BenchValue struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// mockProvider is a minimal in-memory provider for benchmarking.
type mockProvider struct {
	data map[string][]byte
}

func newMockProvider() *mockProvider {
	return &mockProvider{data: make(map[string][]byte)}
}

func (m *mockProvider) Get(_ context.Context, key string) ([]byte, error) {
	data, ok := m.data[key]
	if !ok {
		return nil, grub.ErrNotFound
	}
	return data, nil
}

func (m *mockProvider) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	m.data[key] = value
	return nil
}

func (m *mockProvider) Delete(_ context.Context, key string) error {
	if _, ok := m.data[key]; !ok {
		return grub.ErrNotFound
	}
	delete(m.data, key)
	return nil
}

func (m *mockProvider) Exists(_ context.Context, key string) (bool, error) {
	_, ok := m.data[key]
	return ok, nil
}

func (m *mockProvider) List(_ context.Context, prefix string, limit int) ([]string, error) {
	var keys []string
	for k := range m.data {
		if len(prefix) == 0 || (len(k) >= len(prefix) && k[:len(prefix)] == prefix) {
			keys = append(keys, k)
			if limit > 0 && len(keys) >= limit {
				break
			}
		}
	}
	return keys, nil
}

func (m *mockProvider) GetBatch(_ context.Context, keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	for _, k := range keys {
		if v, ok := m.data[k]; ok {
			result[k] = v
		}
	}
	return result, nil
}

func (m *mockProvider) SetBatch(_ context.Context, items map[string][]byte, _ time.Duration) error {
	for k, v := range items {
		m.data[k] = v
	}
	return nil
}

func BenchmarkStore_Set(b *testing.B) {
	provider := newMockProvider()
	store := grub.NewStore[BenchValue](provider)
	ctx := context.Background()
	value := &BenchValue{ID: "bench", Name: "Benchmark", Count: 42}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Set(ctx, "key", value, 0)
	}
}

func BenchmarkStore_Get(b *testing.B) {
	provider := newMockProvider()
	store := grub.NewStore[BenchValue](provider)
	ctx := context.Background()
	value := &BenchValue{ID: "bench", Name: "Benchmark", Count: 42}
	_ = store.Set(ctx, "key", value, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get(ctx, "key")
	}
}

func BenchmarkStore_SetGet(b *testing.B) {
	provider := newMockProvider()
	store := grub.NewStore[BenchValue](provider)
	ctx := context.Background()
	value := &BenchValue{ID: "bench", Name: "Benchmark", Count: 42}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Set(ctx, "key", value, 0)
		_, _ = store.Get(ctx, "key")
	}
}

func BenchmarkStore_Exists(b *testing.B) {
	provider := newMockProvider()
	store := grub.NewStore[BenchValue](provider)
	ctx := context.Background()
	value := &BenchValue{ID: "bench", Name: "Benchmark", Count: 42}
	_ = store.Set(ctx, "key", value, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Exists(ctx, "key")
	}
}
