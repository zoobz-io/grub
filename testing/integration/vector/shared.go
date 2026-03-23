// Package vector provides shared test infrastructure for grub vector integration tests.
package vector

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/sentinel"
	"github.com/zoobz-io/vecna"
)

// testID generates a unique UUID for a test to avoid conflicts.
func testID() uuid.UUID {
	return uuid.New()
}

func init() {
	sentinel.Tag("json")
}

// TestMetadata is the model used for vector integration tests.
type TestMetadata struct {
	Category string   `json:"category,omitempty"`
	Tags     []string `json:"tags,omitempty"`
	Score    float64  `json:"score,omitempty"`
}

// TestContext holds shared test resources for a provider.
type TestContext struct {
	Provider grub.VectorProvider
	Cleanup  func() // optional cleanup function
}

// RunCRUDTests runs the core CRUD test suite against the given context.
func RunCRUDTests(t *testing.T, tc *TestContext) {
	t.Run("GetNotFound", func(t *testing.T) { testGetNotFound(t, tc) })
	t.Run("UpsertAndGet", func(t *testing.T) { testUpsertAndGet(t, tc) })
	t.Run("UpsertOverwrite", func(t *testing.T) { testUpsertOverwrite(t, tc) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, tc) })
	t.Run("DeleteNotFound", func(t *testing.T) { testDeleteNotFound(t, tc) })
	t.Run("Exists", func(t *testing.T) { testExists(t, tc) })
	t.Run("ExistsNotFound", func(t *testing.T) { testExistsNotFound(t, tc) })
}

// RunSearchTests runs the search test suite against the given context.
func RunSearchTests(t *testing.T, tc *TestContext) {
	t.Run("BasicSearch", func(t *testing.T) { testBasicSearch(t, tc) })
	t.Run("SearchWithLimit", func(t *testing.T) { testSearchWithLimit(t, tc) })
	t.Run("SearchWithFilter", func(t *testing.T) { testSearchWithFilter(t, tc) })
	t.Run("ScoreOrdering", func(t *testing.T) { testScoreOrdering(t, tc) })
	t.Run("ExactMatch", func(t *testing.T) { testExactMatch(t, tc) })
}

// RunBatchTests runs the batch operation test suite.
func RunBatchTests(t *testing.T, tc *TestContext) {
	t.Run("UpsertBatch", func(t *testing.T) { testUpsertBatch(t, tc) })
	t.Run("DeleteBatch", func(t *testing.T) { testDeleteBatch(t, tc) })
	t.Run("List", func(t *testing.T) { testList(t, tc) })
	t.Run("ListWithLimit", func(t *testing.T) { testListWithLimit(t, tc) })
}

// RunAtomicTests runs the atomic index test suite.
func RunAtomicTests(t *testing.T, tc *TestContext) {
	t.Run("AtomicGetUpsert", func(t *testing.T) { testAtomicGetUpsert(t, tc) })
}

// RunFilterTests runs the Filter API test suite.
// Set supportsFilter to false for providers that return ErrFilterNotSupported (e.g., Pinecone).
func RunFilterTests(t *testing.T, tc *TestContext, supportsFilter bool) {
	if !supportsFilter {
		t.Run("FilterNotSupported", func(t *testing.T) { testFilterNotSupported(t, tc) })
		return
	}

	t.Run("FilterNilFilter", func(t *testing.T) { testFilterNilFilter(t, tc) })
	t.Run("FilterWithCondition", func(t *testing.T) { testFilterWithCondition(t, tc) })
	t.Run("FilterWithLimit", func(t *testing.T) { testFilterWithLimit(t, tc) })
	t.Run("FilterNoMatches", func(t *testing.T) { testFilterNoMatches(t, tc) })
}

// RunQueryTests runs the Query API test suite with vecna filters.
// Note: Some tests are provider-specific. Pinecone doesn't support range operators.
func RunQueryTests(t *testing.T, tc *TestContext, supportedOps QueryOperators) {
	t.Run("QueryNilFilter", func(t *testing.T) { testQueryNilFilter(t, tc) })
	t.Run("QueryEq", func(t *testing.T) { testQueryEq(t, tc) })
	t.Run("QueryNe", func(t *testing.T) { testQueryNe(t, tc) })
	t.Run("QueryIn", func(t *testing.T) { testQueryIn(t, tc) })
	t.Run("QueryNin", func(t *testing.T) { testQueryNin(t, tc) })
	t.Run("QueryAnd", func(t *testing.T) { testQueryAnd(t, tc) })
	t.Run("QueryOr", func(t *testing.T) { testQueryOr(t, tc) })
	t.Run("QueryNot", func(t *testing.T) { testQueryNot(t, tc) })

	// Range operators - only run if supported
	if supportedOps.Range {
		t.Run("QueryGt", func(t *testing.T) { testQueryGt(t, tc) })
		t.Run("QueryGte", func(t *testing.T) { testQueryGte(t, tc) })
		t.Run("QueryLt", func(t *testing.T) { testQueryLt(t, tc) })
		t.Run("QueryLte", func(t *testing.T) { testQueryLte(t, tc) })
	}

	// Like - only run if supported
	if supportedOps.Like {
		t.Run("QueryLike", func(t *testing.T) { testQueryLike(t, tc) })
	}

	// Contains - only run if supported
	if supportedOps.Contains {
		t.Run("QueryContains", func(t *testing.T) { testQueryContains(t, tc) })
	}
}

// QueryOperators indicates which operators a provider supports.
type QueryOperators struct {
	Range    bool // Gt, Gte, Lt, Lte
	Like     bool
	Contains bool
}

// HookedMetadata is a model with lifecycle hooks for integration testing.
type HookedMetadata struct {
	Category string `json:"category,omitempty"`
	Score    float64 `json:"score,omitempty"`

	afterLoadCalled bool
}

func (h *HookedMetadata) AfterLoad(_ context.Context) error {
	h.afterLoadCalled = true
	return nil
}

func (h *HookedMetadata) BeforeSave(_ context.Context) error { return nil }
func (h *HookedMetadata) AfterSave(_ context.Context) error  { return nil }

// FailingBeforeSaveMetadata always fails BeforeSave.
type FailingBeforeSaveMetadata struct {
	Category string `json:"category,omitempty"`
}

var errTestHook = errors.New("test hook error")

func (f *FailingBeforeSaveMetadata) BeforeSave(_ context.Context) error { return errTestHook }

// RunHookTests runs the lifecycle hook test suite for vector indices.
func RunHookTests(t *testing.T, tc *TestContext) {
	t.Run("AfterLoadOnGet", func(t *testing.T) { testHookAfterLoadGet(t, tc) })
	t.Run("AfterLoadOnSearch", func(t *testing.T) { testHookAfterLoadSearch(t, tc) })
	t.Run("BeforeSaveOnUpsert", func(t *testing.T) { testHookBeforeSaveUpsert(t, tc) })
	t.Run("BeforeSaveErrorAborts", func(t *testing.T) { testHookBeforeSaveError(t, tc) })
}

func testHookAfterLoadGet(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[HookedMetadata](tc.Provider)

	id := testID()
	meta := &HookedMetadata{Category: "test", Score: 1.0}
	if err := index.Upsert(ctx, id, []float32{1.0, 0.0, 0.0}, meta); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	got, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !got.Metadata.afterLoadCalled {
		t.Error("AfterLoad not called on Get")
	}
	if got.Metadata.Category != "test" {
		t.Errorf("expected category 'test', got %q", got.Metadata.Category)
	}
}

func testHookAfterLoadSearch(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[HookedMetadata](tc.Provider)

	id := testID()
	meta := &HookedMetadata{Category: "search", Score: 2.0}
	if err := index.Upsert(ctx, id, []float32{0.0, 1.0, 0.0}, meta); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	results, err := index.Search(ctx, []float32{0.0, 1.0, 0.0}, 1, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least 1 result")
	}
	if !results[0].Metadata.afterLoadCalled {
		t.Error("AfterLoad not called on Search result")
	}
}

func testHookBeforeSaveUpsert(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[HookedMetadata](tc.Provider)

	id := testID()
	meta := &HookedMetadata{Category: "saved", Score: 3.0}
	if err := index.Upsert(ctx, id, []float32{0.0, 0.0, 1.0}, meta); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	got, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Metadata.Category != "saved" {
		t.Errorf("expected category 'saved', got %q", got.Metadata.Category)
	}
}

func testHookBeforeSaveError(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[FailingBeforeSaveMetadata](tc.Provider)

	id := testID()
	meta := &FailingBeforeSaveMetadata{Category: "fail"}
	err := index.Upsert(ctx, id, []float32{1.0, 1.0, 1.0}, meta)
	if !errors.Is(err, errTestHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}

	// Verify nothing was persisted
	index2 := grub.NewIndex[TestMetadata](tc.Provider)
	_, err = index2.Get(ctx, id)
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound (record should not exist), got: %v", err)
	}
}

// --- CRUD Tests ---

func testGetNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	_, err := index.Get(ctx, testID())
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func testUpsertAndGet(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	id := testID()
	metadata := &TestMetadata{
		Category: "test",
		Tags:     []string{"a", "b"},
		Score:    0.95,
	}
	// Use a distinctive vector that won't conflict with search tests
	vector := []float32{0.33, 0.33, 0.34}

	err := index.Upsert(ctx, id, vector, metadata)
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	got, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.ID != id {
		t.Errorf("expected ID %s, got %s", id, got.ID)
	}
	if got.Metadata.Category != metadata.Category {
		t.Errorf("expected Category %q, got %q", metadata.Category, got.Metadata.Category)
	}
	if len(got.Vector) != len(vector) {
		t.Errorf("expected vector length %d, got %d", len(vector), len(got.Vector))
	}
}

func testUpsertOverwrite(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	id := testID()
	original := &TestMetadata{Category: "original", Score: 1.0}
	updated := &TestMetadata{Category: "updated", Score: 2.0}

	err := index.Upsert(ctx, id, []float32{0.1, 0.2, 0.3}, original)
	if err != nil {
		t.Fatalf("Upsert original failed: %v", err)
	}

	err = index.Upsert(ctx, id, []float32{0.3, 0.2, 0.1}, updated)
	if err != nil {
		t.Fatalf("Upsert updated failed: %v", err)
	}

	got, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Metadata.Category != "updated" {
		t.Errorf("expected Category 'updated', got %q", got.Metadata.Category)
	}
}

func testDelete(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	id := testID()
	metadata := &TestMetadata{Category: "to-delete"}
	err := index.Upsert(ctx, id, []float32{0.4, 0.5, 0.6}, metadata)
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	err = index.Delete(ctx, id)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = index.Get(ctx, id)
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func testDeleteNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	err := index.Delete(ctx, testID())
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func testExists(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	id := testID()
	metadata := &TestMetadata{Category: "exists"}
	err := index.Upsert(ctx, id, []float32{0.7, 0.8, 0.9}, metadata)
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	exists, err := index.Exists(ctx, id)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected vector to exist")
	}
}

func testExistsNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	exists, err := index.Exists(ctx, testID())
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected vector to not exist")
	}
}

// --- Search Tests ---

func testBasicSearch(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	// Use unique IDs and a unique category for this test
	uniqueCategory := testID().String()
	id1, id2, id3 := testID(), testID(), testID()

	// Insert test vectors with unique category for filtering
	vectors := []struct {
		id       uuid.UUID
		vector   []float32
		metadata *TestMetadata
	}{
		{id1, []float32{0.95, 0.05, 0.0}, &TestMetadata{Category: uniqueCategory, Score: 1.0}},
		{id2, []float32{0.05, 0.95, 0.0}, &TestMetadata{Category: uniqueCategory, Score: 2.0}},
		{id3, []float32{0.05, 0.05, 0.9}, &TestMetadata{Category: uniqueCategory, Score: 3.0}},
	}

	for _, v := range vectors {
		err := index.Upsert(ctx, v.id, v.vector, v.metadata)
		if err != nil {
			t.Fatalf("Upsert %s failed: %v", v.id, err)
		}
	}

	// Query closest to [1,0,0] with filter for our category
	query := []float32{1.0, 0.0, 0.0}
	filter := &TestMetadata{Category: uniqueCategory}
	results, err := index.Search(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) < 3 {
		t.Errorf("expected at least 3 results, got %d", len(results))
		return
	}

	// First result should be id1 (closest to [1,0,0])
	if results[0].ID != id1 {
		t.Errorf("expected first result to be %s (closest match), got %s", id1, results[0].ID)
	}

	// Verify all results have our category
	for _, r := range results {
		if r.Metadata.Category != uniqueCategory {
			t.Errorf("expected category %q, got %q", uniqueCategory, r.Metadata.Category)
		}
	}
}

func testSearchWithLimit(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	// Insert multiple vectors
	for i := 0; i < 10; i++ {
		id := testID()
		vec := []float32{float32(i) * 0.1, float32(10-i) * 0.1, 0.0}
		metadata := &TestMetadata{Category: "limit", Score: float64(i)}
		err := index.Upsert(ctx, id, vec, metadata)
		if err != nil {
			t.Fatalf("Upsert %s failed: %v", id, err)
		}
	}

	query := []float32{0.5, 0.5, 0.0}
	results, err := index.Search(ctx, query, 3, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) > 3 {
		t.Errorf("expected at most 3 results, got %d", len(results))
	}
}

func testSearchWithFilter(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	// Use a unique category for this test
	uniqueCategory := testID().String()

	// Insert vectors with different categories
	vectors := []struct {
		id       uuid.UUID
		vector   []float32
		metadata *TestMetadata
	}{
		{testID(), []float32{0.2, 0.3, 0.4}, &TestMetadata{Category: uniqueCategory, Score: 1.0}},
		{testID(), []float32{0.3, 0.4, 0.5}, &TestMetadata{Category: uniqueCategory, Score: 2.0}},
		{testID(), []float32{0.4, 0.5, 0.6}, &TestMetadata{Category: "normal", Score: 3.0}},
	}

	for _, v := range vectors {
		err := index.Upsert(ctx, v.id, v.vector, v.metadata)
		if err != nil {
			t.Fatalf("Upsert %s failed: %v", v.id, err)
		}
	}

	// Search with filter for our unique category
	query := []float32{0.25, 0.35, 0.45}
	filter := &TestMetadata{Category: uniqueCategory}
	results, err := index.Search(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// Should only return vectors with our unique category
	if len(results) < 2 {
		t.Errorf("expected at least 2 filtered results, got %d", len(results))
	}
	for _, r := range results {
		if r.Metadata.Category != uniqueCategory {
			t.Errorf("expected category %q, got %q", uniqueCategory, r.Metadata.Category)
		}
	}
}

// --- Batch Tests ---

func testUpsertBatch(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	id1, id2, id3 := testID(), testID(), testID()
	vectors := []grub.Vector[TestMetadata]{
		{ID: id1, Vector: []float32{1.0, 0.0, 0.0}, Metadata: TestMetadata{Category: "a", Score: 1.0}},
		{ID: id2, Vector: []float32{0.0, 1.0, 0.0}, Metadata: TestMetadata{Category: "b", Score: 2.0}},
		{ID: id3, Vector: []float32{0.5, 0.5, 0.0}, Metadata: TestMetadata{Category: "c", Score: 3.0}},
	}

	err := index.UpsertBatch(ctx, vectors)
	if err != nil {
		t.Fatalf("UpsertBatch failed: %v", err)
	}

	// Verify all were stored
	for _, v := range vectors {
		got, err := index.Get(ctx, v.ID)
		if err != nil {
			t.Fatalf("Get %s failed: %v", v.ID, err)
		}
		if got.Metadata.Category != v.Metadata.Category {
			t.Errorf("vector %s: expected Category %q, got %q", v.ID, v.Metadata.Category, got.Metadata.Category)
		}
	}
}

func testDeleteBatch(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	// Insert vectors to delete
	ids := []uuid.UUID{testID(), testID(), testID()}
	for _, id := range ids {
		err := index.Upsert(ctx, id, []float32{1.0, 0.0, 0.0}, &TestMetadata{Category: "delete"})
		if err != nil {
			t.Fatalf("Upsert %s failed: %v", id, err)
		}
	}

	// Delete batch (including one that doesn't exist)
	err := index.DeleteBatch(ctx, append(ids, testID()))
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}

	// Verify all were deleted
	for _, id := range ids {
		exists, err := index.Exists(ctx, id)
		if err != nil {
			t.Fatalf("Exists %s failed: %v", id, err)
		}
		if exists {
			t.Errorf("vector %s should have been deleted", id)
		}
	}
}

func testList(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	// Insert vectors
	insertedIDs := make([]uuid.UUID, 5)
	for i := 0; i < 5; i++ {
		id := testID()
		insertedIDs[i] = id
		err := index.Upsert(ctx, id, []float32{float32(i) * 0.1, 0.5, 0.5}, &TestMetadata{Category: "list"})
		if err != nil {
			t.Fatalf("Upsert %s failed: %v", id, err)
		}
	}

	ids, err := index.List(ctx, 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	// Should return at least the vectors we just inserted
	if len(ids) < 5 {
		t.Errorf("expected at least 5 ids, got %d", len(ids))
	}
}

func testListWithLimit(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	// Insert vectors
	for i := 0; i < 10; i++ {
		id := testID()
		err := index.Upsert(ctx, id, []float32{float32(i) * 0.1, 0.5, 0.5}, &TestMetadata{Category: "limit"})
		if err != nil {
			t.Fatalf("Upsert %s failed: %v", id, err)
		}
	}

	ids, err := index.List(ctx, 3)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(ids) != 3 {
		t.Errorf("expected 3 ids with limit, got %d", len(ids))
	}
}

// --- Atomic Tests ---

func testAtomicGetUpsert(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	id := testID()
	metadata := &TestMetadata{Category: "atomic", Tags: []string{"x"}, Score: 42.0}
	err := index.Upsert(ctx, id, []float32{1.0, 2.0, 3.0}, metadata)
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	atomic := index.Atomic()
	if atomic == nil {
		t.Fatal("Atomic returned nil")
	}

	a, err := atomic.Get(ctx, id)
	if err != nil {
		t.Fatalf("Atomic().Get failed: %v", err)
	}

	if a.Metadata.Strings["Category"] != "atomic" {
		t.Errorf("expected Category 'atomic', got %q", a.Metadata.Strings["Category"])
	}

	// Modify via atom
	a.Metadata.Strings["Category"] = "modified"

	err = atomic.Upsert(ctx, id, a.Vector, a.Metadata)
	if err != nil {
		t.Fatalf("Atomic().Upsert failed: %v", err)
	}

	// Verify via typed API
	got, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get after Atomic().Upsert failed: %v", err)
	}

	if got.Metadata.Category != "modified" {
		t.Errorf("expected Category 'modified', got %q", got.Metadata.Category)
	}
}

// --- Enhanced Search Tests ---

func testScoreOrdering(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	// Use unique IDs and category
	uniqueCategory := testID().String()
	idClose := testID()
	idMid := testID()
	idFar := testID()

	// Insert vectors at known distances from query [1,0,0]
	// close: [0.99, 0.01, 0.0] - L2 distance ≈ 0.014
	// mid:   [0.5, 0.5, 0.0]   - L2 distance ≈ 0.71
	// far:   [0.0, 1.0, 0.0]   - L2 distance ≈ 1.41
	vectors := []struct {
		id     uuid.UUID
		vector []float32
	}{
		{idClose, []float32{0.99, 0.01, 0.0}},
		{idMid, []float32{0.5, 0.5, 0.0}},
		{idFar, []float32{0.0, 1.0, 0.0}},
	}

	for _, v := range vectors {
		err := index.Upsert(ctx, v.id, v.vector, &TestMetadata{Category: uniqueCategory})
		if err != nil {
			t.Fatalf("Upsert %s failed: %v", v.id, err)
		}
	}

	// Query from [1,0,0] with filter for our category
	query := []float32{1.0, 0.0, 0.0}
	filter := &TestMetadata{Category: uniqueCategory}
	results, err := index.Search(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) < 3 {
		t.Fatalf("expected at least 3 results, got %d", len(results))
	}

	// Verify ordering: scores should be non-decreasing (closest first)
	for i := 1; i < len(results); i++ {
		if results[i].Score < results[i-1].Score {
			t.Errorf("results not ordered by distance: result[%d].Score=%f < result[%d].Score=%f",
				i, results[i].Score, i-1, results[i-1].Score)
		}
	}

	// The closest should be idClose (vector closest to [1,0,0])
	if results[0].ID != idClose {
		t.Errorf("expected first result to be %s (closest), got %s", idClose, results[0].ID)
	}

	// The farthest should be idFar
	if results[2].ID != idFar {
		t.Errorf("expected third result to be %s (farthest), got %s", idFar, results[2].ID)
	}
}

func testExactMatch(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	// Use unique ID and category
	id := testID()
	uniqueCategory := testID().String()

	// Insert a vector
	targetVector := []float32{0.123, 0.456, 0.789}
	err := index.Upsert(ctx, id, targetVector, &TestMetadata{Category: uniqueCategory})
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Search for exact same vector with filter
	filter := &TestMetadata{Category: uniqueCategory}
	results, err := index.Search(ctx, targetVector, 1, filter)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected at least one result")
	}

	// First result should be the exact match
	if results[0].ID != id {
		t.Errorf("expected first result to be %s, got %s", id, results[0].ID)
	}

	// Score should be very close to 0 for exact match (L2 distance)
	if results[0].Score > 0.001 {
		t.Errorf("expected near-zero score for exact match, got %f", results[0].Score)
	}
}

// --- Helper Functions ---

// L2Distance calculates L2 (Euclidean) distance between two vectors.
func L2Distance(a, b []float32) float32 {
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

// CosineSimilarity calculates cosine similarity between two vectors.
func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}

// --- Query Tests ---

// mustQueryBuilder creates a vecna Builder for TestMetadata, failing test if error.
func mustQueryBuilder(t *testing.T) *vecna.Builder[TestMetadata] {
	t.Helper()
	b, err := vecna.New[TestMetadata]()
	if err != nil {
		t.Fatalf("failed to create vecna builder: %v", err)
	}
	return b
}

// setupQueryTestData inserts test vectors for Query tests and returns the unique category.
func setupQueryTestData(t *testing.T, tc *TestContext) string {
	t.Helper()
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	uniqueCategory := testID().String()

	vectors := []struct {
		id       uuid.UUID
		vector   []float32
		metadata *TestMetadata
	}{
		{testID(), []float32{0.9, 0.1, 0.0}, &TestMetadata{Category: uniqueCategory, Score: 10.0, Tags: []string{"alpha", "beta"}}},
		{testID(), []float32{0.1, 0.9, 0.0}, &TestMetadata{Category: uniqueCategory, Score: 20.0, Tags: []string{"gamma"}}},
		{testID(), []float32{0.1, 0.1, 0.8}, &TestMetadata{Category: "other", Score: 30.0, Tags: []string{"delta"}}},
		{testID(), []float32{0.5, 0.5, 0.0}, &TestMetadata{Category: uniqueCategory, Score: 15.0, Tags: []string{"alpha"}}},
		{testID(), []float32{0.3, 0.3, 0.4}, &TestMetadata{Category: uniqueCategory, Score: 25.0, Tags: []string{"beta", "gamma"}}},
	}

	for _, v := range vectors {
		err := index.Upsert(ctx, v.id, v.vector, v.metadata)
		if err != nil {
			t.Fatalf("Upsert %s failed: %v", v.id, err)
		}
	}

	return uniqueCategory
}

func testQueryNilFilter(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	uniqueCategory := setupQueryTestData(t, tc)
	_ = uniqueCategory // suppress unused warning - nil filter returns all

	query := []float32{0.5, 0.5, 0.0}
	results, err := index.Query(ctx, query, 10, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) == 0 {
		t.Error("expected results with nil filter")
	}
}

func testQueryEq(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	filter := b.Where("category").Eq(uniqueCategory)
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should get 4 results (all with uniqueCategory, not "other")
	if len(results) < 4 {
		t.Errorf("expected at least 4 results with Category=%s, got %d", uniqueCategory, len(results))
	}

	for _, r := range results {
		if r.Metadata.Category != uniqueCategory {
			t.Errorf("expected Category=%s, got %s", uniqueCategory, r.Metadata.Category)
		}
	}
}

func testQueryNe(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	filter := b.Where("category").Ne(uniqueCategory)
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// All results should NOT have uniqueCategory
	for _, r := range results {
		if r.Metadata.Category == uniqueCategory {
			t.Errorf("expected Category != %s, got %s", uniqueCategory, r.Metadata.Category)
		}
	}
}

func testQueryIn(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	filter := b.Where("category").In(uniqueCategory, "other")
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should get all 5 vectors (4 with uniqueCategory + 1 with "other")
	if len(results) < 5 {
		t.Errorf("expected at least 5 results, got %d", len(results))
	}
}

func testQueryNin(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	filter := b.Where("category").Nin(uniqueCategory)
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should only get "other" category (1 result from our test data, possibly more from other tests)
	for _, r := range results {
		if r.Metadata.Category == uniqueCategory {
			t.Errorf("expected Category not in [%s], got %s", uniqueCategory, r.Metadata.Category)
		}
	}
}

func testQueryAnd(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	// Category = uniqueCategory AND Score >= 15 (should match 3 of 4)
	filter := b.And(
		b.Where("category").Eq(uniqueCategory),
		b.Where("score").Gte(15.0),
	)
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		// If provider doesn't support Gte, skip
		if errors.Is(err, grub.ErrOperatorNotSupported) {
			t.Skip("provider doesn't support Gte operator")
		}
		t.Fatalf("Query failed: %v", err)
	}

	for _, r := range results {
		if r.Metadata.Category != uniqueCategory {
			t.Errorf("expected Category=%s, got %s", uniqueCategory, r.Metadata.Category)
		}
		if r.Metadata.Score < 15.0 {
			t.Errorf("expected Score >= 15, got %f", r.Metadata.Score)
		}
	}
}

func testQueryOr(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	// Category = uniqueCategory OR Category = "other"
	filter := b.Or(
		b.Where("category").Eq(uniqueCategory),
		b.Where("category").Eq("other"),
	)
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should get all 5 test vectors
	if len(results) < 5 {
		t.Errorf("expected at least 5 results, got %d", len(results))
	}
}

func testQueryNot(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	filter := b.Not(b.Where("category").Eq(uniqueCategory))
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// All results should NOT have uniqueCategory
	for _, r := range results {
		if r.Metadata.Category == uniqueCategory {
			t.Errorf("expected Category != %s with NOT, got %s", uniqueCategory, r.Metadata.Category)
		}
	}
}

func testQueryGt(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	// Score > 20 AND in our category
	filter := b.And(
		b.Where("category").Eq(uniqueCategory),
		b.Where("score").Gt(20.0),
	)
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	for _, r := range results {
		if r.Metadata.Score <= 20.0 {
			t.Errorf("expected Score > 20, got %f", r.Metadata.Score)
		}
	}
}

func testQueryGte(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	filter := b.And(
		b.Where("category").Eq(uniqueCategory),
		b.Where("score").Gte(20.0),
	)
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	for _, r := range results {
		if r.Metadata.Score < 20.0 {
			t.Errorf("expected Score >= 20, got %f", r.Metadata.Score)
		}
	}
}

func testQueryLt(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	filter := b.And(
		b.Where("category").Eq(uniqueCategory),
		b.Where("score").Lt(15.0),
	)
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	for _, r := range results {
		if r.Metadata.Score >= 15.0 {
			t.Errorf("expected Score < 15, got %f", r.Metadata.Score)
		}
	}
}

func testQueryLte(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	filter := b.And(
		b.Where("category").Eq(uniqueCategory),
		b.Where("score").Lte(15.0),
	)
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	for _, r := range results {
		if r.Metadata.Score > 15.0 {
			t.Errorf("expected Score <= 15, got %f", r.Metadata.Score)
		}
	}
}

func testQueryLike(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	uniqueCategory := setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	// Match categories starting with uniqueCategory prefix
	filter := b.Where("category").Like(uniqueCategory[:len(uniqueCategory)-5] + "%")
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should find vectors whose category matches the pattern
	if len(results) == 0 {
		t.Error("expected results with Like pattern")
	}
}

func testQueryContains(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	_ = setupQueryTestData(t, tc)

	query := []float32{0.5, 0.5, 0.0}
	// Tags contains "alpha" - 2 of our test vectors have "alpha" in Tags
	filter := b.Where("tags").Contains("alpha")
	results, err := index.Query(ctx, query, 10, filter)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should find at least the vectors with "alpha" in Tags
	foundAlpha := false
	for _, r := range results {
		for _, tag := range r.Metadata.Tags {
			if tag == "alpha" {
				foundAlpha = true
				break
			}
		}
	}
	if !foundAlpha && len(results) > 0 {
		t.Error("expected to find vectors with 'alpha' in Tags")
	}
}

// --- Filter Tests ---

func testFilterNotSupported(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	_, err := index.Filter(ctx, nil, 10)
	if !errors.Is(err, grub.ErrFilterNotSupported) {
		t.Errorf("expected ErrFilterNotSupported, got %v", err)
	}
}

func testFilterNilFilter(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	// Insert test vectors.
	uniqueCategory := testID().String()
	for i := 0; i < 3; i++ {
		id := testID()
		err := index.Upsert(ctx, id, []float32{float32(i) * 0.1, 0.5, 0.5}, &TestMetadata{Category: uniqueCategory})
		if err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
	}

	// Filter with nil filter should return all vectors.
	results, err := index.Filter(ctx, nil, 0)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if len(results) < 3 {
		t.Errorf("expected at least 3 results with nil filter, got %d", len(results))
	}
}

func testFilterWithCondition(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	// Insert test vectors with different categories.
	uniqueCategory := testID().String()
	otherCategory := testID().String()

	for i := 0; i < 3; i++ {
		id := testID()
		err := index.Upsert(ctx, id, []float32{float32(i) * 0.1, 0.5, 0.5}, &TestMetadata{Category: uniqueCategory, Score: float64(i)})
		if err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		id := testID()
		err := index.Upsert(ctx, id, []float32{float32(i) * 0.2, 0.4, 0.4}, &TestMetadata{Category: otherCategory, Score: float64(i + 10)})
		if err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
	}

	// Filter for uniqueCategory only.
	filter := b.Where("category").Eq(uniqueCategory)
	results, err := index.Filter(ctx, filter, 0)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if len(results) < 3 {
		t.Errorf("expected at least 3 results with category=%s, got %d", uniqueCategory, len(results))
	}

	for _, r := range results {
		if r.Metadata.Category != uniqueCategory {
			t.Errorf("expected Category=%s, got %s", uniqueCategory, r.Metadata.Category)
		}
	}
}

func testFilterWithLimit(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)

	// Insert test vectors.
	uniqueCategory := testID().String()
	for i := 0; i < 10; i++ {
		id := testID()
		err := index.Upsert(ctx, id, []float32{float32(i) * 0.1, 0.5, 0.5}, &TestMetadata{Category: uniqueCategory})
		if err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
	}

	// Filter with limit.
	results, err := index.Filter(ctx, nil, 3)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if len(results) > 3 {
		t.Errorf("expected at most 3 results with limit=3, got %d", len(results))
	}
}

func testFilterNoMatches(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	index := grub.NewIndex[TestMetadata](tc.Provider)
	b := mustQueryBuilder(t)

	// Filter for a category that doesn't exist.
	nonExistentCategory := testID().String()
	filter := b.Where("category").Eq(nonExistentCategory)
	results, err := index.Filter(ctx, filter, 0)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for non-existent category, got %d", len(results))
	}
}
