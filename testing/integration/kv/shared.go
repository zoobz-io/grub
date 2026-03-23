// Package kv provides shared test infrastructure for grub KV integration tests.
package kv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/sentinel"
)

func init() {
	sentinel.Tag("json")
}

// TestValue is the model used for KV integration tests.
type TestValue struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// TestContext holds shared test resources for a provider.
type TestContext struct {
	Provider grub.StoreProvider
	Cleanup  func() // optional cleanup function
}

// RunCRUDTests runs the core CRUD test suite against the given context.
func RunCRUDTests(t *testing.T, tc *TestContext) {
	t.Run("GetNotFound", func(t *testing.T) { testGetNotFound(t, tc) })
	t.Run("SetAndGet", func(t *testing.T) { testSetAndGet(t, tc) })
	t.Run("SetOverwrite", func(t *testing.T) { testSetOverwrite(t, tc) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, tc) })
	t.Run("DeleteNotFound", func(t *testing.T) { testDeleteNotFound(t, tc) })
	t.Run("Exists", func(t *testing.T) { testExists(t, tc) })
	t.Run("ExistsNotFound", func(t *testing.T) { testExistsNotFound(t, tc) })
}

// RunAtomicTests runs the atomic store test suite.
func RunAtomicTests(t *testing.T, tc *TestContext) {
	t.Run("AtomicGetSet", func(t *testing.T) { testAtomicGetSet(t, tc) })
}

// RunTTLTests runs TTL-specific tests (skip for providers that don't support TTL).
func RunTTLTests(t *testing.T, tc *TestContext) {
	t.Run("TTLExpiration", func(t *testing.T) { testTTLExpiration(t, tc) })
}

// RunBatchTests runs the batch operation test suite.
func RunBatchTests(t *testing.T, tc *TestContext) {
	t.Run("List", func(t *testing.T) { testList(t, tc) })
	t.Run("ListWithLimit", func(t *testing.T) { testListWithLimit(t, tc) })
	t.Run("GetBatch", func(t *testing.T) { testGetBatch(t, tc) })
	t.Run("SetBatch", func(t *testing.T) { testSetBatch(t, tc) })
}

// HookedValue is a model with lifecycle hooks for integration testing.
type HookedValue struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`

	afterLoadCalled bool
}

func (h *HookedValue) AfterLoad(_ context.Context) error {
	h.afterLoadCalled = true
	return nil
}

func (h *HookedValue) BeforeSave(_ context.Context) error { return nil }
func (h *HookedValue) AfterSave(_ context.Context) error  { return nil }

// FailingBeforeSaveValue always fails BeforeSave.
type FailingBeforeSaveValue struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

var errTestHook = errors.New("test hook error")

func (f *FailingBeforeSaveValue) BeforeSave(_ context.Context) error { return errTestHook }

// RunHookTests runs the lifecycle hook test suite for KV stores.
func RunHookTests(t *testing.T, tc *TestContext) {
	t.Run("AfterLoadOnGet", func(t *testing.T) { testHookAfterLoadGet(t, tc) })
	t.Run("AfterLoadOnGetBatch", func(t *testing.T) { testHookAfterLoadGetBatch(t, tc) })
	t.Run("BeforeSaveOnSet", func(t *testing.T) { testHookBeforeSaveSet(t, tc) })
	t.Run("BeforeSaveErrorAborts", func(t *testing.T) { testHookBeforeSaveError(t, tc) })
}

func testHookAfterLoadGet(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[HookedValue](tc.Provider)

	value := &HookedValue{ID: "hook-1", Name: "Hook Value", Count: 42}
	if err := store.Set(ctx, "hook-get-key", value, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := store.Get(ctx, "hook-get-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !got.afterLoadCalled {
		t.Error("AfterLoad not called on Get")
	}
	if got.Name != "Hook Value" {
		t.Errorf("expected name 'Hook Value', got %q", got.Name)
	}
}

func testHookAfterLoadGetBatch(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[HookedValue](tc.Provider)

	a := &HookedValue{ID: "ha", Name: "A", Count: 1}
	b := &HookedValue{ID: "hb", Name: "B", Count: 2}
	if err := store.Set(ctx, "hook-batch-a", a, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := store.Set(ctx, "hook-batch-b", b, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	results, err := store.GetBatch(ctx, []string{"hook-batch-a", "hook-batch-b"})
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	for k, v := range results {
		if !v.afterLoadCalled {
			t.Errorf("AfterLoad not called for key %s", k)
		}
	}
}

func testHookBeforeSaveSet(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[HookedValue](tc.Provider)

	value := &HookedValue{ID: "save-1", Name: "Saved", Count: 10}
	if err := store.Set(ctx, "hook-save-key", value, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := store.Get(ctx, "hook-save-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Name != "Saved" {
		t.Errorf("expected name 'Saved', got %q", got.Name)
	}
}

func testHookBeforeSaveError(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[FailingBeforeSaveValue](tc.Provider)

	value := &FailingBeforeSaveValue{ID: "fail-1", Name: "Fail"}
	err := store.Set(ctx, "hook-fail-key", value, 0)
	if !errors.Is(err, errTestHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}

	// Verify nothing was persisted
	store2 := grub.NewStore[TestValue](tc.Provider)
	_, err = store2.Get(ctx, "hook-fail-key")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound (record should not exist), got: %v", err)
	}
}

// --- CRUD Tests ---

func testGetNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	_, err := store.Get(ctx, "nonexistent-key")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func testSetAndGet(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	value := &TestValue{
		ID:    "test-1",
		Name:  "Test Value",
		Count: 42,
	}

	err := store.Set(ctx, "key-1", value, 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := store.Get(ctx, "key-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.ID != value.ID {
		t.Errorf("expected ID %q, got %q", value.ID, got.ID)
	}
	if got.Name != value.Name {
		t.Errorf("expected Name %q, got %q", value.Name, got.Name)
	}
	if got.Count != value.Count {
		t.Errorf("expected Count %d, got %d", value.Count, got.Count)
	}
}

func testSetOverwrite(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	original := &TestValue{ID: "orig", Name: "Original", Count: 1}
	updated := &TestValue{ID: "upd", Name: "Updated", Count: 2}

	err := store.Set(ctx, "overwrite-key", original, 0)
	if err != nil {
		t.Fatalf("Set original failed: %v", err)
	}

	err = store.Set(ctx, "overwrite-key", updated, 0)
	if err != nil {
		t.Fatalf("Set updated failed: %v", err)
	}

	got, err := store.Get(ctx, "overwrite-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Name != "Updated" {
		t.Errorf("expected Name 'Updated', got %q", got.Name)
	}
	if got.Count != 2 {
		t.Errorf("expected Count 2, got %d", got.Count)
	}
}

func testDelete(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	value := &TestValue{ID: "del", Name: "To Delete", Count: 0}
	err := store.Set(ctx, "delete-key", value, 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = store.Delete(ctx, "delete-key")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = store.Get(ctx, "delete-key")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func testDeleteNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	err := store.Delete(ctx, "nonexistent-delete-key")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func testExists(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	value := &TestValue{ID: "ex", Name: "Exists", Count: 1}
	err := store.Set(ctx, "exists-key", value, 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	exists, err := store.Exists(ctx, "exists-key")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected key to exist")
	}
}

func testExistsNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	exists, err := store.Exists(ctx, "nonexistent-exists-key")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected key to not exist")
	}
}

// --- Atomic Tests ---

func testAtomicGetSet(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	value := &TestValue{ID: "atomic-1", Name: "Atomic Value", Count: 100}
	err := store.Set(ctx, "atomic-key", value, 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	a, err := store.Atomic().Get(ctx, "atomic-key")
	if err != nil {
		t.Fatalf("Atomic().Get failed: %v", err)
	}

	if a.Strings["Name"] != "Atomic Value" {
		t.Errorf("expected atom Name 'Atomic Value', got %q", a.Strings["Name"])
	}
	if a.Ints["Count"] != 100 {
		t.Errorf("expected atom Count 100, got %d", a.Ints["Count"])
	}

	// Modify via atom
	a.Strings["Name"] = "Modified Atomic"
	a.Ints["Count"] = 200

	err = store.Atomic().Set(ctx, "atomic-key", a, 0)
	if err != nil {
		t.Fatalf("Atomic().Set failed: %v", err)
	}

	got, err := store.Get(ctx, "atomic-key")
	if err != nil {
		t.Fatalf("Get after Atomic().Set failed: %v", err)
	}

	if got.Name != "Modified Atomic" {
		t.Errorf("expected Name 'Modified Atomic', got %q", got.Name)
	}
	if got.Count != 200 {
		t.Errorf("expected Count 200, got %d", got.Count)
	}
}

// --- TTL Tests ---

func testTTLExpiration(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	value := &TestValue{ID: "ttl", Name: "TTL Value", Count: 1}
	err := store.Set(ctx, "ttl-key", value, 1*time.Second)
	if err != nil {
		t.Fatalf("Set with TTL failed: %v", err)
	}

	// Should exist immediately
	exists, err := store.Exists(ctx, "ttl-key")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected key to exist immediately after set")
	}

	// Wait for expiration
	time.Sleep(1500 * time.Millisecond)

	// Should be gone
	exists, err = store.Exists(ctx, "ttl-key")
	if err != nil {
		t.Fatalf("Exists after TTL failed: %v", err)
	}
	if exists {
		t.Error("expected key to be expired")
	}
}

// --- Batch Tests ---

func testList(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	// Set up test data with a common prefix
	for i := 0; i < 5; i++ {
		key := "list-prefix-" + string(rune('a'+i))
		value := &TestValue{ID: key, Name: "List Value", Count: i}
		if err := store.Set(ctx, key, value, 0); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Also set a key without the prefix
	other := &TestValue{ID: "other", Name: "Other", Count: 99}
	if err := store.Set(ctx, "other-key", other, 0); err != nil {
		t.Fatalf("Set other failed: %v", err)
	}

	keys, err := store.List(ctx, "list-prefix-", 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(keys) != 5 {
		t.Errorf("expected 5 keys, got %d", len(keys))
	}
}

func testListWithLimit(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	// Set up test data
	for i := 0; i < 10; i++ {
		key := "limit-prefix-" + string(rune('a'+i))
		value := &TestValue{ID: key, Name: "Limit Value", Count: i}
		if err := store.Set(ctx, key, value, 0); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	keys, err := store.List(ctx, "limit-prefix-", 3)
	if err != nil {
		t.Fatalf("List with limit failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys with limit, got %d", len(keys))
	}
}

func testGetBatch(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	// Set up test data
	expected := map[string]*TestValue{
		"batch-get-1": {ID: "bg1", Name: "Batch Get 1", Count: 1},
		"batch-get-2": {ID: "bg2", Name: "Batch Get 2", Count: 2},
		"batch-get-3": {ID: "bg3", Name: "Batch Get 3", Count: 3},
	}

	for k, v := range expected {
		if err := store.Set(ctx, k, v, 0); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Get batch including one non-existent key
	keys := []string{"batch-get-1", "batch-get-2", "batch-get-3", "batch-get-missing"}
	result, err := store.GetBatch(ctx, keys)
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("expected 3 results, got %d", len(result))
	}

	for k, exp := range expected {
		got, ok := result[k]
		if !ok {
			t.Errorf("missing key %q in result", k)
			continue
		}
		if got.ID != exp.ID || got.Name != exp.Name || got.Count != exp.Count {
			t.Errorf("key %q: expected %+v, got %+v", k, exp, got)
		}
	}
}

func testSetBatch(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	store := grub.NewStore[TestValue](tc.Provider)

	items := map[string]*TestValue{
		"batch-set-1": {ID: "bs1", Name: "Batch Set 1", Count: 10},
		"batch-set-2": {ID: "bs2", Name: "Batch Set 2", Count: 20},
		"batch-set-3": {ID: "bs3", Name: "Batch Set 3", Count: 30},
	}

	err := store.SetBatch(ctx, items, 0)
	if err != nil {
		t.Fatalf("SetBatch failed: %v", err)
	}

	// Verify each was stored
	for k, exp := range items {
		got, err := store.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get %q failed: %v", k, err)
		}
		if got.ID != exp.ID || got.Name != exp.Name || got.Count != exp.Count {
			t.Errorf("key %q: expected %+v, got %+v", k, exp, got)
		}
	}
}
