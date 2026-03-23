// Package bucket provides shared test infrastructure for grub Bucket integration tests.
package bucket

import (
	"context"
	"errors"
	"testing"

	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/sentinel"
)

func init() {
	sentinel.Tag("json")
}

// TestPayload is the model used for Bucket integration tests.
type TestPayload struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// TestContext holds shared test resources for a provider.
type TestContext struct {
	Provider grub.BucketProvider
	Cleanup  func() // optional cleanup function
}

// RunCRUDTests runs the core CRUD test suite against the given context.
func RunCRUDTests(t *testing.T, tc *TestContext) {
	t.Run("GetNotFound", func(t *testing.T) { testGetNotFound(t, tc) })
	t.Run("PutAndGet", func(t *testing.T) { testPutAndGet(t, tc) })
	t.Run("PutOverwrite", func(t *testing.T) { testPutOverwrite(t, tc) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, tc) })
	t.Run("DeleteNotFound", func(t *testing.T) { testDeleteNotFound(t, tc) })
	t.Run("Exists", func(t *testing.T) { testExists(t, tc) })
	t.Run("ExistsNotFound", func(t *testing.T) { testExistsNotFound(t, tc) })
}

// RunMetadataTests runs metadata-specific tests.
func RunMetadataTests(t *testing.T, tc *TestContext) {
	t.Run("ContentType", func(t *testing.T) { testContentType(t, tc) })
	t.Run("CustomMetadata", func(t *testing.T) { testCustomMetadata(t, tc) })
}

// RunContentTypeTest runs only the content type test.
func RunContentTypeTest(t *testing.T, tc *TestContext) {
	testContentType(t, tc)
}

// RunAtomicTests runs the atomic bucket test suite.
func RunAtomicTests(t *testing.T, tc *TestContext) {
	t.Run("AtomicGetPut", func(t *testing.T) { testAtomicGetPut(t, tc) })
}

// RunListTests runs the list operation test suite.
func RunListTests(t *testing.T, tc *TestContext) {
	t.Run("List", func(t *testing.T) { testList(t, tc) })
	t.Run("ListWithLimit", func(t *testing.T) { testListWithLimit(t, tc) })
}

// HookedPayload is a model with lifecycle hooks for integration testing.
type HookedPayload struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`

	afterLoadCalled bool
}

func (h *HookedPayload) AfterLoad(_ context.Context) error {
	h.afterLoadCalled = true
	return nil
}

func (h *HookedPayload) BeforeSave(_ context.Context) error { return nil }
func (h *HookedPayload) AfterSave(_ context.Context) error  { return nil }

// FailingBeforeSavePayload always fails BeforeSave.
type FailingBeforeSavePayload struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

var errTestHook = errors.New("test hook error")

func (f *FailingBeforeSavePayload) BeforeSave(_ context.Context) error { return errTestHook }

// RunHookTests runs the lifecycle hook test suite for Buckets.
func RunHookTests(t *testing.T, tc *TestContext) {
	t.Run("AfterLoadOnGet", func(t *testing.T) { testHookAfterLoadGet(t, tc) })
	t.Run("BeforeSaveOnPut", func(t *testing.T) { testHookBeforeSavePut(t, tc) })
	t.Run("BeforeSaveErrorAborts", func(t *testing.T) { testHookBeforeSaveError(t, tc) })
}

func testHookAfterLoadGet(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[HookedPayload](tc.Provider)

	obj := &grub.Object[HookedPayload]{
		Key:         "hook-get-key",
		ContentType: "application/json",
		Data:        HookedPayload{ID: "h1", Name: "Hook", Count: 1},
	}
	if err := bucket.Put(ctx, obj); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := bucket.Get(ctx, "hook-get-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !got.Data.afterLoadCalled {
		t.Error("AfterLoad not called on Get")
	}
	if got.Data.Name != "Hook" {
		t.Errorf("expected name 'Hook', got %q", got.Data.Name)
	}
}

func testHookBeforeSavePut(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[HookedPayload](tc.Provider)

	obj := &grub.Object[HookedPayload]{
		Key:         "hook-put-key",
		ContentType: "application/json",
		Data:        HookedPayload{ID: "s1", Name: "Saved", Count: 10},
	}
	if err := bucket.Put(ctx, obj); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := bucket.Get(ctx, "hook-put-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Data.Name != "Saved" {
		t.Errorf("expected name 'Saved', got %q", got.Data.Name)
	}
}

func testHookBeforeSaveError(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[FailingBeforeSavePayload](tc.Provider)

	obj := &grub.Object[FailingBeforeSavePayload]{
		Key:         "hook-fail-key",
		ContentType: "application/json",
		Data:        FailingBeforeSavePayload{ID: "f1", Name: "Fail"},
	}
	err := bucket.Put(ctx, obj)
	if !errors.Is(err, errTestHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}

	// Verify nothing was persisted
	bucket2 := grub.NewBucket[TestPayload](tc.Provider)
	_, err = bucket2.Get(ctx, "hook-fail-key")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound (record should not exist), got: %v", err)
	}
}

// --- CRUD Tests ---

func testGetNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	_, err := bucket.Get(ctx, "nonexistent-key")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func testPutAndGet(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	obj := &grub.Object[TestPayload]{
		Key:         "key-1",
		ContentType: "application/json",
		Data: TestPayload{
			ID:    "test-1",
			Name:  "Test Value",
			Count: 42,
		},
	}

	err := bucket.Put(ctx, obj)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := bucket.Get(ctx, "key-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Data.ID != obj.Data.ID {
		t.Errorf("expected ID %q, got %q", obj.Data.ID, got.Data.ID)
	}
	if got.Data.Name != obj.Data.Name {
		t.Errorf("expected Name %q, got %q", obj.Data.Name, got.Data.Name)
	}
	if got.Data.Count != obj.Data.Count {
		t.Errorf("expected Count %d, got %d", obj.Data.Count, got.Data.Count)
	}
}

func testPutOverwrite(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	original := &grub.Object[TestPayload]{
		Key:         "overwrite-key",
		ContentType: "application/json",
		Data:        TestPayload{ID: "orig", Name: "Original", Count: 1},
	}
	updated := &grub.Object[TestPayload]{
		Key:         "overwrite-key",
		ContentType: "application/json",
		Data:        TestPayload{ID: "upd", Name: "Updated", Count: 2},
	}

	err := bucket.Put(ctx, original)
	if err != nil {
		t.Fatalf("Put original failed: %v", err)
	}

	err = bucket.Put(ctx, updated)
	if err != nil {
		t.Fatalf("Put updated failed: %v", err)
	}

	got, err := bucket.Get(ctx, "overwrite-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Data.Name != "Updated" {
		t.Errorf("expected Name 'Updated', got %q", got.Data.Name)
	}
	if got.Data.Count != 2 {
		t.Errorf("expected Count 2, got %d", got.Data.Count)
	}
}

func testDelete(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	obj := &grub.Object[TestPayload]{
		Key:         "delete-key",
		ContentType: "application/json",
		Data:        TestPayload{ID: "del", Name: "To Delete", Count: 0},
	}
	err := bucket.Put(ctx, obj)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = bucket.Delete(ctx, "delete-key")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = bucket.Get(ctx, "delete-key")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func testDeleteNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	err := bucket.Delete(ctx, "nonexistent-delete-key")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func testExists(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	obj := &grub.Object[TestPayload]{
		Key:         "exists-key",
		ContentType: "application/json",
		Data:        TestPayload{ID: "ex", Name: "Exists", Count: 1},
	}
	err := bucket.Put(ctx, obj)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	exists, err := bucket.Exists(ctx, "exists-key")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected key to exist")
	}
}

func testExistsNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	exists, err := bucket.Exists(ctx, "nonexistent-exists-key")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected key to not exist")
	}
}

// --- Metadata Tests ---

func testContentType(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	obj := &grub.Object[TestPayload]{
		Key:         "content-type-key",
		ContentType: "application/x-custom",
		Data:        TestPayload{ID: "ct", Name: "Content Type", Count: 1},
	}

	err := bucket.Put(ctx, obj)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := bucket.Get(ctx, "content-type-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.ContentType != "application/x-custom" {
		t.Errorf("expected ContentType 'application/x-custom', got %q", got.ContentType)
	}
}

func testCustomMetadata(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	// Use alphanumeric keys for Azure compatibility
	obj := &grub.Object[TestPayload]{
		Key:         "metadata-key",
		ContentType: "application/json",
		Metadata: map[string]string{
			"customheader": "custom-value",
			"another":      "another-value",
		},
		Data: TestPayload{ID: "meta", Name: "Metadata", Count: 1},
	}

	err := bucket.Put(ctx, obj)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := bucket.Get(ctx, "metadata-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Metadata == nil {
		t.Fatal("expected Metadata to be set")
	}
	if got.Metadata["customheader"] != "custom-value" {
		t.Errorf("expected customheader 'custom-value', got %q", got.Metadata["customheader"])
	}
	if got.Metadata["another"] != "another-value" {
		t.Errorf("expected another 'another-value', got %q", got.Metadata["another"])
	}
}

// --- Atomic Tests ---

func testAtomicGetPut(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	obj := &grub.Object[TestPayload]{
		Key:         "atomic-key",
		ContentType: "application/json",
		Data:        TestPayload{ID: "atomic-1", Name: "Atomic Value", Count: 100},
	}
	err := bucket.Put(ctx, obj)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	atomicObj, err := bucket.Atomic().Get(ctx, "atomic-key")
	if err != nil {
		t.Fatalf("Atomic().Get failed: %v", err)
	}

	// Metadata is preserved as-is
	if atomicObj.ContentType != "application/json" {
		t.Errorf("expected ContentType 'application/json', got %q", atomicObj.ContentType)
	}

	// Payload is atomized - access fields via atom
	if atomicObj.Data.Strings["Name"] != "Atomic Value" {
		t.Errorf("expected atom Name 'Atomic Value', got %q", atomicObj.Data.Strings["Name"])
	}
	if atomicObj.Data.Ints["Count"] != 100 {
		t.Errorf("expected atom Count 100, got %d", atomicObj.Data.Ints["Count"])
	}

	// Modify payload via atom
	atomicObj.Data.Strings["Name"] = "Modified Atomic"
	atomicObj.Data.Ints["Count"] = 200
	// Also modify metadata
	atomicObj.ContentType = "text/plain"

	err = bucket.Atomic().Put(ctx, "atomic-key", atomicObj)
	if err != nil {
		t.Fatalf("Atomic().Put failed: %v", err)
	}

	got, err := bucket.Get(ctx, "atomic-key")
	if err != nil {
		t.Fatalf("Get after Atomic().Put failed: %v", err)
	}

	if got.ContentType != "text/plain" {
		t.Errorf("expected ContentType 'text/plain', got %q", got.ContentType)
	}
	if got.Data.Name != "Modified Atomic" {
		t.Errorf("expected Name 'Modified Atomic', got %q", got.Data.Name)
	}
	if got.Data.Count != 200 {
		t.Errorf("expected Count 200, got %d", got.Data.Count)
	}
}

// --- List Tests ---

func testList(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	// Set up test data with a common prefix
	for i := 0; i < 5; i++ {
		key := "list-prefix-" + string(rune('a'+i))
		obj := &grub.Object[TestPayload]{
			Key:         key,
			ContentType: "application/json",
			Data:        TestPayload{ID: key, Name: "List Value", Count: i},
		}
		if err := bucket.Put(ctx, obj); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Also set an object without the prefix
	other := &grub.Object[TestPayload]{
		Key:         "other-key",
		ContentType: "application/json",
		Data:        TestPayload{ID: "other", Name: "Other", Count: 99},
	}
	if err := bucket.Put(ctx, other); err != nil {
		t.Fatalf("Put other failed: %v", err)
	}

	infos, err := bucket.List(ctx, "list-prefix-", 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(infos) != 5 {
		t.Errorf("expected 5 objects, got %d", len(infos))
	}
}

func testListWithLimit(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	// Set up test data
	for i := 0; i < 10; i++ {
		key := "limit-prefix-" + string(rune('a'+i))
		obj := &grub.Object[TestPayload]{
			Key:         key,
			ContentType: "application/json",
			Data:        TestPayload{ID: key, Name: "Limit Value", Count: i},
		}
		if err := bucket.Put(ctx, obj); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	infos, err := bucket.List(ctx, "limit-prefix-", 3)
	if err != nil {
		t.Fatalf("List with limit failed: %v", err)
	}

	if len(infos) != 3 {
		t.Errorf("expected 3 objects with limit, got %d", len(infos))
	}
}
