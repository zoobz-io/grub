// Package bucket provides shared test infrastructure for grub Bucket integration tests.
package bucket

import (
	"bytes"
	"context"
	"errors"
	"io"
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

// RunListTests runs the list operation test suite.
func RunListTests(t *testing.T, tc *TestContext) {
	t.Run("List", func(t *testing.T) { testList(t, tc) })
	t.Run("ListWithLimit", func(t *testing.T) { testListWithLimit(t, tc) })
}

// RunStatTests runs the Stat and LastModified test suite.
func RunStatTests(t *testing.T, tc *TestContext) {
	t.Run("Stat", func(t *testing.T) { testStat(t, tc) })
	t.Run("StatNotFound", func(t *testing.T) { testStatNotFound(t, tc) })
	t.Run("LastModified", func(t *testing.T) { testLastModified(t, tc) })
}

// RunStreamTests runs the GetStream/PutStream test suite.
func RunStreamTests(t *testing.T, tc *TestContext) {
	t.Run("StreamRoundTrip", func(t *testing.T) { testStreamRoundTrip(t, tc) })
	t.Run("GetStreamNotFound", func(t *testing.T) { testGetStreamNotFound(t, tc) })
}

// RunPaginationTests runs the ListPage cursor pagination test suite.
func RunPaginationTests(t *testing.T, tc *TestContext) {
	t.Run("ListPage", func(t *testing.T) { testListPage(t, tc) })
}

// RunHierarchyTests runs the ListLevel hierarchy listing test suite.
func RunHierarchyTests(t *testing.T, tc *TestContext) {
	t.Run("ListLevel", func(t *testing.T) { testListLevel(t, tc) })
	t.Run("ListLevelPaged", func(t *testing.T) { testListLevelPaged(t, tc) })
	t.Run("ListLevelDelimiter", func(t *testing.T) { testListLevelDelimiter(t, tc) })
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

// --- Stat / LastModified Tests ---

func testStat(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	obj := &grub.Object[TestPayload]{
		Key:         "stat-key",
		ContentType: "application/json",
		Data:        TestPayload{ID: "s", Name: "Stat", Count: 7},
	}
	if err := bucket.Put(ctx, obj); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	info, err := bucket.Stat(ctx, "stat-key")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Key != "stat-key" {
		t.Errorf("expected key 'stat-key', got %q", info.Key)
	}
	if info.Size == 0 {
		t.Error("expected non-zero size")
	}
	if info.LastModified.IsZero() {
		t.Error("expected non-zero LastModified from Stat")
	}
}

func testStatNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	_, err := bucket.Stat(ctx, "stat-missing-key")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func testLastModified(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	obj := &grub.Object[TestPayload]{
		Key:         "lastmod-key",
		ContentType: "application/json",
		Data:        TestPayload{ID: "lm", Name: "LastMod", Count: 1},
	}
	if err := bucket.Put(ctx, obj); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := bucket.Get(ctx, "lastmod-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.LastModified.IsZero() {
		t.Error("expected non-zero LastModified from Get")
	}

	infos, err := bucket.List(ctx, "lastmod-key", 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(infos) == 0 {
		t.Fatal("expected at least one listing")
	}
	for _, info := range infos {
		if info.LastModified.IsZero() {
			t.Errorf("expected non-zero LastModified from List for %q", info.Key)
		}
	}
}

// --- Stream Tests ---

func testStreamRoundTrip(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	// PutStream/GetStream are raw access: they bypass the codec.
	payload := []byte("raw stream bytes of unknown length")
	if err := bucket.PutStream(ctx, "stream-key", bytes.NewReader(payload), &grub.ObjectInfo{Key: "stream-key"}); err != nil {
		t.Fatalf("PutStream failed: %v", err)
	}

	r, info, err := bucket.GetStream(ctx, "stream-key")
	if err != nil {
		t.Fatalf("GetStream failed: %v", err)
	}
	defer func() { _ = r.Close() }()
	if info.Key != "stream-key" {
		t.Errorf("expected key 'stream-key', got %q", info.Key)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("stream round-trip mismatch: got %q, want %q", got, payload)
	}
}

func testGetStreamNotFound(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	_, _, err := bucket.GetStream(ctx, "stream-missing-key")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// --- Pagination Tests ---

func testListPage(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	keys := []string{"pagetest/a", "pagetest/b", "pagetest/c"}
	for _, k := range keys {
		obj := &grub.Object[TestPayload]{
			Key:         k,
			ContentType: "application/json",
			Data:        TestPayload{ID: k, Name: "Page", Count: 1},
		}
		if err := bucket.Put(ctx, obj); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Limit 1 over 3 keys yields all 3 across pages, then an empty cursor.
	seen := make(map[string]bool)
	cursor := ""
	calls := 0
	for {
		infos, next, err := bucket.ListPage(ctx, "pagetest/", cursor, 1)
		if err != nil {
			t.Fatalf("ListPage failed: %v", err)
		}
		calls++
		for _, info := range infos {
			seen[info.Key] = true
		}
		if next == "" {
			break
		}
		cursor = next
		if calls > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != len(keys) {
		t.Errorf("expected %d unique keys, got %d (%v)", len(keys), len(seen), seen)
	}
}

// --- Hierarchy Tests ---

func testListLevel(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	keys := []string{"levels/x", "levels/sub/y", "levels/sub/z"}
	for _, k := range keys {
		obj := &grub.Object[TestPayload]{
			Key:         k,
			ContentType: "application/json",
			Data:        TestPayload{ID: k, Name: "Level", Count: 1},
		}
		if err := bucket.Put(ctx, obj); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	level, err := bucket.ListLevel(ctx, "levels/", "/", "", 0)
	if err != nil {
		t.Fatalf("ListLevel failed: %v", err)
	}
	if len(level.Objects) != 1 || level.Objects[0].Key != "levels/x" {
		t.Errorf("expected objects [levels/x], got %+v", level.Objects)
	}
	if len(level.Prefixes) != 1 || level.Prefixes[0] != "levels/sub/" {
		t.Errorf("expected prefixes [levels/sub/], got %v", level.Prefixes)
	}
}

// testListLevelPaged verifies that ListLevel pages common prefixes correctly
// when the limit is smaller than the number of prefixes — the cursor must not
// re-emit a prefix already seen on an earlier page.
func testListLevelPaged(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	// Three distinct sub-prefixes under "paged/": p1/, p2/, p3/.
	keys := []string{"paged/p1/a", "paged/p2/b", "paged/p3/c"}
	for _, k := range keys {
		obj := &grub.Object[TestPayload]{
			Key:         k,
			ContentType: "application/json",
			Data:        TestPayload{ID: k, Name: "Paged", Count: 1},
		}
		if err := bucket.Put(ctx, obj); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	want := map[string]bool{"paged/p1/": false, "paged/p2/": false, "paged/p3/": false}
	seen := make(map[string]int)
	cursor := ""
	calls := 0
	for {
		level, err := bucket.ListLevel(ctx, "paged/", "/", cursor, 1)
		if err != nil {
			t.Fatalf("ListLevel failed: %v", err)
		}
		calls++
		for _, p := range level.Prefixes {
			seen[p]++
		}
		if level.Next == "" {
			break
		}
		cursor = level.Next
		if calls > 10 {
			t.Fatal("hierarchy pagination did not terminate")
		}
	}

	for p := range want {
		if seen[p] == 0 {
			t.Errorf("prefix %q never returned", p)
		}
		if seen[p] > 1 {
			t.Errorf("prefix %q returned %d times (duplicated across pages)", p, seen[p])
		}
	}
	if len(seen) != len(want) {
		t.Errorf("expected prefixes %v, got %v", want, seen)
	}
}

// testListLevelDelimiter verifies that ListLevel honors a non-"/" delimiter.
func testListLevelDelimiter(t *testing.T, tc *TestContext) {
	ctx := context.Background()
	bucket := grub.NewBucket[TestPayload](tc.Provider)

	// Under prefix "dash-": "dash-file" is a leaf; "dash-sub-y"/"dash-sub-z"
	// collapse into the common prefix "dash-sub-".
	keys := []string{"dash-file", "dash-sub-y", "dash-sub-z"}
	for _, k := range keys {
		obj := &grub.Object[TestPayload]{
			Key:         k,
			ContentType: "application/json",
			Data:        TestPayload{ID: k, Name: "Dash", Count: 1},
		}
		if err := bucket.Put(ctx, obj); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	level, err := bucket.ListLevel(ctx, "dash-", "-", "", 0)
	if err != nil {
		t.Fatalf("ListLevel failed: %v", err)
	}
	if len(level.Objects) != 1 || level.Objects[0].Key != "dash-file" {
		t.Errorf("expected objects [dash-file], got %+v", level.Objects)
	}
	if len(level.Prefixes) != 1 || level.Prefixes[0] != "dash-sub-" {
		t.Errorf("expected prefixes [dash-sub-], got %v", level.Prefixes)
	}
}
