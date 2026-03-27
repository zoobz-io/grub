package grub

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	astqlsqlite "github.com/zoobz-io/astql/sqlite"
	"github.com/zoobz-io/grub/internal/mockdb"
)

var errHook = errors.New("hook error")

// hookedRecord implements all lifecycle hooks and tracks calls.
type hookedRecord struct {
	ID   int    `json:"id" atom:"id"`
	Name string `json:"name" atom:"name"`

	beforeSaveCalled   bool
	afterSaveCalled    bool
	afterLoadCalled    bool
	beforeDeleteCalled bool
	afterDeleteCalled  bool
}

func (h *hookedRecord) BeforeSave(_ context.Context) error {
	h.beforeSaveCalled = true
	return nil
}

func (h *hookedRecord) AfterSave(_ context.Context) error {
	h.afterSaveCalled = true
	return nil
}

func (h *hookedRecord) AfterLoad(_ context.Context) error {
	h.afterLoadCalled = true
	return nil
}

func (h *hookedRecord) BeforeDelete(_ context.Context) error {
	h.beforeDeleteCalled = true
	return nil
}

func (h *hookedRecord) AfterDelete(_ context.Context) error {
	h.afterDeleteCalled = true
	return nil
}

// failingBeforeSave implements BeforeSave that returns an error.
type failingBeforeSave struct {
	ID   int    `json:"id" atom:"id"`
	Name string `json:"name" atom:"name"`
}

func (*failingBeforeSave) BeforeSave(_ context.Context) error { return errHook }

// failingAfterSave implements AfterSave that returns an error.
type failingAfterSave struct {
	ID   int    `json:"id" atom:"id"`
	Name string `json:"name" atom:"name"`
}

func (*failingAfterSave) AfterSave(_ context.Context) error { return errHook }

// failingAfterLoad implements AfterLoad that returns an error.
type failingAfterLoad struct {
	ID   int    `json:"id" atom:"id"`
	Name string `json:"name" atom:"name"`
}

func (*failingAfterLoad) AfterLoad(_ context.Context) error { return errHook }

// failingBeforeDelete implements BeforeDelete that returns an error.
type failingBeforeDelete struct {
	ID   int    `json:"id" atom:"id"`
	Name string `json:"name" atom:"name"`
}

func (*failingBeforeDelete) BeforeDelete(_ context.Context) error { return errHook }

// failingAfterDelete implements AfterDelete that returns an error.
type failingAfterDelete struct {
	ID   int    `json:"id" atom:"id"`
	Name string `json:"name" atom:"name"`
}

func (*failingAfterDelete) AfterDelete(_ context.Context) error { return errHook }

// failingBeforeSaveDBUser is a Database-compatible model whose BeforeSave fails.
type failingBeforeSaveDBUser struct {
	ID    int    `db:"id" constraints:"primarykey"`
	Email string `db:"email" constraints:"notnull,unique"`
	Name  string `db:"name" constraints:"notnull"`
	Age   *int   `db:"age"`
}

func (*failingBeforeSaveDBUser) BeforeSave(_ context.Context) error { return errHook }

// failingBeforeDeleteDBUser is a Database-compatible model whose BeforeDelete fails.
type failingBeforeDeleteDBUser struct {
	ID    int    `db:"id" constraints:"primarykey"`
	Email string `db:"email" constraints:"notnull,unique"`
	Name  string `db:"name" constraints:"notnull"`
	Age   *int   `db:"age"`
}

func (*failingBeforeDeleteDBUser) BeforeDelete(_ context.Context) error { return errHook }

// ============================================================
// Helper function tests
// ============================================================

func TestCallBeforeSave_NoInterface(t *testing.T) {
	rec := &testRecord{ID: 1, Name: "test"}
	if err := callBeforeSave(context.Background(), rec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCallAfterSave_NoInterface(t *testing.T) {
	rec := &testRecord{ID: 1, Name: "test"}
	if err := callAfterSave(context.Background(), rec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCallAfterLoad_NoInterface(t *testing.T) {
	rec := &testRecord{ID: 1, Name: "test"}
	if err := callAfterLoad(context.Background(), rec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCallBeforeDelete_NoInterface(t *testing.T) {
	if err := callBeforeDelete[testRecord](context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCallAfterDelete_NoInterface(t *testing.T) {
	if err := callAfterDelete[testRecord](context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCallAfterLoadSlice(t *testing.T) {
	ctx := context.Background()
	values := []*hookedRecord{
		{ID: 1, Name: "a"},
		{ID: 2, Name: "b"},
	}
	if err := callAfterLoadSlice(ctx, values); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, v := range values {
		if !v.afterLoadCalled {
			t.Errorf("AfterLoad not called on element %d", i)
		}
	}
}

func TestCallAfterLoadSlice_Error(t *testing.T) {
	ctx := context.Background()
	values := []*failingAfterLoad{{ID: 1, Name: "a"}}
	err := callAfterLoadSlice(ctx, values)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestCallAfterLoadSlice_Empty(t *testing.T) {
	if err := callAfterLoadSlice[hookedRecord](context.Background(), nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ============================================================
// Database[T] hook tests (mockdb — query capture only)
// ============================================================

func TestDatabaseHooks_SetBeforeSaveError(t *testing.T) {
	db, _ := mockdb.New()
	d := NewDatabase[failingBeforeSaveDBUser](db, "test_users", astqlsqlite.New())
	ctx := context.Background()
	rec := &failingBeforeSaveDBUser{ID: 1, Email: "a@b.c", Name: "test"}
	err := d.Set(ctx, "1", rec)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestDatabaseHooks_SetTxBeforeSaveError(t *testing.T) {
	db, _ := mockdb.New()
	d := NewDatabase[failingBeforeSaveDBUser](db, "test_users", astqlsqlite.New())
	ctx := context.Background()
	tx, _ := db.Beginx()
	rec := &failingBeforeSaveDBUser{ID: 1, Email: "a@b.c", Name: "test"}
	err := d.SetTx(ctx, tx, "1", rec)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestDatabaseHooks_DeleteBeforeDeleteError(t *testing.T) {
	db, _ := mockdb.New()
	d := NewDatabase[failingBeforeDeleteDBUser](db, "test_users", astqlsqlite.New())
	ctx := context.Background()
	err := d.Delete(ctx, "1")
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestDatabaseHooks_DeleteTxBeforeDeleteError(t *testing.T) {
	db, _ := mockdb.New()
	d := NewDatabase[failingBeforeDeleteDBUser](db, "test_users", astqlsqlite.New())
	ctx := context.Background()
	tx, _ := db.Beginx()
	err := d.DeleteTx(ctx, tx, "1")
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

// ============================================================
// Store[T] hook tests
// ============================================================

func TestStoreHooks_SetCallsBeforeAndAfterSave(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[hookedRecord](provider)
	ctx := context.Background()

	rec := &hookedRecord{ID: 1, Name: "test"}
	if err := store.Set(ctx, "k", rec, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rec.beforeSaveCalled {
		t.Error("BeforeSave not called")
	}
	if !rec.afterSaveCalled {
		t.Error("AfterSave not called")
	}
}

func TestStoreHooks_SetBeforeSaveError(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[failingBeforeSave](provider)
	ctx := context.Background()

	rec := &failingBeforeSave{ID: 1, Name: "test"}
	err := store.Set(ctx, "k", rec, 0)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
	if _, ok := provider.data["k"]; ok {
		t.Error("provider.Set should not have been called")
	}
}

func TestStoreHooks_SetAfterSaveError(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[failingAfterSave](provider)
	ctx := context.Background()

	rec := &failingAfterSave{ID: 1, Name: "test"}
	err := store.Set(ctx, "k", rec, 0)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestStoreHooks_GetCallsAfterLoad(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[hookedRecord](provider)
	ctx := context.Background()

	data, _ := JSONCodec{}.Encode(&hookedRecord{ID: 1, Name: "test"})
	provider.data["k"] = data

	result, err := store.Get(ctx, "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.afterLoadCalled {
		t.Error("AfterLoad not called")
	}
}

func TestStoreHooks_GetAfterLoadError(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[failingAfterLoad](provider)
	ctx := context.Background()

	data, _ := JSONCodec{}.Encode(&failingAfterLoad{ID: 1, Name: "test"})
	provider.data["k"] = data

	_, err := store.Get(ctx, "k")
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestStoreHooks_DeleteCallsHooks(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[hookedRecord](provider)
	ctx := context.Background()

	provider.data["k"] = []byte(`{}`)

	if err := store.Delete(ctx, "k"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStoreHooks_DeleteBeforeDeleteError(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[failingBeforeDelete](provider)
	ctx := context.Background()

	provider.data["k"] = []byte(`{}`)

	err := store.Delete(ctx, "k")
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
	if _, ok := provider.data["k"]; !ok {
		t.Error("provider.Delete should not have been called")
	}
}

func TestStoreHooks_DeleteAfterDeleteError(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[failingAfterDelete](provider)
	ctx := context.Background()

	provider.data["k"] = []byte(`{}`)

	err := store.Delete(ctx, "k")
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestStoreHooks_GetBatchCallsAfterLoad(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[hookedRecord](provider)
	ctx := context.Background()

	data, _ := JSONCodec{}.Encode(&hookedRecord{ID: 1, Name: "a"})
	provider.data["a"] = data
	data, _ = JSONCodec{}.Encode(&hookedRecord{ID: 2, Name: "b"})
	provider.data["b"] = data

	results, err := store.GetBatch(ctx, []string{"a", "b"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for k, v := range results {
		if !v.afterLoadCalled {
			t.Errorf("AfterLoad not called for key %s", k)
		}
	}
}

func TestStoreHooks_GetBatchAfterLoadError(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[failingAfterLoad](provider)
	ctx := context.Background()

	data, _ := JSONCodec{}.Encode(&failingAfterLoad{ID: 1, Name: "a"})
	provider.data["a"] = data

	_, err := store.GetBatch(ctx, []string{"a"})
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestStoreHooks_SetBatchCallsBeforeAndAfterSave(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[hookedRecord](provider)
	ctx := context.Background()

	items := map[string]*hookedRecord{
		"a": {ID: 1, Name: "a"},
		"b": {ID: 2, Name: "b"},
	}
	if err := store.SetBatch(ctx, items, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for k, v := range items {
		if !v.beforeSaveCalled {
			t.Errorf("BeforeSave not called for key %s", k)
		}
		if !v.afterSaveCalled {
			t.Errorf("AfterSave not called for key %s", k)
		}
	}
}

func TestStoreHooks_SetBatchBeforeSaveError(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[failingBeforeSave](provider)
	ctx := context.Background()

	items := map[string]*failingBeforeSave{"a": {ID: 1, Name: "a"}}
	err := store.SetBatch(ctx, items, 0)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestStoreHooks_SetBatchAfterSaveError(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[failingAfterSave](provider)
	ctx := context.Background()

	items := map[string]*failingAfterSave{"a": {ID: 1, Name: "a"}}
	err := store.SetBatch(ctx, items, 0)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestStoreHooks_NoHookType(t *testing.T) {
	provider := newMockStoreProvider()
	store := NewStore[testRecord](provider)
	ctx := context.Background()

	rec := &testRecord{ID: 1, Name: "test"}
	if err := store.Set(ctx, "k", rec, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := store.Get(ctx, "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Name != "test" {
		t.Errorf("expected name 'test', got %q", result.Name)
	}
	if err := store.Delete(ctx, "k"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ============================================================
// Bucket[T] hook tests
// ============================================================

func TestBucketHooks_PutCallsBeforeAndAfterSave(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[hookedRecord](provider)
	ctx := context.Background()

	obj := &Object[hookedRecord]{
		Key:         "k",
		ContentType: "application/json",
		Data:        hookedRecord{ID: 1, Name: "test"},
	}
	if err := bucket.Put(ctx, obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obj.Data.beforeSaveCalled {
		t.Error("BeforeSave not called")
	}
	if !obj.Data.afterSaveCalled {
		t.Error("AfterSave not called")
	}
}

func TestBucketHooks_PutBeforeSaveError(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[failingBeforeSave](provider)
	ctx := context.Background()

	obj := &Object[failingBeforeSave]{Key: "k", Data: failingBeforeSave{ID: 1, Name: "test"}}
	err := bucket.Put(ctx, obj)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestBucketHooks_PutAfterSaveError(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[failingAfterSave](provider)
	ctx := context.Background()

	obj := &Object[failingAfterSave]{Key: "k", Data: failingAfterSave{ID: 1, Name: "test"}}
	err := bucket.Put(ctx, obj)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestBucketHooks_GetCallsAfterLoad(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[hookedRecord](provider)
	ctx := context.Background()

	data, _ := JSONCodec{}.Encode(&hookedRecord{ID: 1, Name: "test"})
	provider.data["k"] = data
	provider.info["k"] = &ObjectInfo{Key: "k", ContentType: "application/json", Size: int64(len(data))}

	result, err := bucket.Get(ctx, "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Data.afterLoadCalled {
		t.Error("AfterLoad not called")
	}
}

func TestBucketHooks_GetAfterLoadError(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[failingAfterLoad](provider)
	ctx := context.Background()

	data, _ := JSONCodec{}.Encode(&failingAfterLoad{ID: 1, Name: "test"})
	provider.data["k"] = data
	provider.info["k"] = &ObjectInfo{Key: "k", ContentType: "application/json", Size: int64(len(data))}

	_, err := bucket.Get(ctx, "k")
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestBucketHooks_DeleteBeforeDeleteError(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[failingBeforeDelete](provider)
	ctx := context.Background()

	provider.data["k"] = []byte(`{}`)
	provider.info["k"] = &ObjectInfo{Key: "k"}

	err := bucket.Delete(ctx, "k")
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestBucketHooks_DeleteAfterDeleteError(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[failingAfterDelete](provider)
	ctx := context.Background()

	provider.data["k"] = []byte(`{}`)
	provider.info["k"] = &ObjectInfo{Key: "k"}

	err := bucket.Delete(ctx, "k")
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestBucketHooks_NoHookType(t *testing.T) {
	provider := newMockBucketProvider()
	bucket := NewBucket[testRecord](provider)
	ctx := context.Background()

	obj := &Object[testRecord]{
		Key:         "k",
		ContentType: "application/json",
		Data:        testRecord{ID: 1, Name: "test"},
	}
	if err := bucket.Put(ctx, obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := bucket.Get(ctx, "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Data.Name != "test" {
		t.Errorf("expected name 'test', got %q", result.Data.Name)
	}
}

// ============================================================
// Index[T] hook tests
// ============================================================

func TestIndexHooks_UpsertCallsBeforeAndAfterSave(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[hookedRecord](provider)
	ctx := context.Background()

	meta := &hookedRecord{ID: 1, Name: "test"}
	if err := index.Upsert(ctx, uuid.New(), []float32{1.0, 2.0}, meta); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !meta.beforeSaveCalled {
		t.Error("BeforeSave not called")
	}
	if !meta.afterSaveCalled {
		t.Error("AfterSave not called")
	}
}

func TestIndexHooks_UpsertBeforeSaveError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingBeforeSave](provider)
	ctx := context.Background()

	meta := &failingBeforeSave{ID: 1, Name: "test"}
	err := index.Upsert(ctx, uuid.New(), []float32{1.0}, meta)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_UpsertAfterSaveError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingAfterSave](provider)
	ctx := context.Background()

	meta := &failingAfterSave{ID: 1, Name: "test"}
	err := index.Upsert(ctx, uuid.New(), []float32{1.0}, meta)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_UpsertBatchCallsBeforeAndAfterSave(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[hookedRecord](provider)
	ctx := context.Background()

	vectors := []Vector[hookedRecord]{
		{ID: uuid.New(), Vector: []float32{1.0}, Metadata: hookedRecord{ID: 1, Name: "a"}},
		{ID: uuid.New(), Vector: []float32{2.0}, Metadata: hookedRecord{ID: 2, Name: "b"}},
	}
	if err := index.UpsertBatch(ctx, vectors); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, v := range vectors {
		if !v.Metadata.beforeSaveCalled {
			t.Errorf("BeforeSave not called for vector %d", i)
		}
		if !v.Metadata.afterSaveCalled {
			t.Errorf("AfterSave not called for vector %d", i)
		}
	}
}

func TestIndexHooks_UpsertBatchBeforeSaveError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingBeforeSave](provider)
	ctx := context.Background()

	vectors := []Vector[failingBeforeSave]{
		{ID: uuid.New(), Vector: []float32{1.0}, Metadata: failingBeforeSave{ID: 1}},
	}
	err := index.UpsertBatch(ctx, vectors)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_UpsertBatchAfterSaveError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingAfterSave](provider)
	ctx := context.Background()

	vectors := []Vector[failingAfterSave]{
		{ID: uuid.New(), Vector: []float32{1.0}, Metadata: failingAfterSave{ID: 1}},
	}
	err := index.UpsertBatch(ctx, vectors)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_DeleteBatchProviderError(t *testing.T) {
	provider := newMockVectorProvider()
	provider.deleteErr = errors.New("provider error")
	index := NewIndex[testRecord](provider)
	ctx := context.Background()

	err := index.DeleteBatch(ctx, []uuid.UUID{uuid.New()})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestIndexHooks_GetCallsAfterLoad(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[hookedRecord](provider)
	ctx := context.Background()

	id := uuid.New()
	meta, _ := JSONCodec{}.Encode(&hookedRecord{ID: 1, Name: "test"})
	provider.vectors[id] = vectorEntry{vector: []float32{1.0}, metadata: meta}

	result, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Metadata.afterLoadCalled {
		t.Error("AfterLoad not called")
	}
}

func TestIndexHooks_GetAfterLoadError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingAfterLoad](provider)
	ctx := context.Background()

	id := uuid.New()
	meta, _ := JSONCodec{}.Encode(&failingAfterLoad{ID: 1, Name: "test"})
	provider.vectors[id] = vectorEntry{vector: []float32{1.0}, metadata: meta}

	_, err := index.Get(ctx, id)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_SearchCallsAfterLoad(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[hookedRecord](provider)
	ctx := context.Background()

	meta, _ := JSONCodec{}.Encode(&hookedRecord{ID: 1, Name: "test"})
	provider.vectors[uuid.New()] = vectorEntry{vector: []float32{1.0, 0.0}, metadata: meta}
	meta, _ = JSONCodec{}.Encode(&hookedRecord{ID: 2, Name: "test2"})
	provider.vectors[uuid.New()] = vectorEntry{vector: []float32{0.0, 1.0}, metadata: meta}

	results, err := index.Search(ctx, []float32{1.0, 0.0}, 2, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, v := range results {
		if !v.Metadata.afterLoadCalled {
			t.Errorf("AfterLoad not called for result %d", i)
		}
	}
}

func TestIndexHooks_SearchAfterLoadError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingAfterLoad](provider)
	ctx := context.Background()

	meta, _ := JSONCodec{}.Encode(&failingAfterLoad{ID: 1})
	provider.vectors[uuid.New()] = vectorEntry{vector: []float32{1.0}, metadata: meta}

	_, err := index.Search(ctx, []float32{1.0}, 1, nil)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_QueryCallsAfterLoad(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[hookedRecord](provider)
	ctx := context.Background()

	meta, _ := JSONCodec{}.Encode(&hookedRecord{ID: 1, Name: "test"})
	provider.vectors[uuid.New()] = vectorEntry{vector: []float32{1.0}, metadata: meta}

	results, err := index.Query(ctx, []float32{1.0}, 1, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].Metadata.afterLoadCalled {
		t.Error("AfterLoad not called")
	}
}

func TestIndexHooks_QueryAfterLoadError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingAfterLoad](provider)
	ctx := context.Background()

	meta, _ := JSONCodec{}.Encode(&failingAfterLoad{ID: 1})
	provider.vectors[uuid.New()] = vectorEntry{vector: []float32{1.0}, metadata: meta}

	_, err := index.Query(ctx, []float32{1.0}, 1, nil)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_FilterCallsAfterLoad(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[hookedRecord](provider)
	ctx := context.Background()

	meta, _ := JSONCodec{}.Encode(&hookedRecord{ID: 1, Name: "test"})
	provider.vectors[uuid.New()] = vectorEntry{vector: []float32{1.0}, metadata: meta}

	results, err := index.Filter(ctx, nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].Metadata.afterLoadCalled {
		t.Error("AfterLoad not called")
	}
}

func TestIndexHooks_FilterAfterLoadError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingAfterLoad](provider)
	ctx := context.Background()

	meta, _ := JSONCodec{}.Encode(&failingAfterLoad{ID: 1})
	provider.vectors[uuid.New()] = vectorEntry{vector: []float32{1.0}, metadata: meta}

	_, err := index.Filter(ctx, nil, 0)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_DeleteBeforeDeleteError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingBeforeDelete](provider)
	ctx := context.Background()

	id := uuid.New()
	provider.vectors[id] = vectorEntry{vector: []float32{1.0}}

	err := index.Delete(ctx, id)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_DeleteAfterDeleteError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingAfterDelete](provider)
	ctx := context.Background()

	id := uuid.New()
	provider.vectors[id] = vectorEntry{vector: []float32{1.0}}

	err := index.Delete(ctx, id)
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_DeleteBatchBeforeDeleteError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingBeforeDelete](provider)
	ctx := context.Background()

	err := index.DeleteBatch(ctx, []uuid.UUID{uuid.New()})
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_DeleteBatchAfterDeleteError(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[failingAfterDelete](provider)
	ctx := context.Background()

	err := index.DeleteBatch(ctx, []uuid.UUID{uuid.New()})
	if !errors.Is(err, errHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}
}

func TestIndexHooks_NoHookType(t *testing.T) {
	provider := newMockVectorProvider()
	index := NewIndex[testRecord](provider)
	ctx := context.Background()

	id := uuid.New()
	meta := &testRecord{ID: 1, Name: "test"}
	if err := index.Upsert(ctx, id, []float32{1.0}, meta); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Metadata.Name != "test" {
		t.Errorf("expected name 'test', got %q", result.Metadata.Name)
	}
}
