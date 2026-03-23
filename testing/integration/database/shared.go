// Package database provides shared test infrastructure for grub database integration tests.
package database

import (
	"context"
	"errors"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobz-io/astql"
	"github.com/zoobz-io/atom"
	"github.com/zoobz-io/edamame"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/sentinel"
)

func init() {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
}

// intPtr returns a pointer to an int.
func intPtr(i int) *int {
	return &i
}

// TestUser is the model used for integration tests.
type TestUser struct {
	ID    int    `db:"id" constraints:"primarykey"`
	Email string `db:"email" constraints:"notnull,unique"`
	Name  string `db:"name" constraints:"notnull"`
	Age   *int   `db:"age"`
}

// TestContext holds shared test resources for a dialect.
type TestContext struct {
	DB            *sqlx.DB
	Renderer      astql.Renderer
	ResetSQL      string // SQL to drop/recreate test_users table
	InsertUserSQL string // SQL to insert a user with explicit ID (for MSSQL IDENTITY_INSERT)
}

// Reset drops and recreates the test_users table.
func (tc *TestContext) Reset(t *testing.T) {
	t.Helper()
	_, err := tc.DB.Exec(tc.ResetSQL)
	if err != nil {
		t.Fatalf("failed to reset table: %v", err)
	}
}

// InsertUser inserts a user with explicit ID using dialect-specific SQL.
func (tc *TestContext) InsertUser(t *testing.T, id int, email, name string, age int) {
	t.Helper()
	sql := tc.InsertUserSQL
	if sql == "" {
		// Default for dialects that don't need special handling
		sql = `INSERT INTO test_users (id, email, name, age) VALUES ($1, $2, $3, $4)`
	}
	_, err := tc.DB.Exec(sql, id, email, name, age)
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}
}

// RunCRUDTests runs the core CRUD test suite against the given context.
func RunCRUDTests(t *testing.T, tc *TestContext) {
	t.Run("Get", func(t *testing.T) { testGet(t, tc) })
	t.Run("GetAtom", func(t *testing.T) { testGetAtom(t, tc) })
	t.Run("Set", func(t *testing.T) { testSet(t, tc) })
	t.Run("SetUpdate", func(t *testing.T) { testSetUpdate(t, tc) })
	t.Run("SetAtom", func(t *testing.T) { testSetAtom(t, tc) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, tc) })
	t.Run("DeleteNotFound", func(t *testing.T) { testDeleteNotFound(t, tc) })
}

// RunQueryTests runs the query engine test suite.
func RunQueryTests(t *testing.T, tc *TestContext) {
	t.Run("Query", func(t *testing.T) { testQuery(t, tc) })
	t.Run("QueryWithStatement", func(t *testing.T) { testQueryWithStatement(t, tc) })
	t.Run("QueryAtom", func(t *testing.T) { testQueryAtom(t, tc) })
	t.Run("Select", func(t *testing.T) { testSelect(t, tc) })
	t.Run("SelectAtom", func(t *testing.T) { testSelectAtom(t, tc) })
	t.Run("Update", func(t *testing.T) { testUpdate(t, tc) })
	t.Run("Aggregate", func(t *testing.T) { testAggregate(t, tc) })
	t.Run("AggregateSum", func(t *testing.T) { testAggregateSum(t, tc) })
	t.Run("QueryPagination", func(t *testing.T) { testQueryPagination(t, tc) })
}

// HookedUser is a model with lifecycle hooks for integration testing.
// It uses the same table schema as TestUser.
type HookedUser struct {
	ID    int    `db:"id" constraints:"primarykey"`
	Email string `db:"email" constraints:"notnull,unique"`
	Name  string `db:"name" constraints:"notnull"`
	Age   *int   `db:"age"`

	afterLoadCalled bool
}

func (h *HookedUser) AfterLoad(_ context.Context) error {
	h.afterLoadCalled = true
	return nil
}

func (h *HookedUser) BeforeSave(_ context.Context) error { return nil }
func (h *HookedUser) AfterSave(_ context.Context) error  { return nil }

// FailingBeforeSaveUser always fails BeforeSave.
type FailingBeforeSaveUser struct {
	ID    int    `db:"id" constraints:"primarykey"`
	Email string `db:"email" constraints:"notnull,unique"`
	Name  string `db:"name" constraints:"notnull"`
	Age   *int   `db:"age"`
}

var errTestHook = errors.New("test hook error")

func (f *FailingBeforeSaveUser) BeforeSave(_ context.Context) error { return errTestHook }

// RunHookTests runs the lifecycle hook test suite.
func RunHookTests(t *testing.T, tc *TestContext) {
	t.Run("AfterLoadOnGet", func(t *testing.T) { testHookAfterLoadGet(t, tc) })
	t.Run("AfterLoadOnExecQuery", func(t *testing.T) { testHookAfterLoadExecQuery(t, tc) })
	t.Run("AfterLoadOnExecSelect", func(t *testing.T) { testHookAfterLoadExecSelect(t, tc) })
	t.Run("BeforeSaveOnSet", func(t *testing.T) { testHookBeforeSaveSet(t, tc) })
	t.Run("BeforeSaveErrorAborts", func(t *testing.T) { testHookBeforeSaveError(t, tc) })
}

func testHookAfterLoadGet(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	tc.InsertUser(t, 1, "hook@example.com", "Hook User", 25)

	db, err := grub.NewDatabase[HookedUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	user, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !user.afterLoadCalled {
		t.Error("AfterLoad not called on Get")
	}
	if user.Email != "hook@example.com" {
		t.Errorf("expected email 'hook@example.com', got %q", user.Email)
	}
}

func testHookAfterLoadExecQuery(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	tc.InsertUser(t, 1, "a@example.com", "A", 20)
	tc.InsertUser(t, 2, "b@example.com", "B", 30)

	db, err := grub.NewDatabase[HookedUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	results, err := db.ExecQuery(ctx, grub.QueryAll, nil)
	if err != nil {
		t.Fatalf("ExecQuery failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.afterLoadCalled {
			t.Errorf("AfterLoad not called on result %d", i)
		}
	}
}

func testHookAfterLoadExecSelect(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	tc.InsertUser(t, 1, "sel@example.com", "Select User", 25)

	db, err := grub.NewDatabase[HookedUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	stmt := edamame.NewSelectStatement("by_email", "Select by email", edamame.SelectSpec{
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})

	result, err := db.ExecSelect(ctx, stmt, map[string]any{"email": "sel@example.com"})
	if err != nil {
		t.Fatalf("ExecSelect failed: %v", err)
	}

	if !result.afterLoadCalled {
		t.Error("AfterLoad not called on ExecSelect result")
	}
}

func testHookBeforeSaveSet(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	db, err := grub.NewDatabase[HookedUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	age := 25
	user := &HookedUser{ID: 1, Email: "save@example.com", Name: "Saved", Age: &age}
	if err := db.Set(ctx, "1", user); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Name != "Saved" {
		t.Errorf("expected name 'Saved', got %q", got.Name)
	}
}

func testHookBeforeSaveError(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	db, err := grub.NewDatabase[FailingBeforeSaveUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	user := &FailingBeforeSaveUser{ID: 1, Email: "fail@example.com", Name: "Fail"}
	err = db.Set(ctx, "1", user)
	if !errors.Is(err, errTestHook) {
		t.Fatalf("expected hook error, got: %v", err)
	}

	// Verify nothing was persisted
	db2, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	_, err = db2.Get(ctx, "1")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound (record should not exist), got: %v", err)
	}
}

// --- CRUD Tests ---

func testGet(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`INSERT INTO test_users (email, name, age) VALUES ('test@example.com', 'Test User', 25)`)
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	user, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if user.Email != "test@example.com" {
		t.Errorf("expected email 'test@example.com', got %q", user.Email)
	}
	if user.Name != "Test User" {
		t.Errorf("expected name 'Test User', got %q", user.Name)
	}
	if user.Age == nil || *user.Age != 25 {
		t.Errorf("expected age 25, got %v", user.Age)
	}
}

func testGetAtom(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`INSERT INTO test_users (email, name, age) VALUES ('atom@example.com', 'Atom User', 30)`)
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	a, err := db.Atomic().Get(ctx, "1")
	if err != nil {
		t.Fatalf("Atomic().Get failed: %v", err)
	}

	if a.Strings["Email"] != "atom@example.com" {
		t.Errorf("expected atom email 'atom@example.com', got %q", a.Strings["Email"])
	}
	if a.Strings["Name"] != "Atom User" {
		t.Errorf("expected atom name 'Atom User', got %q", a.Strings["Name"])
	}
	if a.IntPtrs["Age"] == nil || *a.IntPtrs["Age"] != 30 {
		t.Errorf("expected atom age 30, got %v", a.IntPtrs["Age"])
	}
}

func testSet(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	user := &TestUser{
		ID:    1,
		Email: "set@example.com",
		Name:  "Set User",
		Age:   intPtr(35),
	}
	err = db.Set(ctx, "1", user)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get after Set failed: %v", err)
	}

	if got.Email != "set@example.com" {
		t.Errorf("expected email 'set@example.com', got %q", got.Email)
	}
	if got.Name != "Set User" {
		t.Errorf("expected name 'Set User', got %q", got.Name)
	}
	if got.Age == nil || *got.Age != 35 {
		t.Errorf("expected age 35, got %v", got.Age)
	}
}

func testSetUpdate(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	tc.InsertUser(t, 1, "old@example.com", "Old Name", 20)

	updated := &TestUser{
		ID:    1,
		Email: "new@example.com",
		Name:  "New Name",
		Age:   intPtr(40),
	}
	err = db.Set(ctx, "1", updated)
	if err != nil {
		t.Fatalf("Set (update) failed: %v", err)
	}

	got, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}

	if got.Email != "new@example.com" {
		t.Errorf("expected email 'new@example.com', got %q", got.Email)
	}
	if got.Name != "New Name" {
		t.Errorf("expected name 'New Name', got %q", got.Name)
	}
	if got.Age == nil || *got.Age != 40 {
		t.Errorf("expected age 40, got %v", got.Age)
	}
}

func testSetAtom(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	a := &atom.Atom{
		Ints:    map[string]int64{"ID": 1},
		Strings: map[string]string{"Email": "atom-set@example.com", "Name": "Atom Set User"},
		IntPtrs: map[string]*int64{},
	}
	age := int64(50)
	a.IntPtrs["Age"] = &age

	err = db.Atomic().Set(ctx, "1", a)
	if err != nil {
		t.Fatalf("Atomic().Set failed: %v", err)
	}

	got, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get after SetAtom failed: %v", err)
	}

	if got.Email != "atom-set@example.com" {
		t.Errorf("expected email 'atom-set@example.com', got %q", got.Email)
	}
	if got.Name != "Atom Set User" {
		t.Errorf("expected name 'Atom Set User', got %q", got.Name)
	}
	if got.Age == nil || *got.Age != 50 {
		t.Errorf("expected age 50, got %v", got.Age)
	}
}

func testDelete(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`INSERT INTO test_users (email, name, age) VALUES ('delete@example.com', 'Delete Me', 40)`)
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	err = db.Delete(ctx, "1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = db.Get(ctx, "1")
	if err == nil {
		t.Error("expected error after delete, got nil")
	}
}

func testDeleteNotFound(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	err = db.Delete(ctx, "999")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// --- Query Tests ---

func testQuery(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('alice@example.com', 'Alice', 25),
		('bob@example.com', 'Bob', 30),
		('carol@example.com', 'Carol', 35)
	`)
	if err != nil {
		t.Fatalf("failed to insert test records: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	users, err := db.ExecQuery(ctx, grub.QueryAll, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(users) != 3 {
		t.Errorf("expected 3 users, got %d", len(users))
	}
}

func testQueryWithStatement(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('alice@example.com', 'Alice', 25),
		('bob@example.com', 'Bob', 30),
		('carol@example.com', 'Carol', 35)
	`)
	if err != nil {
		t.Fatalf("failed to insert test records: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	stmt := edamame.NewQueryStatement("by-min-age", "Users with age >= min_age", edamame.QuerySpec{
		Where: []edamame.ConditionSpec{
			{Field: "age", Operator: ">=", Param: "min_age"},
		},
		OrderBy: []edamame.OrderBySpec{
			{Field: "age", Direction: "asc"},
		},
	})

	users, err := db.ExecQuery(ctx, stmt, map[string]any{"min_age": 30})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(users) != 2 {
		t.Errorf("expected 2 users (age >= 30), got %d", len(users))
	}

	if users[0].Name != "Bob" {
		t.Errorf("expected first user to be Bob, got %s", users[0].Name)
	}
}

func testQueryAtom(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('alice@example.com', 'Alice', 25),
		('bob@example.com', 'Bob', 30)
	`)
	if err != nil {
		t.Fatalf("failed to insert test records: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	atoms, err := db.Atomic().ExecQuery(ctx, grub.QueryAll, nil)
	if err != nil {
		t.Fatalf("Atomic().Query failed: %v", err)
	}

	if len(atoms) != 2 {
		t.Errorf("expected 2 atoms, got %d", len(atoms))
	}

	names := make(map[string]bool)
	for _, a := range atoms {
		names[a.Strings["Name"]] = true
	}
	if !names["Alice"] || !names["Bob"] {
		t.Errorf("expected Alice and Bob in results, got %v", names)
	}
}

func testSelect(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('alice@example.com', 'Alice', 25),
		('bob@example.com', 'Bob', 30)
	`)
	if err != nil {
		t.Fatalf("failed to insert test records: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	stmt := edamame.NewSelectStatement("by-email", "Find user by email", edamame.SelectSpec{
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})

	user, err := db.ExecSelect(ctx, stmt, map[string]any{"email": "bob@example.com"})
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	if user.Name != "Bob" {
		t.Errorf("expected name 'Bob', got %q", user.Name)
	}
	if user.Age == nil || *user.Age != 30 {
		t.Errorf("expected age 30, got %v", user.Age)
	}
}

func testSelectAtom(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`INSERT INTO test_users (email, name, age) VALUES ('carol@example.com', 'Carol', 35)`)
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	stmt := edamame.NewSelectStatement("by-name", "Find user by name", edamame.SelectSpec{
		Where: []edamame.ConditionSpec{
			{Field: "name", Operator: "=", Param: "name"},
		},
	})

	a, err := db.Atomic().ExecSelect(ctx, stmt, map[string]any{"name": "Carol"})
	if err != nil {
		t.Fatalf("Atomic().Select failed: %v", err)
	}

	if a.Strings["Name"] != "Carol" {
		t.Errorf("expected name 'Carol', got %q", a.Strings["Name"])
	}
	if a.IntPtrs["Age"] == nil || *a.IntPtrs["Age"] != 35 {
		t.Errorf("expected age 35, got %v", a.IntPtrs["Age"])
	}
}

func testUpdate(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`INSERT INTO test_users (email, name, age) VALUES ('update@example.com', 'Original', 20)`)
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	stmt := edamame.NewUpdateStatement("rename-by-email", "Update user name by email", edamame.UpdateSpec{
		Set: map[string]string{
			"name": "new_name",
		},
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})

	updated, err := db.ExecUpdate(ctx, stmt, map[string]any{
		"email":    "update@example.com",
		"new_name": "Updated",
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if updated.Name != "Updated" {
		t.Errorf("expected name 'Updated', got %q", updated.Name)
	}

	got, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get after Update failed: %v", err)
	}
	if got.Name != "Updated" {
		t.Errorf("expected name 'Updated' after Get, got %q", got.Name)
	}
}

func testAggregate(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('alice@example.com', 'Alice', 25),
		('bob@example.com', 'Bob', 30),
		('carol@example.com', 'Carol', 35)
	`)
	if err != nil {
		t.Fatalf("failed to insert test records: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	count, err := db.ExecAggregate(ctx, grub.CountAll, nil)
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if count != 3 {
		t.Errorf("expected count 3, got %v", count)
	}
}

func testAggregateSum(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('a@example.com', 'A', 10),
		('b@example.com', 'B', 20),
		('c@example.com', 'C', 30)
	`)
	if err != nil {
		t.Fatalf("failed to insert test records: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	stmt := edamame.NewAggregateStatement("sum-age", "Sum of all ages", edamame.AggSum, edamame.AggregateSpec{
		Field: "age",
	})

	sum, err := db.ExecAggregate(ctx, stmt, nil)
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if sum != 60 {
		t.Errorf("expected sum 60, got %v", sum)
	}
}

func testQueryPagination(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('a@example.com', 'Alice', 25),
		('b@example.com', 'Bob', 30),
		('c@example.com', 'Carol', 35),
		('d@example.com', 'Dave', 40),
		('e@example.com', 'Eve', 45)
	`)
	if err != nil {
		t.Fatalf("failed to insert test records: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	stmt := edamame.NewQueryStatement("paginated", "Paginated users ordered by age", edamame.QuerySpec{
		OrderBy: []edamame.OrderBySpec{
			{Field: "age", Direction: "asc"},
		},
		LimitParam:  "limit",
		OffsetParam: "offset",
	})

	page1, err := db.ExecQuery(ctx, stmt, map[string]any{"limit": 2, "offset": 0})
	if err != nil {
		t.Fatalf("Query page 1 failed: %v", err)
	}
	if len(page1) != 2 {
		t.Errorf("expected 2 users on page 1, got %d", len(page1))
	}
	if page1[0].Name != "Alice" {
		t.Errorf("expected first user Alice, got %s", page1[0].Name)
	}

	page2, err := db.ExecQuery(ctx, stmt, map[string]any{"limit": 2, "offset": 2})
	if err != nil {
		t.Fatalf("Query page 2 failed: %v", err)
	}
	if len(page2) != 2 {
		t.Errorf("expected 2 users on page 2, got %d", len(page2))
	}
	if page2[0].Name != "Carol" {
		t.Errorf("expected first user on page 2 Carol, got %s", page2[0].Name)
	}
}

// RunTransactionTests runs the transaction test suite.
func RunTransactionTests(t *testing.T, tc *TestContext) {
	t.Run("GetTx", func(t *testing.T) { testGetTx(t, tc) })
	t.Run("SetTx", func(t *testing.T) { testSetTx(t, tc) })
	t.Run("DeleteTx", func(t *testing.T) { testDeleteTx(t, tc) })
	t.Run("TransactionCommit", func(t *testing.T) { testTransactionCommit(t, tc) })
	t.Run("TransactionRollback", func(t *testing.T) { testTransactionRollback(t, tc) })
	t.Run("QueryTx", func(t *testing.T) { testQueryTx(t, tc) })
	t.Run("UpdateTx", func(t *testing.T) { testUpdateTx(t, tc) })
	t.Run("AggregateTx", func(t *testing.T) { testAggregateTx(t, tc) })
}

// --- Transaction Tests ---

func testGetTx(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`INSERT INTO test_users (email, name, age) VALUES ('tx@example.com', 'Tx User', 30)`)
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	tx, err := tc.DB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	user, err := db.GetTx(ctx, tx, "1")
	if err != nil {
		t.Fatalf("GetTx failed: %v", err)
	}

	if user.Email != "tx@example.com" {
		t.Errorf("expected email 'tx@example.com', got %q", user.Email)
	}
}

func testSetTx(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	tx, err := tc.DB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}

	user := &TestUser{
		ID:    1,
		Email: "settx@example.com",
		Name:  "SetTx User",
		Age:   intPtr(25),
	}

	err = db.SetTx(ctx, tx, "1", user)
	if err != nil {
		tx.Rollback()
		t.Fatalf("SetTx failed: %v", err)
	}

	// Verify visible within transaction
	got, err := db.GetTx(ctx, tx, "1")
	if err != nil {
		tx.Rollback()
		t.Fatalf("GetTx after SetTx failed: %v", err)
	}

	if got.Email != "settx@example.com" {
		t.Errorf("expected email 'settx@example.com', got %q", got.Email)
	}

	tx.Commit()
}

func testDeleteTx(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`INSERT INTO test_users (email, name, age) VALUES ('delete@example.com', 'Delete User', 40)`)
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	tx, err := tc.DB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}

	err = db.DeleteTx(ctx, tx, "1")
	if err != nil {
		tx.Rollback()
		t.Fatalf("DeleteTx failed: %v", err)
	}

	// Verify deleted within transaction
	_, err = db.GetTx(ctx, tx, "1")
	if !errors.Is(err, grub.ErrNotFound) {
		tx.Rollback()
		t.Errorf("expected ErrNotFound after DeleteTx, got: %v", err)
	}

	tx.Commit()
}

func testTransactionCommit(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	tx, err := tc.DB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}

	user := &TestUser{
		ID:    1,
		Email: "commit@example.com",
		Name:  "Commit User",
		Age:   intPtr(35),
	}

	err = db.SetTx(ctx, tx, "1", user)
	if err != nil {
		tx.Rollback()
		t.Fatalf("SetTx failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify visible after commit (using non-Tx method)
	got, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get after commit failed: %v", err)
	}

	if got.Email != "commit@example.com" {
		t.Errorf("expected email 'commit@example.com', got %q", got.Email)
	}
}

func testTransactionRollback(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	tx, err := tc.DB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}

	user := &TestUser{
		ID:    1,
		Email: "rollback@example.com",
		Name:  "Rollback User",
		Age:   intPtr(45),
	}

	err = db.SetTx(ctx, tx, "1", user)
	if err != nil {
		tx.Rollback()
		t.Fatalf("SetTx failed: %v", err)
	}

	// Rollback instead of commit
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify NOT visible after rollback
	_, err = db.Get(ctx, "1")
	if !errors.Is(err, grub.ErrNotFound) {
		t.Errorf("expected ErrNotFound after rollback, got: %v", err)
	}
}

func testQueryTx(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('alice@example.com', 'Alice', 25),
		('bob@example.com', 'Bob', 30)
	`)
	if err != nil {
		t.Fatalf("failed to insert test records: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	tx, err := tc.DB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	users, err := db.ExecQueryTx(ctx, tx, grub.QueryAll, nil)
	if err != nil {
		t.Fatalf("QueryTx failed: %v", err)
	}

	if len(users) != 2 {
		t.Errorf("expected 2 users, got %d", len(users))
	}
}

func testUpdateTx(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`INSERT INTO test_users (email, name, age) VALUES ('update@example.com', 'Original', 20)`)
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	tx, err := tc.DB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}

	stmt := edamame.NewUpdateStatement("rename", "Update user name", edamame.UpdateSpec{
		Set: map[string]string{
			"name": "new_name",
		},
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})

	updated, err := db.ExecUpdateTx(ctx, tx, stmt, map[string]any{
		"email":    "update@example.com",
		"new_name": "TxUpdated",
	})
	if err != nil {
		tx.Rollback()
		t.Fatalf("UpdateTx failed: %v", err)
	}

	if updated.Name != "TxUpdated" {
		t.Errorf("expected name 'TxUpdated', got %q", updated.Name)
	}

	tx.Commit()

	// Verify persisted
	got, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get after commit failed: %v", err)
	}
	if got.Name != "TxUpdated" {
		t.Errorf("expected name 'TxUpdated' after commit, got %q", got.Name)
	}
}

func testAggregateTx(t *testing.T, tc *TestContext) {
	tc.Reset(t)
	ctx := context.Background()

	_, err := tc.DB.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('a@example.com', 'A', 10),
		('b@example.com', 'B', 20)
	`)
	if err != nil {
		t.Fatalf("failed to insert test records: %v", err)
	}

	db, err := grub.NewDatabase[TestUser](tc.DB, "test_users", tc.Renderer)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	tx, err := tc.DB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	count, err := db.ExecAggregateTx(ctx, tx, grub.CountAll, nil)
	if err != nil {
		t.Fatalf("AggregateTx failed: %v", err)
	}

	if count != 2 {
		t.Errorf("expected count 2, got %v", count)
	}
}
