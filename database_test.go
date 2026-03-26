package grub

import (
	"context"
	"errors"
	"strings"
	"testing"

	astqlsqlite "github.com/zoobz-io/astql/sqlite"
	"github.com/zoobz-io/edamame"
	"github.com/zoobz-io/grub/internal/mockdb"
	"github.com/zoobz-io/sentinel"
)

func init() {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
}

// TestDBUser is the model used for database tests.
type TestDBUser struct {
	ID    int    `db:"id" constraints:"primarykey"`
	Email string `db:"email" constraints:"notnull,unique"`
	Name  string `db:"name" constraints:"notnull"`
	Age   *int   `db:"age"`
}

var testDBRenderer = astqlsqlite.New()

func TestNewDatabase(t *testing.T) {
	mockDB, _ := mockdb.New()
	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}
	if db == nil {
		t.Fatal("NewDatabase returned nil")
	}
	if db.executor == nil {
		t.Error("executor not set")
	}
	if db.keyCol != "id" {
		t.Errorf("keyCol mismatch: got %q", db.keyCol)
	}
	if db.tableName != "test_users" {
		t.Errorf("tableName mismatch: got %q", db.tableName)
	}
}

func TestDatabase_Get(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Call Get - it will fail to find data but we can check the SQL
	_, _ = db.Get(ctx, "123")

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	// Verify SELECT query structure
	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"id"`) {
		t.Errorf("expected id column in WHERE clause, got: %s", query.Query)
	}
}

func TestDatabase_Set(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	user := &TestDBUser{
		ID:    1,
		Email: "test@example.com",
		Name:  "Test User",
		Age:   intPtr(30),
	}

	_ = db.Set(ctx, "1", user)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	// Verify INSERT/UPSERT query structure
	if !strings.Contains(query.Query, "INSERT") {
		t.Errorf("expected INSERT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, "ON CONFLICT") {
		t.Errorf("expected ON CONFLICT clause for upsert, got: %s", query.Query)
	}
}

func TestDatabase_Delete(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	_ = db.Delete(ctx, "123")

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	// Verify DELETE query structure
	if !strings.Contains(query.Query, "DELETE") {
		t.Errorf("expected DELETE query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"id"`) {
		t.Errorf("expected id column in WHERE clause, got: %s", query.Query)
	}
}

func TestDatabase_Exists(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	_, _ = db.Exists(ctx, "123")

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	// Verify SELECT with LIMIT query structure
	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, "LIMIT") {
		t.Errorf("expected LIMIT clause, got: %s", query.Query)
	}
}

func TestDatabase_Executor(t *testing.T) {
	mockDB, _ := mockdb.New()
	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	executor := db.Executor()
	if executor == nil {
		t.Error("Executor returned nil")
	}
}

// --- Builder Accessor Tests ---

func TestDatabase_QueryBuilder(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Use the Query builder directly
	_, _ = db.Query().
		Where("age", ">=", "min_age").
		Limit(10).
		Exec(ctx, map[string]any{"min_age": 25})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, "LIMIT") {
		t.Errorf("expected LIMIT clause, got: %s", query.Query)
	}
}

func TestDatabase_SelectBuilder(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Use the Select builder directly
	_, _ = db.Select().
		Where("email", "=", "email").
		Exec(ctx, map[string]any{"email": "test@example.com"})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"email"`) {
		t.Errorf("expected email column in query, got: %s", query.Query)
	}
}

func TestDatabase_InsertBuilder(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	user := &TestDBUser{
		Email: "insert@example.com",
		Name:  "Insert User",
		Age:   intPtr(30),
	}

	// Use the Insert builder directly (auto-gen PK)
	_, _ = db.Insert().Exec(ctx, user)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "INSERT") {
		t.Errorf("expected INSERT query, got: %s", query.Query)
	}
}

func TestDatabase_InsertFullBuilder(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	user := &TestDBUser{
		ID:    1,
		Email: "insertfull@example.com",
		Name:  "InsertFull User",
		Age:   intPtr(35),
	}

	// Use the InsertFull builder directly (include PK)
	_, _ = db.InsertFull().Exec(ctx, user)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "INSERT") {
		t.Errorf("expected INSERT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"id"`) {
		t.Errorf("expected id column in INSERT, got: %s", query.Query)
	}
}

func TestDatabase_ModifyBuilder(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Use the Modify builder directly
	_, _ = db.Modify().
		Set("name", "new_name").
		Where("id", "=", "user_id").
		Exec(ctx, map[string]any{"new_name": "Updated", "user_id": 1})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "UPDATE") {
		t.Errorf("expected UPDATE query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"name"`) {
		t.Errorf("expected name column in SET, got: %s", query.Query)
	}
}

func TestDatabase_RemoveBuilder(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Use the Remove builder directly
	_, _ = db.Remove().
		Where("id", "=", "user_id").
		Exec(ctx, map[string]any{"user_id": 1})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "DELETE") {
		t.Errorf("expected DELETE query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"id"`) {
		t.Errorf("expected id column in WHERE, got: %s", query.Query)
	}
}

func TestDatabase_CountBuilder(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Use the Count builder directly
	_, _ = db.Count().
		Where("age", ">=", "min_age").
		Exec(ctx, map[string]any{"min_age": 18})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "COUNT") {
		t.Errorf("expected COUNT in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"age"`) {
		t.Errorf("expected age column in WHERE, got: %s", query.Query)
	}
}

func TestDatabase_ExecQuery(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Use default QueryAll statement
	_, _ = db.ExecQuery(ctx, QueryAll, nil)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
}

func TestDatabase_ExecQueryWithStatement(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	stmt := edamame.NewQueryStatement("by-min-age", "Users with age >= min_age", edamame.QuerySpec{
		Where: []edamame.ConditionSpec{
			{Field: "age", Operator: ">=", Param: "min_age"},
		},
		OrderBy: []edamame.OrderBySpec{
			{Field: "age", Direction: "asc"},
		},
	})

	_, _ = db.ExecQuery(ctx, stmt, map[string]any{"min_age": 30})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, `"age"`) {
		t.Errorf("expected age column in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, ">=") {
		t.Errorf("expected >= operator in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, "ORDER BY") {
		t.Errorf("expected ORDER BY clause, got: %s", query.Query)
	}
}

func TestDatabase_ExecSelect(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	stmt := edamame.NewSelectStatement("by-email", "Find user by email", edamame.SelectSpec{
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})

	_, _ = db.ExecSelect(ctx, stmt, map[string]any{"email": "test@example.com"})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"email"`) {
		t.Errorf("expected email column in query, got: %s", query.Query)
	}
}

func TestDatabase_ExecUpdate(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	stmt := edamame.NewUpdateStatement("rename-by-email", "Update user name by email", edamame.UpdateSpec{
		Set: map[string]string{
			"name": "new_name",
		},
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})

	_, _ = db.ExecUpdate(ctx, stmt, map[string]any{
		"email":    "test@example.com",
		"new_name": "Updated",
	})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "UPDATE") {
		t.Errorf("expected UPDATE query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"name"`) {
		t.Errorf("expected name column in SET clause, got: %s", query.Query)
	}
}

func TestDatabase_ExecAggregate(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Default count aggregate
	_, _ = db.ExecAggregate(ctx, CountAll, nil)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "COUNT") {
		t.Errorf("expected COUNT in query, got: %s", query.Query)
	}
}

func TestDatabase_ExecAggregateSum(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	stmt := edamame.NewAggregateStatement("sum-age", "Sum of all ages", edamame.AggSum, edamame.AggregateSpec{
		Field: "age",
	})

	_, _ = db.ExecAggregate(ctx, stmt, nil)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SUM") {
		t.Errorf("expected SUM in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"age"`) {
		t.Errorf("expected age column in query, got: %s", query.Query)
	}
}

func TestDatabase_Atomic(t *testing.T) {
	mockDB, _ := mockdb.New()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	atomic := db.Atomic()
	if atomic == nil {
		t.Fatal("Atomic returned nil")
	}

	// Verify it returns the same instance
	atomic2 := db.Atomic()
	if atomic != atomic2 {
		t.Error("Atomic should return cached instance")
	}
}

func intPtr(i int) *int {
	return &i
}

func TestDatabase_Get_NotFound(t *testing.T) {
	mockDB, _ := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// mockdb returns empty rows, which should result in ErrNotFound
	_, err = db.Get(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for missing key")
	}
	// The error should be ErrNotFound (mapped from soy.ErrNotFound)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestDatabase_Get_QueryError(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Configure mock to return an error
	queryErr := errors.New("database connection error")
	cfg.SetQueryErr(queryErr)
	defer cfg.Reset()

	_, err = db.Get(ctx, "123")
	if err == nil {
		t.Error("expected query error")
	}
	if !strings.Contains(err.Error(), "database connection error") {
		t.Errorf("expected database error, got: %v", err)
	}
}

func TestDatabase_Delete_NotFound(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Configure mock to return 0 rows affected (simulates key not found)
	cfg.SetRowsAffected(0)
	defer cfg.Reset()

	err = db.Delete(ctx, "nonexistent")
	if err == nil {
		t.Error("expected ErrNotFound for 0 rows affected")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestDatabase_Delete_ExecError(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Configure mock to return an exec error
	execErr := errors.New("database exec error")
	cfg.SetExecErr(execErr)
	defer cfg.Reset()

	err = db.Delete(ctx, "123")
	if err == nil {
		t.Error("expected exec error")
	}
	if !strings.Contains(err.Error(), "database exec error") {
		t.Errorf("expected exec error, got: %v", err)
	}
}

func TestDatabase_Exists_QueryError(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Configure mock to return a query error
	queryErr := errors.New("exists query error")
	cfg.SetQueryErr(queryErr)
	defer cfg.Reset()

	_, err = db.Exists(ctx, "123")
	if err == nil {
		t.Error("expected query error")
	}
	if !strings.Contains(err.Error(), "exists query error") {
		t.Errorf("expected query error, got: %v", err)
	}
}

// --- Transaction Method Tests ---

func TestDatabase_GetTx(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	_, _ = db.GetTx(ctx, tx, "123")

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
}

func TestDatabase_SetTx(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	user := &TestDBUser{
		ID:    1,
		Email: "test@example.com",
		Name:  "Test User",
		Age:   intPtr(30),
	}

	_ = db.SetTx(ctx, tx, "1", user)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "INSERT") {
		t.Errorf("expected INSERT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, "ON CONFLICT") {
		t.Errorf("expected ON CONFLICT clause for upsert, got: %s", query.Query)
	}
}

func TestDatabase_DeleteTx(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	_ = db.DeleteTx(ctx, tx, "123")

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "DELETE") {
		t.Errorf("expected DELETE query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
}

func TestDatabase_ExistsTx(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	_, _ = db.ExistsTx(ctx, tx, "123")

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, "LIMIT") {
		t.Errorf("expected LIMIT clause, got: %s", query.Query)
	}
}

func TestDatabase_ExecQueryTx(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	_, _ = db.ExecQueryTx(ctx, tx, QueryAll, nil)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
}

func TestDatabase_ExecSelectTx(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	stmt := edamame.NewSelectStatement("by-email", "Find user by email", edamame.SelectSpec{
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})

	_, _ = db.ExecSelectTx(ctx, tx, stmt, map[string]any{"email": "test@example.com"})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"email"`) {
		t.Errorf("expected email column in query, got: %s", query.Query)
	}
}

func TestDatabase_ExecUpdateTx(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	stmt := edamame.NewUpdateStatement("rename-by-email", "Update user name by email", edamame.UpdateSpec{
		Set: map[string]string{
			"name": "new_name",
		},
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})

	_, _ = db.ExecUpdateTx(ctx, tx, stmt, map[string]any{
		"email":    "test@example.com",
		"new_name": "Updated",
	})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "UPDATE") {
		t.Errorf("expected UPDATE query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"name"`) {
		t.Errorf("expected name column in SET clause, got: %s", query.Query)
	}
}

func TestDatabase_ExecAggregateTx(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	_, _ = db.ExecAggregateTx(ctx, tx, CountAll, nil)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "COUNT") {
		t.Errorf("expected COUNT in query, got: %s", query.Query)
	}
}

func TestDatabase_GetTx_NotFound(t *testing.T) {
	mockDB, _ := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	_, err = db.GetTx(ctx, tx, "nonexistent")
	if err == nil {
		t.Error("expected error for missing key")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestDatabase_DeleteTx_NotFound(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	cfg.SetRowsAffected(0)
	defer cfg.Reset()

	err = db.DeleteTx(ctx, tx, "nonexistent")
	if err == nil {
		t.Error("expected ErrNotFound for 0 rows affected")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestDatabase_GetTx_QueryError(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	queryErr := errors.New("tx query error")
	cfg.SetQueryErr(queryErr)
	defer cfg.Reset()

	_, err = db.GetTx(ctx, tx, "123")
	if err == nil {
		t.Error("expected query error")
	}
	if !strings.Contains(err.Error(), "tx query error") {
		t.Errorf("expected tx query error, got: %v", err)
	}
}

func TestDatabase_DeleteTx_ExecError(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	execErr := errors.New("tx exec error")
	cfg.SetExecErr(execErr)
	defer cfg.Reset()

	err = db.DeleteTx(ctx, tx, "123")
	if err == nil {
		t.Error("expected exec error")
	}
	if !strings.Contains(err.Error(), "tx exec error") {
		t.Errorf("expected tx exec error, got: %v", err)
	}
}

func TestDatabase_ExistsTx_QueryError(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	queryErr := errors.New("tx exists query error")
	cfg.SetQueryErr(queryErr)
	defer cfg.Reset()

	_, err = db.ExistsTx(ctx, tx, "123")
	if err == nil {
		t.Error("expected query error")
	}
	if !strings.Contains(err.Error(), "tx exists query error") {
		t.Errorf("expected tx query error, got: %v", err)
	}
}

// --- Primary Key Detection Tests ---

type NoPKUser struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

type MultiplePKUser struct {
	ID1 int `db:"id1" constraints:"primarykey"`
	ID2 int `db:"id2" constraints:"primarykey"`
}

type CommaPKUser struct {
	ID   int    `db:"id" constraints:"primarykey,notnull"`
	Name string `db:"name"`
}

type PKNoDBTag struct {
	ID   int    `constraints:"primarykey"`
	Name string `db:"name"`
}

type PKIgnoredDBTag struct {
	ID   int    `db:"-" constraints:"primarykey"`
	Name string `db:"name"`
}

func TestNewDatabase_NoPrimaryKey(t *testing.T) {
	mockDB, _ := mockdb.New()
	_, err := NewDatabase[NoPKUser](mockDB, "test", testDBRenderer)
	if err == nil {
		t.Fatal("expected ErrNoPrimaryKey")
	}
	if !errors.Is(err, ErrNoPrimaryKey) {
		t.Errorf("expected ErrNoPrimaryKey, got: %v", err)
	}
}

func TestNewDatabase_MultiplePrimaryKeys(t *testing.T) {
	mockDB, _ := mockdb.New()
	_, err := NewDatabase[MultiplePKUser](mockDB, "test", testDBRenderer)
	if err == nil {
		t.Fatal("expected ErrMultiplePrimaryKeys")
	}
	if !errors.Is(err, ErrMultiplePrimaryKeys) {
		t.Errorf("expected ErrMultiplePrimaryKeys, got: %v", err)
	}
}

func TestNewDatabase_PrimaryKeyInCommaList(t *testing.T) {
	mockDB, _ := mockdb.New()
	db, err := NewDatabase[CommaPKUser](mockDB, "test", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}
	if db.keyCol != "id" {
		t.Errorf("expected keyCol 'id', got %q", db.keyCol)
	}
}

func TestNewDatabase_PrimaryKeyNoDBTag(t *testing.T) {
	mockDB, _ := mockdb.New()
	_, err := NewDatabase[PKNoDBTag](mockDB, "test", testDBRenderer)
	if err == nil {
		t.Fatal("expected ErrNoPrimaryKey")
	}
	if !errors.Is(err, ErrNoPrimaryKey) {
		t.Errorf("expected ErrNoPrimaryKey, got: %v", err)
	}
}

func TestNewDatabase_PrimaryKeyIgnoredDBTag(t *testing.T) {
	mockDB, _ := mockdb.New()
	_, err := NewDatabase[PKIgnoredDBTag](mockDB, "test", testDBRenderer)
	if err == nil {
		t.Fatal("expected ErrNoPrimaryKey")
	}
	if !errors.Is(err, ErrNoPrimaryKey) {
		t.Errorf("expected ErrNoPrimaryKey, got: %v", err)
	}
}

// --- DatabaseProvider Tests ---

// mockDatabaseProvider is an in-memory DatabaseProvider for testing.
type mockDatabaseProvider struct {
	data map[string][]byte
}

func newMockDatabaseProvider() *mockDatabaseProvider {
	return &mockDatabaseProvider{data: make(map[string][]byte)}
}

func (m *mockDatabaseProvider) Get(_ context.Context, key string) ([]byte, error) {
	v, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

func (m *mockDatabaseProvider) Set(_ context.Context, _ string, value []byte) error {
	// Extract key from the JSON to store by key
	var record TestDBUser
	codec := JSONCodec{}
	if err := codec.Decode(value, &record); err != nil {
		return err
	}
	key := string(rune(record.ID + '0'))
	m.data[key] = value
	return nil
}

func (m *mockDatabaseProvider) Delete(_ context.Context, key string) error {
	if _, ok := m.data[key]; !ok {
		return ErrNotFound
	}
	delete(m.data, key)
	return nil
}

func (m *mockDatabaseProvider) Exists(_ context.Context, key string) (bool, error) {
	_, ok := m.data[key]
	return ok, nil
}

func (m *mockDatabaseProvider) ExecQuery(_ context.Context, _ edamame.QueryStatement, _ map[string]any) ([][]byte, error) {
	results := make([][]byte, 0, len(m.data))
	for _, v := range m.data {
		results = append(results, v)
	}
	return results, nil
}

func (m *mockDatabaseProvider) ExecSelect(_ context.Context, _ edamame.SelectStatement, _ map[string]any) ([]byte, error) {
	for _, v := range m.data {
		return v, nil
	}
	return nil, ErrNotFound
}

func (m *mockDatabaseProvider) ExecUpdate(_ context.Context, _ edamame.UpdateStatement, _ map[string]any) ([]byte, error) {
	for _, v := range m.data {
		return v, nil
	}
	return nil, ErrNotFound
}

func (m *mockDatabaseProvider) ExecAggregate(_ context.Context, _ edamame.AggregateStatement, _ map[string]any) (float64, error) {
	return float64(len(m.data)), nil
}

func TestNewDatabaseFromProvider(t *testing.T) {
	provider := newMockDatabaseProvider()
	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")
	if db == nil {
		t.Fatal("NewDatabaseFromProvider returned nil")
	}
	if db.provider == nil {
		t.Error("provider not set")
	}
	if db.codec == nil {
		t.Error("codec not set")
	}
	if db.tableName != "test_users" {
		t.Errorf("tableName mismatch: got %q", db.tableName)
	}
	if db.executor != nil {
		t.Error("executor should be nil in provider mode")
	}
}

func TestDatabaseFromProvider_Get(t *testing.T) {
	provider := newMockDatabaseProvider()
	ctx := context.Background()

	// Pre-populate the mock
	data, _ := JSONCodec{}.Encode(&TestDBUser{ID: 1, Email: "test@example.com", Name: "Test", Age: intPtr(30)})
	provider.data["1"] = data

	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")

	user, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if user.ID != 1 {
		t.Errorf("ID mismatch: got %d", user.ID)
	}
	if user.Email != "test@example.com" {
		t.Errorf("Email mismatch: got %q", user.Email)
	}
}

func TestDatabaseFromProvider_Get_NotFound(t *testing.T) {
	provider := newMockDatabaseProvider()
	ctx := context.Background()

	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")

	_, err := db.Get(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for missing key")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestDatabaseFromProvider_Set(t *testing.T) {
	provider := newMockDatabaseProvider()
	ctx := context.Background()

	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")

	user := &TestDBUser{ID: 1, Email: "test@example.com", Name: "Test", Age: intPtr(25)}
	err := db.Set(ctx, "1", user)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify stored
	if len(provider.data) == 0 {
		t.Error("provider data should not be empty after Set")
	}
}

func TestDatabaseFromProvider_Delete(t *testing.T) {
	provider := newMockDatabaseProvider()
	ctx := context.Background()

	// Pre-populate
	data, _ := JSONCodec{}.Encode(&TestDBUser{ID: 1, Email: "test@example.com", Name: "Test"})
	provider.data["1"] = data

	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")

	err := db.Delete(ctx, "1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if len(provider.data) != 0 {
		t.Error("expected empty data after delete")
	}
}

func TestDatabaseFromProvider_Delete_NotFound(t *testing.T) {
	provider := newMockDatabaseProvider()
	ctx := context.Background()

	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")

	err := db.Delete(ctx, "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestDatabaseFromProvider_Exists(t *testing.T) {
	provider := newMockDatabaseProvider()
	ctx := context.Background()

	data, _ := JSONCodec{}.Encode(&TestDBUser{ID: 1})
	provider.data["1"] = data

	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")

	exists, err := db.Exists(ctx, "1")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected key to exist")
	}

	exists, err = db.Exists(ctx, "2")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected key to not exist")
	}
}

func TestDatabaseFromProvider_ExecQuery(t *testing.T) {
	provider := newMockDatabaseProvider()
	ctx := context.Background()

	data, _ := JSONCodec{}.Encode(&TestDBUser{ID: 1, Email: "a@b.com", Name: "A"})
	provider.data["1"] = data

	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")

	results, err := db.ExecQuery(ctx, QueryAll, nil)
	if err != nil {
		t.Fatalf("ExecQuery failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != 1 {
		t.Errorf("ID mismatch: got %d", results[0].ID)
	}
}

func TestDatabaseFromProvider_ExecAggregate(t *testing.T) {
	provider := newMockDatabaseProvider()
	ctx := context.Background()

	provider.data["1"] = []byte(`{}`)
	provider.data["2"] = []byte(`{}`)

	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")

	count, err := db.ExecAggregate(ctx, CountAll, nil)
	if err != nil {
		t.Fatalf("ExecAggregate failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected count 2, got %f", count)
	}
}

func TestDatabaseFromProvider_PanicOnBuilders(t *testing.T) {
	provider := newMockDatabaseProvider()
	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")

	methods := []struct {
		name string
		fn   func()
	}{
		{"Query", func() { db.Query() }},
		{"Select", func() { db.Select() }},
		{"Insert", func() { db.Insert() }},
		{"InsertFull", func() { db.InsertFull() }},
		{"Modify", func() { db.Modify() }},
		{"Remove", func() { db.Remove() }},
		{"Count", func() { db.Count() }},
		{"Executor", func() { db.Executor() }},
		{"Atomic", func() { db.Atomic() }},
	}

	for _, m := range methods {
		t.Run(m.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("%s should panic in provider mode", m.name)
				}
			}()
			m.fn()
		})
	}
}

func TestDatabaseFromProvider_PanicOnTxMethods(t *testing.T) {
	provider := newMockDatabaseProvider()
	db := NewDatabaseFromProvider[TestDBUser](provider, "test_users")
	ctx := context.Background()

	methods := []struct {
		name string
		fn   func()
	}{
		{"GetTx", func() { db.GetTx(ctx, nil, "1") }},
		{"SetTx", func() { db.SetTx(ctx, nil, "1", &TestDBUser{}) }},
		{"DeleteTx", func() { db.DeleteTx(ctx, nil, "1") }},
		{"ExistsTx", func() { db.ExistsTx(ctx, nil, "1") }},
		{"ExecQueryTx", func() { db.ExecQueryTx(ctx, nil, QueryAll, nil) }},
		{"ExecSelectTx", func() {
			stmt := edamame.NewSelectStatement("x", "x", edamame.SelectSpec{})
			db.ExecSelectTx(ctx, nil, stmt, nil)
		}},
		{"ExecUpdateTx", func() {
			stmt := edamame.NewUpdateStatement("x", "x", edamame.UpdateSpec{})
			db.ExecUpdateTx(ctx, nil, stmt, nil)
		}},
		{"ExecAggregateTx", func() { db.ExecAggregateTx(ctx, nil, CountAll, nil) }},
	}

	for _, m := range methods {
		t.Run(m.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("%s should panic in provider mode", m.name)
				}
			}()
			m.fn()
		})
	}
}

func TestNewDatabaseProvider(t *testing.T) {
	mockDB, _ := mockdb.New()
	provider, err := NewDatabaseProvider[TestDBUser](mockDB, "test_users", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabaseProvider failed: %v", err)
	}
	if provider == nil {
		t.Fatal("NewDatabaseProvider returned nil")
	}
}

func TestNewDatabaseProvider_NoPrimaryKey(t *testing.T) {
	mockDB, _ := mockdb.New()
	_, err := NewDatabaseProvider[NoPKUser](mockDB, "test", testDBRenderer)
	if err == nil {
		t.Fatal("expected ErrNoPrimaryKey")
	}
	if !errors.Is(err, ErrNoPrimaryKey) {
		t.Errorf("expected ErrNoPrimaryKey, got: %v", err)
	}
}
