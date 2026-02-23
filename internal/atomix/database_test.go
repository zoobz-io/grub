package atomix

import (
	"context"
	"errors"
	"strings"
	"testing"

	astqlsqlite "github.com/zoobzio/astql/sqlite"
	"github.com/zoobzio/atom"
	"github.com/zoobzio/edamame"
	"github.com/zoobzio/grub/internal/mockdb"
	"github.com/zoobzio/grub/internal/shared"
	"github.com/zoobzio/sentinel"
)

func init() {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
}

// TestUser is the model used for database tests.
type TestUser struct {
	ID    int    `db:"id" atom:"id" constraints:"primarykey"`
	Email string `db:"email" atom:"email" constraints:"notnull,unique"`
	Name  string `db:"name" atom:"name" constraints:"notnull"`
	Age   *int   `db:"age" atom:"age"`
}

var testRenderer = astqlsqlite.New()

func TestNew(t *testing.T) {
	mockDB, _ := mockdb.New()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()

	db := New[TestUser](executor, "id", "test_users", spec)

	if db == nil {
		t.Fatal("New returned nil")
	}
	if db.executor != executor {
		t.Error("executor not set correctly")
	}
	if db.keyCol != "id" {
		t.Errorf("keyCol not set correctly: %q", db.keyCol)
	}
	if db.tableName != "test_users" {
		t.Errorf("tableName not set correctly: %q", db.tableName)
	}
}

func TestDatabase_Table(t *testing.T) {
	mockDB, _ := mockdb.New()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()

	db := New[TestUser](executor, "id", "test_users", spec)

	if table := db.Table(); table != "test_users" {
		t.Errorf("Table() returned %q, expected 'test_users'", table)
	}
}

func TestDatabase_Spec(t *testing.T) {
	mockDB, _ := mockdb.New()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()

	db := New[TestUser](executor, "id", "test_users", spec)

	returnedSpec := db.Spec()
	if returnedSpec.TypeName != spec.TypeName {
		t.Errorf("Spec TypeName mismatch: got %q, want %q", returnedSpec.TypeName, spec.TypeName)
	}
}

func TestDatabase_Get(t *testing.T) {
	mockDB, capture := mockdb.New()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

	age := int64(30)
	a := &atom.Atom{
		Ints:    map[string]int64{"ID": 1},
		Strings: map[string]string{"Email": "new@example.com", "Name": "New User"},
		IntPtrs: map[string]*int64{"Age": &age},
	}

	_ = db.Set(ctx, "1", a)

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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

func TestDatabase_ExecQuery(t *testing.T) {
	mockDB, capture := mockdb.New()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

	stmt := edamame.NewQueryStatement("query", "Query all", edamame.QuerySpec{})
	_, _ = db.ExecQuery(ctx, stmt, nil)

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

func TestDatabase_ExecSelect(t *testing.T) {
	mockDB, capture := mockdb.New()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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

func TestDatabase_Get_NotFound(t *testing.T) {
	mockDB, _ := mockdb.New()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

	// mockdb returns empty rows, which should result in shared.ErrNotFound
	_, err = db.Get(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for missing key")
	}
	if !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("expected shared.ErrNotFound, got: %v", err)
	}
}

func TestDatabase_Get_QueryError(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

	// Configure mock to return 0 rows affected
	cfg.SetRowsAffected(0)
	defer cfg.Reset()

	err = db.Delete(ctx, "nonexistent")
	if err == nil {
		t.Error("expected shared.ErrNotFound for 0 rows affected")
	}
	if !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("expected shared.ErrNotFound, got: %v", err)
	}
}

func TestDatabase_Delete_ExecError(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	age := int64(30)
	a := &atom.Atom{
		Ints:    map[string]int64{"ID": 1},
		Strings: map[string]string{"Email": "new@example.com", "Name": "New User"},
		IntPtrs: map[string]*int64{"Age": &age},
	}

	_ = db.SetTx(ctx, tx, "1", a)

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	stmt := edamame.NewQueryStatement("query", "Query all", edamame.QuerySpec{})
	_, _ = db.ExecQueryTx(ctx, tx, stmt, nil)

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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

func TestDatabase_GetTx_NotFound(t *testing.T) {
	mockDB, _ := mockdb.New()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	_, err = db.GetTx(ctx, tx, "nonexistent")
	if err == nil {
		t.Error("expected error for missing key")
	}
	if !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("expected shared.ErrNotFound, got: %v", err)
	}
}

func TestDatabase_DeleteTx_NotFound(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	cfg.SetRowsAffected(0)
	defer cfg.Reset()

	err = db.DeleteTx(ctx, tx, "nonexistent")
	if err == nil {
		t.Error("expected shared.ErrNotFound for 0 rows affected")
	}
	if !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("expected shared.ErrNotFound, got: %v", err)
	}
}

func TestDatabase_GetTx_QueryError(t *testing.T) {
	mockDB, _, cfg := mockdb.NewWithConfig()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

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

func TestDatabase_ExecQueryTx_StatementError(t *testing.T) {
	mockDB, _ := mockdb.New()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	// Create a statement with an invalid field that doesn't exist on the type
	invalidStmt := edamame.NewQueryStatement("invalid", "Invalid query", edamame.QuerySpec{
		Where: []edamame.ConditionSpec{
			{Field: "nonexistent_field", Operator: "=", Param: "value"},
		},
	})

	_, err = db.ExecQueryTx(ctx, tx, invalidStmt, map[string]any{"value": "test"})
	if err == nil {
		t.Error("expected error for invalid statement field")
	}
}

func TestDatabase_ExecSelectTx_StatementError(t *testing.T) {
	mockDB, _ := mockdb.New()
	executor, err := edamame.New[TestUser](mockDB, "test_users", testRenderer)
	if err != nil {
		t.Fatalf("failed to create executor: %v", err)
	}

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](executor, "id", "test_users", spec)

	ctx := context.Background()

	tx, err := mockDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx failed: %v", err)
	}
	defer tx.Rollback()

	// Create a statement with an invalid field that doesn't exist on the type
	invalidStmt := edamame.NewSelectStatement("invalid", "Invalid select", edamame.SelectSpec{
		Where: []edamame.ConditionSpec{
			{Field: "nonexistent_field", Operator: "=", Param: "value"},
		},
	})

	_, err = db.ExecSelectTx(ctx, tx, invalidStmt, map[string]any{"value": "test"})
	if err == nil {
		t.Error("expected error for invalid statement field")
	}
}
