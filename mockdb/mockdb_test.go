package mockdb_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	astqlsqlite "github.com/zoobz-io/astql/sqlite"
	"github.com/zoobz-io/edamame"
	"github.com/zoobz-io/sentinel"

	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/grub/mockdb"
)

// This test consumes the public mockdb package exactly as a downstream repo
// would — importing only grub and grub/mockdb, no internal packages — proving
// the "mockdb" driver registers and queries are captured through the re-export.

func init() {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
}

type User struct {
	ID    int    `db:"id" constraints:"primarykey"`
	Email string `db:"email" constraints:"notnull,unique"`
	Name  string `db:"name" constraints:"notnull"`
}

var renderer = astqlsqlite.New()

func TestNew_CapturesGeneratedSQL(t *testing.T) {
	mockDB, capture := mockdb.New()
	db := grub.NewDatabase[User](mockDB, "users", renderer)

	_, _ = db.Get(context.Background(), "123")

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}
	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
}

func TestNewWithConfig_DrivesRowsAndErrors(t *testing.T) {
	mockDB, capture, cfg := mockdb.NewWithConfig()
	db := grub.NewDatabase[User](mockDB, "users", renderer)

	// Error path: exec surfaces the configured error.
	cfg.SetExecErr(errors.New("boom"))
	if err := db.Set(context.Background(), "1", &User{ID: 1, Email: "a@b.c", Name: "A"}); err == nil {
		t.Error("expected error from Set when ExecErr is configured")
	}
	if _, ok := capture.Last(); !ok {
		t.Error("expected the INSERT to be captured even on the error path")
	}
	cfg.SetExecErr(nil)

	// Row path: configured rows flow back through a store read.
	cfg.SetRowData(&mockdb.RowData{
		Columns: []string{"id", "email", "name"},
		Rows:    [][]any{{int64(7), "seven@example.com", "Seven"}},
	})
	stmt := edamame.NewSelectStatement("by-email", "Find user by email", edamame.SelectSpec{
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})
	got, err := db.ExecSelect(context.Background(), stmt, map[string]any{"email": "seven@example.com"})
	if err != nil {
		t.Fatalf("ExecSelect returned error: %v", err)
	}
	if got == nil || got.Email != "seven@example.com" {
		t.Errorf("expected configured row to flow back, got: %+v", got)
	}
}
