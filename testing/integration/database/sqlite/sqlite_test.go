package sqlite

import (
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	astqlsqlite "github.com/zoobz-io/astql/sqlite"
	"github.com/zoobz-io/grub/testing/integration/database"
	_ "modernc.org/sqlite"
)

var tc *database.TestContext

func TestMain(m *testing.M) {
	db, err := sqlx.Connect("sqlite", ":memory:")
	if err != nil {
		panic("failed to connect to sqlite: " + err.Error())
	}

	tc = &database.TestContext{
		DB:       db,
		Renderer: astqlsqlite.New(),
		ResetSQL: `
			DROP TABLE IF EXISTS test_users;
			CREATE TABLE test_users (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				email TEXT NOT NULL UNIQUE,
				name TEXT NOT NULL,
				age INTEGER
			)
		`,
		InsertUserSQL: `INSERT INTO test_users (id, email, name, age) VALUES (?, ?, ?, ?)`,
	}

	code := m.Run()

	_ = db.Close()

	os.Exit(code)
}

func TestSQLite_CRUD(t *testing.T) {
	database.RunCRUDTests(t, tc)
}

func TestSQLite_Query(t *testing.T) {
	database.RunQueryTests(t, tc)
}

func TestSQLite_Transaction(t *testing.T) {
	database.RunTransactionTests(t, tc)
}

func TestSQLite_Hooks(t *testing.T) {
	database.RunHookTests(t, tc)
}
