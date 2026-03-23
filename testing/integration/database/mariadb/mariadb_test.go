package mariadb

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"
	"github.com/testcontainers/testcontainers-go/wait"
	astqlmariadb "github.com/zoobz-io/astql/mariadb"
	"github.com/zoobz-io/grub/testing/integration/database"
)

var tc *database.TestContext

func TestMain(m *testing.M) {
	ctx := context.Background()

	mariadbContainer, err := mariadb.Run(ctx,
		"mariadb:11",
		mariadb.WithDatabase("testdb"),
		mariadb.WithUsername("test"),
		mariadb.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("ready for connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		panic("failed to start mariadb container: " + err.Error())
	}

	connStr, err := mariadbContainer.ConnectionString(ctx, "multiStatements=true")
	if err != nil {
		panic("failed to get connection string: " + err.Error())
	}

	// MariaDB may need a moment after reporting ready; retry connection
	var db *sqlx.DB
	for i := 0; i < 10; i++ {
		db, err = sqlx.Connect("mysql", connStr)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		panic("failed to connect to database: " + err.Error())
	}

	tc = &database.TestContext{
		DB:       db,
		Renderer: astqlmariadb.New(),
		ResetSQL: `
			DROP TABLE IF EXISTS test_users;
			CREATE TABLE test_users (
				id INT AUTO_INCREMENT PRIMARY KEY,
				email VARCHAR(255) NOT NULL UNIQUE,
				name VARCHAR(255) NOT NULL,
				age INT
			)
		`,
		InsertUserSQL: `INSERT INTO test_users (id, email, name, age) VALUES (?, ?, ?, ?)`,
	}

	code := m.Run()

	_ = db.Close()
	_ = mariadbContainer.Terminate(ctx)

	os.Exit(code)
}

func TestMariaDB_CRUD(t *testing.T) {
	database.RunCRUDTests(t, tc)
}

func TestMariaDB_Query(t *testing.T) {
	database.RunQueryTests(t, tc)
}

func TestMariaDB_Transaction(t *testing.T) {
	database.RunTransactionTests(t, tc)
}

func TestMariaDB_Hooks(t *testing.T) {
	database.RunHookTests(t, tc)
}
