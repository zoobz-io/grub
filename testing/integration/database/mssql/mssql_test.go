package mssql

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mssql"
	"github.com/testcontainers/testcontainers-go/wait"
	astqlmssql "github.com/zoobz-io/astql/mssql"
	"github.com/zoobz-io/grub/testing/integration/database"
)

var tc *database.TestContext

func TestMain(m *testing.M) {
	ctx := context.Background()

	mssqlContainer, err := mssql.Run(ctx,
		"mcr.microsoft.com/mssql/server:2022-latest",
		mssql.WithAcceptEULA(),
		mssql.WithPassword("Test@12345"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("SQL Server is now ready for client connections").
				WithStartupTimeout(120*time.Second),
		),
	)
	if err != nil {
		panic("failed to start mssql container: " + err.Error())
	}

	connStr, err := mssqlContainer.ConnectionString(ctx)
	if err != nil {
		panic("failed to get connection string: " + err.Error())
	}

	// MSSQL may need a moment after reporting ready; retry connection
	var db *sqlx.DB
	for i := 0; i < 10; i++ {
		db, err = sqlx.Connect("sqlserver", connStr)
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
		Renderer: astqlmssql.New(),
		ResetSQL: `
			IF OBJECT_ID('test_users', 'U') IS NOT NULL DROP TABLE test_users;
			CREATE TABLE test_users (
				id INT IDENTITY(1,1) PRIMARY KEY,
				email NVARCHAR(255) NOT NULL UNIQUE,
				name NVARCHAR(255) NOT NULL,
				age INT
			)
		`,
		InsertUserSQL: `
			SET IDENTITY_INSERT test_users ON;
			INSERT INTO test_users (id, email, name, age) VALUES (@p1, @p2, @p3, @p4);
			SET IDENTITY_INSERT test_users OFF;
		`,
	}

	code := m.Run()

	_ = db.Close()
	_ = mssqlContainer.Terminate(ctx)

	os.Exit(code)
}

func TestMSSQL_CRUD(t *testing.T) {
	database.RunCRUDTests(t, tc)
}

func TestMSSQL_Query(t *testing.T) {
	database.RunQueryTests(t, tc)
}

func TestMSSQL_Transaction(t *testing.T) {
	database.RunTransactionTests(t, tc)
}

func TestMSSQL_Hooks(t *testing.T) {
	database.RunHookTests(t, tc)
}
