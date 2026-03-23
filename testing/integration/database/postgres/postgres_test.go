package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	astqlpg "github.com/zoobz-io/astql/postgres"
	"github.com/zoobz-io/grub/testing/integration/database"
)

var tc *database.TestContext

func TestMain(m *testing.M) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		panic("failed to start postgres container: " + err.Error())
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic("failed to get connection string: " + err.Error())
	}

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		panic("failed to connect to database: " + err.Error())
	}

	tc = &database.TestContext{
		DB:       db,
		Renderer: astqlpg.New(),
		ResetSQL: `
			DROP TABLE IF EXISTS test_users;
			CREATE TABLE test_users (
				id SERIAL PRIMARY KEY,
				email TEXT NOT NULL UNIQUE,
				name TEXT NOT NULL,
				age INTEGER
			)
		`,
		InsertUserSQL: `INSERT INTO test_users (id, email, name, age) VALUES ($1, $2, $3, $4)`,
	}

	code := m.Run()

	_ = db.Close()
	_ = pgContainer.Terminate(ctx)

	os.Exit(code)
}

func TestPostgres_CRUD(t *testing.T) {
	database.RunCRUDTests(t, tc)
}

func TestPostgres_Query(t *testing.T) {
	database.RunQueryTests(t, tc)
}

func TestPostgres_Transaction(t *testing.T) {
	database.RunTransactionTests(t, tc)
}

func TestPostgres_Hooks(t *testing.T) {
	database.RunHookTests(t, tc)
}
