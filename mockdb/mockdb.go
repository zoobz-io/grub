// Package mockdb exposes grub's mock SQL driver for testing query generation
// in downstream repositories.
//
// It re-exports the capture-based mock that grub's own database tests use, so
// consumers can assert the SQL that grub/soy builders generate — "confirm the
// query is built as we expect" — without a live database:
//
//	mockDB, capture := mockdb.New()
//	db := grub.NewDatabase[User](mockDB, "users", renderer)
//	_, _ = db.Get(ctx, "123")
//	query, ok := capture.Last() // captured SQL + args
//
// Importing this package registers the "mockdb" SQL driver via the underlying
// package's init. Import it only from test code: production binaries that never
// import it pull in none of this and pay nothing.
package mockdb

import (
	"github.com/jmoiron/sqlx"

	"github.com/zoobz-io/grub/internal/mockdb"
)

// Capture holds captured query information.
type Capture = mockdb.Capture

// CapturedQuery represents a captured SQL query (the generated SQL and its args).
type CapturedQuery = mockdb.CapturedQuery

// Config holds configurable behavior for the mock database, letting tests drive
// returned rows and error paths.
type Config = mockdb.Config

// RowData holds configurable column and row data for query results.
type RowData = mockdb.RowData

// New creates a new mock-backed *sqlx.DB and returns it with the query Capture.
func New() (*sqlx.DB, *Capture) {
	return mockdb.New()
}

// NewWithConfig creates a new mock-backed *sqlx.DB and returns it with the query
// Capture and a Config for driving rows and error paths.
func NewWithConfig() (*sqlx.DB, *Capture, *Config) {
	return mockdb.NewWithConfig()
}
