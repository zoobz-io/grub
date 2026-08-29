// Package mockdb provides a mock SQL driver for testing query generation.
package mockdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"

	"github.com/jmoiron/sqlx"
)

// globalCapture is shared across all connections for test inspection.
var globalCapture = &Capture{}

// globalConfig is shared across all connections for configurable behavior.
var globalConfig = &Config{}

func init() {
	sql.Register("mockdb", &Driver{})
}

// Config holds configurable behavior for the mock database.
//
// Two layers of configuration exist. The Set* methods define a single global
// response returned for every query/exec in a run — the default. The Push*
// methods enqueue per-call responses consumed in order, one per QueryContext or
// ExecContext, so a multi-query store method can be driven step by step. When a
// queue is exhausted, behavior falls back to the single Set* configuration.
type Config struct {
	mu              sync.Mutex
	QueryErr        error    // Error to return from QueryContext
	ExecErr         error    // Error to return from ExecContext
	RowsAffected    int64    // Value to return from RowsAffected (default 1)
	rowsAffectedSet bool     // Whether RowsAffected was explicitly set
	rowData         *RowData // Configurable row data for queries

	queryQueue []queryResponse // Per-call query responses, consumed in order
	execQueue  []execResponse  // Per-call exec responses, consumed in order
}

// queryResponse is a single queued response for a QueryContext call: either rows
// (possibly nil for empty) or an error.
type queryResponse struct {
	rows *RowData
	err  error
}

// execResponse is a single queued response for an ExecContext call: an error, or
// a RowsAffected value.
type execResponse struct {
	rowsAffected    int64
	rowsAffectedSet bool
	err             error
}

// SetQueryErr sets the error to return from queries.
func (c *Config) SetQueryErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.QueryErr = err
}

// SetExecErr sets the error to return from exec operations.
func (c *Config) SetExecErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ExecErr = err
}

// RowData holds configurable column and row data for query results.
type RowData struct {
	Columns []string
	Rows    [][]any
}

// SetRowData configures rows to return from queries.
func (c *Config) SetRowData(data *RowData) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rowData = data
}

func (c *Config) getRowData() *RowData {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rowData
}

// PushRowData enqueues rows for the next query. Queued responses are consumed in
// order, one per QueryContext, before falling back to SetRowData.
func (c *Config) PushRowData(data *RowData) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryQueue = append(c.queryQueue, queryResponse{rows: data})
}

// PushQueryErr enqueues an error for the next query, consumed in order before
// falling back to SetQueryErr.
func (c *Config) PushQueryErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryQueue = append(c.queryQueue, queryResponse{err: err})
}

// PushExecErr enqueues an error for the next exec, consumed in order before
// falling back to SetExecErr.
func (c *Config) PushExecErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execQueue = append(c.execQueue, execResponse{err: err})
}

// PushRowsAffected enqueues a RowsAffected value for the next exec, consumed in
// order before falling back to SetRowsAffected.
func (c *Config) PushRowsAffected(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execQueue = append(c.execQueue, execResponse{rowsAffected: n, rowsAffectedSet: true})
}

func (c *Config) popQueryResponse() (queryResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queryQueue) == 0 {
		return queryResponse{}, false
	}
	r := c.queryQueue[0]
	c.queryQueue = c.queryQueue[1:]
	return r, true
}

func (c *Config) popExecResponse() (execResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.execQueue) == 0 {
		return execResponse{}, false
	}
	r := c.execQueue[0]
	c.execQueue = c.execQueue[1:]
	return r, true
}

// SetRowsAffected sets the rows affected value to return.
func (c *Config) SetRowsAffected(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.RowsAffected = n
	c.rowsAffectedSet = true
}

// Reset resets all configuration to defaults.
func (c *Config) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.QueryErr = nil
	c.ExecErr = nil
	c.RowsAffected = 0
	c.rowsAffectedSet = false
	c.rowData = nil
	c.queryQueue = nil
	c.execQueue = nil
}

func (c *Config) getQueryErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.QueryErr
}

func (c *Config) getExecErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ExecErr
}

func (c *Config) getRowsAffected() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.rowsAffectedSet {
		return 1 // default
	}
	return c.RowsAffected
}

// Driver is a mock SQL driver that captures queries.
type Driver struct{}

// Open returns a new mock connection.
func (*Driver) Open(_ string) (driver.Conn, error) {
	return &Conn{capture: globalCapture, config: globalConfig}, nil
}

// Capture holds captured query information.
type Capture struct {
	mu      sync.Mutex
	Queries []CapturedQuery
}

// CapturedQuery represents a captured SQL query.
type CapturedQuery struct {
	Query string
	Args  []any
}

// Last returns the most recently captured query.
func (c *Capture) Last() (CapturedQuery, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.Queries) == 0 {
		return CapturedQuery{}, false
	}
	return c.Queries[len(c.Queries)-1], true
}

// Reset clears all captured queries.
func (c *Capture) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Queries = nil
}

// add records a query.
func (c *Capture) add(query string, args []any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Queries = append(c.Queries, CapturedQuery{Query: query, Args: args})
}

// Conn is a mock database connection.
type Conn struct {
	capture *Capture
	config  *Config
}

// Capture returns the query capture for this connection.
func (c *Conn) Capture() *Capture {
	return c.capture
}

// Prepare returns a mock statement.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{query: query, capture: c.capture}, nil
}

// Close is a no-op.
func (*Conn) Close() error {
	return nil
}

// Begin returns a mock transaction.
func (*Conn) Begin() (driver.Tx, error) {
	return &Tx{}, nil
}

// QueryContext implements driver.QueryerContext.
func (c *Conn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.capture.add(query, namedValuesToAny(args))
	if resp, ok := c.config.popQueryResponse(); ok {
		if resp.err != nil {
			return nil, resp.err
		}
		if resp.rows != nil {
			return &DataRows{columns: resp.rows.Columns, rows: resp.rows.Rows}, nil
		}
		return &Rows{}, nil
	}
	if err := c.config.getQueryErr(); err != nil {
		return nil, err
	}
	if data := c.config.getRowData(); data != nil {
		return &DataRows{columns: data.Columns, rows: data.Rows}, nil
	}
	return &Rows{}, nil
}

// ExecContext implements driver.ExecerContext.
func (c *Conn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.capture.add(query, namedValuesToAny(args))
	if resp, ok := c.config.popExecResponse(); ok {
		if resp.err != nil {
			return nil, resp.err
		}
		if resp.rowsAffectedSet {
			return &Result{rowsAffected: resp.rowsAffected}, nil
		}
		return &Result{rowsAffected: c.config.getRowsAffected()}, nil
	}
	if err := c.config.getExecErr(); err != nil {
		return nil, err
	}
	return &Result{rowsAffected: c.config.getRowsAffected()}, nil
}

// Stmt is a mock prepared statement.
type Stmt struct {
	query   string
	capture *Capture
}

// Close is a no-op.
func (*Stmt) Close() error {
	return nil
}

// NumInput returns -1 to accept any number of arguments.
func (*Stmt) NumInput() int {
	return -1
}

// Exec captures the query and returns a mock result.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	s.capture.add(s.query, valuesToAny(args))
	return &Result{}, nil
}

// Query captures the query and returns empty rows.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	s.capture.add(s.query, valuesToAny(args))
	return &Rows{}, nil
}

// Tx is a mock transaction.
type Tx struct{}

// Commit is a no-op.
func (*Tx) Commit() error {
	return nil
}

// Rollback is a no-op.
func (*Tx) Rollback() error {
	return nil
}

// Result is a mock result.
type Result struct {
	rowsAffected int64
}

// LastInsertId returns 1.
func (*Result) LastInsertId() (int64, error) {
	return 1, nil
}

// RowsAffected returns the configured rows affected value.
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Rows is a mock rows result that returns no data.
type Rows struct {
	closed bool
}

// Columns returns empty column list.
func (*Rows) Columns() []string {
	return []string{}
}

// Close marks rows as closed.
func (r *Rows) Close() error {
	r.closed = true
	return nil
}

// Next always returns io.EOF (no rows).
func (*Rows) Next(_ []driver.Value) error {
	return io.EOF
}

// DataRows is a mock rows result that returns configurable data.
type DataRows struct {
	columns []string
	rows    [][]any
	index   int
	closed  bool
}

// Columns returns the configured column names.
func (r *DataRows) Columns() []string {
	return r.columns
}

// Close marks rows as closed.
func (r *DataRows) Close() error {
	r.closed = true
	return nil
}

// Next copies the next row into dest, or returns io.EOF when exhausted.
func (r *DataRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.index]
	r.index++
	for i, v := range row {
		if i < len(dest) {
			dest[i] = v
		}
	}
	return nil
}

// New creates a new mock database connection and returns the sqlx.DB and the capture.
func New() (*sqlx.DB, *Capture) {
	db, err := sql.Open("mockdb", "")
	if err != nil {
		panic("mockdb: failed to open: " + err.Error())
	}
	globalCapture.Reset()
	globalConfig.Reset()
	return sqlx.NewDb(db, "mockdb"), globalCapture
}

// NewWithConfig creates a new mock database connection and returns the sqlx.DB, capture, and config.
func NewWithConfig() (*sqlx.DB, *Capture, *Config) {
	db, err := sql.Open("mockdb", "")
	if err != nil {
		panic("mockdb: failed to open: " + err.Error())
	}
	globalCapture.Reset()
	globalConfig.Reset()
	return sqlx.NewDb(db, "mockdb"), globalCapture, globalConfig
}

func namedValuesToAny(nvs []driver.NamedValue) []any {
	result := make([]any, len(nvs))
	for i, nv := range nvs {
		result[i] = nv.Value
	}
	return result
}

func valuesToAny(vs []driver.Value) []any {
	result := make([]any, len(vs))
	for i, v := range vs {
		result[i] = v
	}
	return result
}
