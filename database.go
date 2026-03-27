package grub

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/zoobz-io/astql"
	"github.com/zoobz-io/atom"
	"github.com/zoobz-io/edamame"
	"github.com/zoobz-io/grub/internal/atomix"
	"github.com/zoobz-io/soy"
)

// Default statements for common operations.
var (
	// QueryAll returns all records from the table.
	QueryAll = edamame.NewQueryStatement("query", "Query all records", edamame.QuerySpec{})

	// CountAll counts all records in the table.
	CountAll = edamame.NewAggregateStatement("count", "Count all records", edamame.AggCount, edamame.AggregateSpec{})
)

// Database provides type-safe SQL storage operations for T.
// Constructed via NewDatabase (executor path) or NewDatabaseFromProvider (provider path).
// Builder methods (Query, Select, Insert, etc.), Tx variants, Executor(), and Atomic()
// require the executor path and will panic if called on a provider-backed instance.
type Database[T any] struct {
	executor   *edamame.Executor[T] // set by NewDatabase (nil in provider path)
	provider   DatabaseProvider     // set by NewDatabaseFromProvider (nil in executor path)
	codec      Codec                // set by NewDatabaseFromProvider (nil in executor path)
	keyCol     string
	tableName  string
	atomic     *atomix.Database[T] // lazily created via Atomic()
	atomicOnce sync.Once
}

// findPrimaryKey inspects the struct metadata and returns the db column name
// of the field tagged with constraints:"primarykey".
// Panics if the struct has no primary key or multiple primary keys (programmer error).
func findPrimaryKey[T any](exec *edamame.Executor[T]) string {
	s := exec.Soy()
	var keyCol string

	for _, field := range s.Metadata().Fields {
		constraints := field.Tags["constraints"]
		if constraints == "" {
			continue
		}
		for _, c := range strings.Split(constraints, ",") {
			if strings.TrimSpace(c) != "primarykey" {
				continue
			}
			col := field.Tags["db"]
			if col == "" || col == "-" {
				continue
			}
			if keyCol != "" {
				panic("grub: " + ErrMultiplePrimaryKeys.Error())
			}
			keyCol = col
			break
		}
	}

	if keyCol == "" {
		panic("grub: " + ErrNoPrimaryKey.Error())
	}
	return keyCol
}

// NewDatabase creates a Database for type T.
// The primary key column is derived from the struct field tagged with constraints:"primarykey".
// Panics if T is not a valid struct or has incorrect primary key tags (programmer error).
// Use the *Tx method variants (GetTx, SetTx, etc.) for transaction support.
func NewDatabase[T any](db *sqlx.DB, table string, renderer astql.Renderer) *Database[T] {
	exec, err := edamame.New[T](db, table, renderer)
	if err != nil {
		panic("grub: invalid type for database: " + err.Error())
	}

	keyCol := findPrimaryKey(exec)

	// Register lifecycle hook callbacks on the soy instance so hooks
	// fire through both wrapper methods and direct builder paths.
	s := exec.Soy()
	s.OnScan(callAfterLoad)
	s.OnRecord(callBeforeSave)

	return &Database[T]{
		executor:  exec,
		keyCol:    keyCol,
		tableName: table,
	}
}

// NewDatabaseFromProvider creates a Database for type T backed by a DatabaseProvider.
// Uses JSON codec by default. Builder methods (Query, Select, Insert, Modify, Remove, Count),
// Tx variants, Executor(), and Atomic() are not available and will panic if called.
func NewDatabaseFromProvider[T any](provider DatabaseProvider, table string) *Database[T] {
	return &Database[T]{
		provider:  provider,
		codec:     JSONCodec{},
		tableName: table,
	}
}

// NewDatabaseFromProviderWithCodec creates a Database for type T with a custom codec.
// Builder methods, Tx variants, Executor(), and Atomic() are not available and will panic if called.
func NewDatabaseFromProviderWithCodec[T any](provider DatabaseProvider, table string, codec Codec) *Database[T] {
	return &Database[T]{
		provider:  provider,
		codec:     codec,
		tableName: table,
	}
}

// requireExecutor panics if the database was constructed from a provider.
func (d *Database[T]) requireExecutor(method string) {
	if d.executor == nil {
		panic("grub: " + method + " requires executor (use NewDatabase, not NewDatabaseFromProvider)")
	}
}

// Get retrieves the record at key as T.
// Returns ErrNotFound if the key does not exist.
func (d *Database[T]) Get(ctx context.Context, key string) (*T, error) {
	if d.provider != nil {
		data, err := d.provider.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		var value T
		if err := d.codec.Decode(data, &value); err != nil {
			return nil, err
		}
		if err := callAfterLoad(ctx, &value); err != nil {
			return nil, err
		}
		return &value, nil
	}
	result, err := d.executor.Soy().Select().
		Where(d.keyCol, "=", "key").
		Exec(ctx, map[string]any{"key": key})
	if err != nil {
		if errors.Is(err, soy.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return result, nil
}

// Set stores value at key (insert or update via upsert).
func (d *Database[T]) Set(ctx context.Context, _ string, value *T) error {
	if d.provider != nil {
		if err := callBeforeSave(ctx, value); err != nil {
			return err
		}
		data, err := d.codec.Encode(value)
		if err != nil {
			return err
		}
		if err := d.provider.Set(ctx, "", data); err != nil {
			return err
		}
		return callAfterSave(ctx, value)
	}
	s := d.executor.Soy()
	// Use InsertFull to include PK in the INSERT for proper ON CONFLICT matching
	insert := s.InsertFull().OnConflict(d.keyCol).DoUpdate()

	for _, field := range s.Metadata().Fields {
		col := field.Tags["db"]
		if col == "" || col == "-" || col == d.keyCol {
			continue
		}
		insert = insert.Set(col, col)
	}

	_, err := insert.Build().Exec(ctx, value)
	if err != nil {
		return err
	}
	return callAfterSave(ctx, value)
}

// Delete removes the record at key.
func (d *Database[T]) Delete(ctx context.Context, key string) error {
	if d.provider != nil {
		if err := callBeforeDelete[T](ctx); err != nil {
			return err
		}
		if err := d.provider.Delete(ctx, key); err != nil {
			return err
		}
		return callAfterDelete[T](ctx)
	}
	if err := callBeforeDelete[T](ctx); err != nil {
		return err
	}
	affected, err := d.executor.Soy().Remove().
		Where(d.keyCol, "=", "key").
		Exec(ctx, map[string]any{"key": key})
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return callAfterDelete[T](ctx)
}

// Exists checks whether a record exists at key.
func (d *Database[T]) Exists(ctx context.Context, key string) (bool, error) {
	if d.provider != nil {
		return d.provider.Exists(ctx, key)
	}
	results, err := d.executor.Soy().Query().
		Where(d.keyCol, "=", "key").
		Limit(1).
		Exec(ctx, map[string]any{"key": key})
	if err != nil {
		return false, err
	}
	return len(results) > 0, nil
}

// Executor returns the underlying edamame Executor for advanced query operations.
// Panics if the database was constructed from a provider.
func (d *Database[T]) Executor() *edamame.Executor[T] {
	d.requireExecutor("Executor")
	return d.executor
}

// Query returns a query builder for fetching multiple records.
// Panics if the database was constructed from a provider.
func (d *Database[T]) Query() *soy.Query[T] {
	d.requireExecutor("Query")
	return d.executor.Soy().Query()
}

// Select returns a select builder for fetching a single record.
// Panics if the database was constructed from a provider.
func (d *Database[T]) Select() *soy.Select[T] {
	d.requireExecutor("Select")
	return d.executor.Soy().Select()
}

// Insert returns an insert builder (auto-generates PK).
// Panics if the database was constructed from a provider.
func (d *Database[T]) Insert() *soy.Create[T] {
	d.requireExecutor("Insert")
	return d.executor.Soy().Insert()
}

// InsertFull returns an insert builder that includes the PK field.
// Panics if the database was constructed from a provider.
func (d *Database[T]) InsertFull() *soy.Create[T] {
	d.requireExecutor("InsertFull")
	return d.executor.Soy().InsertFull()
}

// Modify returns an update builder.
// Panics if the database was constructed from a provider.
func (d *Database[T]) Modify() *soy.Update[T] {
	d.requireExecutor("Modify")
	return d.executor.Soy().Modify()
}

// Remove returns a delete builder.
// Panics if the database was constructed from a provider.
func (d *Database[T]) Remove() *soy.Delete[T] {
	d.requireExecutor("Remove")
	return d.executor.Soy().Remove()
}

// Count returns an aggregate builder for counting records.
// Panics if the database was constructed from a provider.
func (d *Database[T]) Count() *soy.Aggregate[T] {
	d.requireExecutor("Count")
	return d.executor.Soy().Count()
}

// ExecQuery executes a query statement and returns multiple records.
func (d *Database[T]) ExecQuery(ctx context.Context, stmt edamame.QueryStatement, params map[string]any) ([]*T, error) {
	if d.provider != nil {
		rawResults, err := d.provider.ExecQuery(ctx, stmt, params)
		if err != nil {
			return nil, err
		}
		results := make([]*T, len(rawResults))
		for i, data := range rawResults {
			var value T
			if err := d.codec.Decode(data, &value); err != nil {
				return nil, err
			}
			if err := callAfterLoad(ctx, &value); err != nil {
				return nil, err
			}
			results[i] = &value
		}
		return results, nil
	}
	return d.executor.ExecQuery(ctx, stmt, params)
}

// ExecSelect executes a select statement and returns a single record.
func (d *Database[T]) ExecSelect(ctx context.Context, stmt edamame.SelectStatement, params map[string]any) (*T, error) {
	if d.provider != nil {
		data, err := d.provider.ExecSelect(ctx, stmt, params)
		if err != nil {
			return nil, err
		}
		var value T
		if err := d.codec.Decode(data, &value); err != nil {
			return nil, err
		}
		if err := callAfterLoad(ctx, &value); err != nil {
			return nil, err
		}
		return &value, nil
	}
	return d.executor.ExecSelect(ctx, stmt, params)
}

// ExecUpdate executes an update statement.
func (d *Database[T]) ExecUpdate(ctx context.Context, stmt edamame.UpdateStatement, params map[string]any) (*T, error) {
	if d.provider != nil {
		data, err := d.provider.ExecUpdate(ctx, stmt, params)
		if err != nil {
			return nil, err
		}
		var value T
		if err := d.codec.Decode(data, &value); err != nil {
			return nil, err
		}
		return &value, nil
	}
	return d.executor.ExecUpdate(ctx, stmt, params)
}

// ExecAggregate executes an aggregate statement.
func (d *Database[T]) ExecAggregate(ctx context.Context, stmt edamame.AggregateStatement, params map[string]any) (float64, error) {
	if d.provider != nil {
		return d.provider.ExecAggregate(ctx, stmt, params)
	}
	return d.executor.ExecAggregate(ctx, stmt, params)
}

// GetTx retrieves the record at key as T within a transaction.
// Returns ErrNotFound if the key does not exist.
// Panics if the database was constructed from a provider.
func (d *Database[T]) GetTx(ctx context.Context, tx *sqlx.Tx, key string) (*T, error) {
	d.requireExecutor("GetTx")
	result, err := d.executor.Soy().Select().
		Where(d.keyCol, "=", "key").
		ExecTx(ctx, tx, map[string]any{"key": key})
	if err != nil {
		if errors.Is(err, soy.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return result, nil
}

// SetTx stores value at key within a transaction (insert or update via upsert).
// Panics if the database was constructed from a provider.
func (d *Database[T]) SetTx(ctx context.Context, tx *sqlx.Tx, _ string, value *T) error {
	d.requireExecutor("SetTx")
	s := d.executor.Soy()
	insert := s.InsertFull().OnConflict(d.keyCol).DoUpdate()

	for _, field := range s.Metadata().Fields {
		col := field.Tags["db"]
		if col == "" || col == "-" || col == d.keyCol {
			continue
		}
		insert = insert.Set(col, col)
	}

	_, err := insert.Build().ExecTx(ctx, tx, value)
	if err != nil {
		return err
	}
	return callAfterSave(ctx, value)
}

// DeleteTx removes the record at key within a transaction.
// Panics if the database was constructed from a provider.
func (d *Database[T]) DeleteTx(ctx context.Context, tx *sqlx.Tx, key string) error {
	d.requireExecutor("DeleteTx")
	if err := callBeforeDelete[T](ctx); err != nil {
		return err
	}
	affected, err := d.executor.Soy().Remove().
		Where(d.keyCol, "=", "key").
		ExecTx(ctx, tx, map[string]any{"key": key})
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return callAfterDelete[T](ctx)
}

// ExistsTx checks whether a record exists at key within a transaction.
// Panics if the database was constructed from a provider.
func (d *Database[T]) ExistsTx(ctx context.Context, tx *sqlx.Tx, key string) (bool, error) {
	d.requireExecutor("ExistsTx")
	results, err := d.executor.Soy().Query().
		Where(d.keyCol, "=", "key").
		Limit(1).
		ExecTx(ctx, tx, map[string]any{"key": key})
	if err != nil {
		return false, err
	}
	return len(results) > 0, nil
}

// ExecQueryTx executes a query statement within a transaction and returns multiple records.
// Panics if the database was constructed from a provider.
func (d *Database[T]) ExecQueryTx(ctx context.Context, tx *sqlx.Tx, stmt edamame.QueryStatement, params map[string]any) ([]*T, error) {
	d.requireExecutor("ExecQueryTx")
	return d.executor.ExecQueryTx(ctx, tx, stmt, params)
}

// ExecSelectTx executes a select statement within a transaction and returns a single record.
// Panics if the database was constructed from a provider.
func (d *Database[T]) ExecSelectTx(ctx context.Context, tx *sqlx.Tx, stmt edamame.SelectStatement, params map[string]any) (*T, error) {
	d.requireExecutor("ExecSelectTx")
	return d.executor.ExecSelectTx(ctx, tx, stmt, params)
}

// ExecUpdateTx executes an update statement within a transaction.
// Panics if the database was constructed from a provider.
func (d *Database[T]) ExecUpdateTx(ctx context.Context, tx *sqlx.Tx, stmt edamame.UpdateStatement, params map[string]any) (*T, error) {
	d.requireExecutor("ExecUpdateTx")
	return d.executor.ExecUpdateTx(ctx, tx, stmt, params)
}

// ExecAggregateTx executes an aggregate statement within a transaction.
// Panics if the database was constructed from a provider.
func (d *Database[T]) ExecAggregateTx(ctx context.Context, tx *sqlx.Tx, stmt edamame.AggregateStatement, params map[string]any) (float64, error) {
	d.requireExecutor("ExecAggregateTx")
	return d.executor.ExecAggregateTx(ctx, tx, stmt, params)
}

// Atomic returns an atom-based view of this database.
// The returned atomix.Database satisfies the AtomicDatabase interface.
// The instance is created once and cached for subsequent calls.
// Panics if T is not atomizable (a programmer error) or if constructed from a provider.
func (d *Database[T]) Atomic() AtomicDatabase {
	d.requireExecutor("Atomic")
	d.atomicOnce.Do(func() {
		atomizer, err := atom.Use[T]()
		if err != nil {
			panic("grub: invalid type for atomization: " + err.Error())
		}
		d.atomic = atomix.New(
			d.executor,
			d.keyCol,
			d.tableName,
			atomizer.Spec(),
		)
	})
	return d.atomic
}
