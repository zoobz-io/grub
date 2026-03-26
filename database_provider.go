package grub

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
	"github.com/zoobz-io/astql"
	"github.com/zoobz-io/edamame"
	"github.com/zoobz-io/soy"
)

// databaseProvider implements DatabaseProvider using an edamame Executor.
type databaseProvider[T any] struct {
	executor *edamame.Executor[T]
	keyCol   string
	codec    Codec
}

// NewDatabaseProvider creates a DatabaseProvider backed by a sqlx.DB connection.
// The primary key column is derived from the struct field tagged with constraints:"primarykey".
// Lifecycle hooks (OnScan, OnRecord) are registered on the soy instance.
func NewDatabaseProvider[T any](db *sqlx.DB, table string, renderer astql.Renderer) (DatabaseProvider, error) {
	exec, err := edamame.New[T](db, table, renderer)
	if err != nil {
		return nil, err
	}

	keyCol, err := findPrimaryKey(exec)
	if err != nil {
		return nil, err
	}

	s := exec.Soy()
	s.OnScan(callAfterLoad)
	s.OnRecord(callBeforeSave)

	return &databaseProvider[T]{
		executor: exec,
		keyCol:   keyCol,
		codec:    JSONCodec{},
	}, nil
}

// Get retrieves the record at key as raw bytes.
func (p *databaseProvider[T]) Get(ctx context.Context, key string) ([]byte, error) {
	result, err := p.executor.Soy().Select().
		Where(p.keyCol, "=", "key").
		Exec(ctx, map[string]any{"key": key})
	if err != nil {
		if errors.Is(err, soy.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return p.codec.Encode(result)
}

// Set stores value at key (insert or update via upsert).
func (p *databaseProvider[T]) Set(ctx context.Context, _ string, value []byte) error {
	var record T
	if err := p.codec.Decode(value, &record); err != nil {
		return err
	}

	s := p.executor.Soy()
	insert := s.InsertFull().OnConflict(p.keyCol).DoUpdate()

	for _, field := range s.Metadata().Fields {
		col := field.Tags["db"]
		if col == "" || col == "-" || col == p.keyCol {
			continue
		}
		insert = insert.Set(col, col)
	}

	_, err := insert.Build().Exec(ctx, &record)
	if err != nil {
		return err
	}
	return callAfterSave(ctx, &record)
}

// Delete removes the record at key.
func (p *databaseProvider[T]) Delete(ctx context.Context, key string) error {
	if err := callBeforeDelete[T](ctx); err != nil {
		return err
	}
	affected, err := p.executor.Soy().Remove().
		Where(p.keyCol, "=", "key").
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
func (p *databaseProvider[T]) Exists(ctx context.Context, key string) (bool, error) {
	results, err := p.executor.Soy().Query().
		Where(p.keyCol, "=", "key").
		Limit(1).
		Exec(ctx, map[string]any{"key": key})
	if err != nil {
		return false, err
	}
	return len(results) > 0, nil
}

// ExecQuery executes a query statement and returns multiple records as raw bytes.
func (p *databaseProvider[T]) ExecQuery(ctx context.Context, stmt edamame.QueryStatement, params map[string]any) ([][]byte, error) {
	results, err := p.executor.ExecQuery(ctx, stmt, params)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(results))
	for i, r := range results {
		b, err := p.codec.Encode(r)
		if err != nil {
			return nil, err
		}
		out[i] = b
	}
	return out, nil
}

// ExecSelect executes a select statement and returns a single record as raw bytes.
func (p *databaseProvider[T]) ExecSelect(ctx context.Context, stmt edamame.SelectStatement, params map[string]any) ([]byte, error) {
	result, err := p.executor.ExecSelect(ctx, stmt, params)
	if err != nil {
		return nil, err
	}
	return p.codec.Encode(result)
}

// ExecUpdate executes an update statement and returns the affected record as raw bytes.
func (p *databaseProvider[T]) ExecUpdate(ctx context.Context, stmt edamame.UpdateStatement, params map[string]any) ([]byte, error) {
	result, err := p.executor.ExecUpdate(ctx, stmt, params)
	if err != nil {
		return nil, err
	}
	return p.codec.Encode(result)
}

// ExecAggregate executes an aggregate statement and returns a scalar.
func (p *databaseProvider[T]) ExecAggregate(ctx context.Context, stmt edamame.AggregateStatement, params map[string]any) (float64, error) {
	return p.executor.ExecAggregate(ctx, stmt, params)
}
