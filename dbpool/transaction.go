package dbpool

import (
	"context"
	"database/sql"
	"fmt"
)

// TxFunc is a function that operates within a transaction
type TxFunc func(*sql.Tx) error

// WithTransaction executes a function within a transaction
// Automatically commits on success and rolls back on error or panic
func (p *DBPool) WithTransaction(ctx context.Context, fn TxFunc) (err error) {
	return p.WithTransactionOptions(ctx, nil, fn)
}

// WithTransactionOptions executes a function within a transaction with custom options
func (p *DBPool) WithTransactionOptions(ctx context.Context, opts *sql.TxOptions, fn TxFunc) (err error) {
	tx, err := p.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Handle panic and ensure rollback
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			err = fmt.Errorf("panic in transaction: %v", r)
		}
	}()

	// Execute the function
	err = fn(tx)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx error: %w, rollback error: %v", err, rbErr)
		}
		return err
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// TxManager provides transaction management utilities
type TxManager struct {
	pool *DBPool
}

// NewTxManager creates a new transaction manager
func NewTxManager(pool *DBPool) *TxManager {
	return &TxManager{pool: pool}
}

// Execute runs a function in a transaction with retry logic
func (tm *TxManager) Execute(ctx context.Context, fn TxFunc) error {
	return tm.pool.WithRetry(ctx, func() error {
		return tm.pool.WithTransaction(ctx, fn)
	})
}

// ExecuteWithOptions runs a function in a transaction with custom options and retry
func (tm *TxManager) ExecuteWithOptions(ctx context.Context, opts *sql.TxOptions, fn TxFunc) error {
	return tm.pool.WithRetry(ctx, func() error {
		return tm.pool.WithTransactionOptions(ctx, opts, fn)
	})
}

// ReadOnly executes a read-only transaction
func (tm *TxManager) ReadOnly(ctx context.Context, fn TxFunc) error {
	return tm.pool.WithTransactionOptions(ctx, &sql.TxOptions{
		ReadOnly: true,
	}, fn)
}

// Serializable executes a transaction with serializable isolation
func (tm *TxManager) Serializable(ctx context.Context, fn TxFunc) error {
	return tm.pool.WithTransactionOptions(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	}, fn)
}

// SavePoint represents a transaction savepoint
type SavePoint struct {
	tx   *sql.Tx
	name string
}

// NewSavePoint creates a savepoint in the transaction
func NewSavePoint(tx *sql.Tx, name string) (*SavePoint, error) {
	_, err := tx.Exec(fmt.Sprintf("SAVEPOINT %s", name))
	if err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	return &SavePoint{
		tx:   tx,
		name: name,
	}, nil
}

// Rollback rolls back to this savepoint
func (sp *SavePoint) Rollback() error {
	_, err := sp.tx.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", sp.name))
	if err != nil {
		return fmt.Errorf("failed to rollback to savepoint: %w", err)
	}
	return nil
}

// Release releases the savepoint
func (sp *SavePoint) Release() error {
	_, err := sp.tx.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", sp.name))
	if err != nil {
		return fmt.Errorf("failed to release savepoint: %w", err)
	}
	return nil
}
