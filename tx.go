package litestore

import (
	"context"
	"database/sql"
	"fmt"
)

// txContextKey is a private key for storing the transaction in the context.
type txContextKey struct{}

// GetTx retrieves a transaction from the context, if one exists.
func GetTx(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txContextKey{}).(*sql.Tx)
	return tx, ok
}

// InjectTx returns a new context with the provided transaction injected.
// This is for users who want to manage the transaction lifecycle manually.
func InjectTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txContextKey{}, tx)
}

// WithTransaction executes a function within a database transaction.
// It begins a transaction, injects it into the context for the callback,
// and then commits or rolls back based on the error returned by the callback.
func WithTransaction(ctx context.Context, db *sql.DB, fn func(ctx context.Context) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Defer a rollback. It will be a no-op if the transaction is committed.
	defer tx.Rollback()

	// Create a new context with the transaction.
	txCtx := InjectTx(ctx, tx)

	// Execute the user's callback with the transactional context.
	if err := fn(txCtx); err != nil {
		// The callback returned an error, so the deferred Rollback will execute.
		return err
	}

	// The callback succeeded, so we commit the transaction.
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
