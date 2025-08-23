package litestore_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/dir01/litestore"
)

// TestGetAndInjectTx tests the manual injection and retrieval of a transaction.
func TestGetAndInjectTx(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// 1. Test GetTx on a context without a transaction
	ctx := t.Context()
	tx, ok := litestore.GetTx(ctx)
	if ok {
		t.Error("GetTx returned ok=true for a context without a transaction")
	}
	if tx != nil {
		t.Error("GetTx returned a non-nil transaction for a context without one")
	}

	// 2. Test InjectTx and GetTx
	dbtx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err := dbtx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Logf("failed to rollback tx: %v", err)
		}
	}() // Rollback in case test fails before commit

	txCtx := litestore.InjectTx(ctx, dbtx)
	retrievedTx, ok := litestore.GetTx(txCtx)
	if !ok {
		t.Fatal("GetTx returned ok=false for a context with an injected transaction")
	}
	if retrievedTx != dbtx {
		t.Fatal("GetTx returned a different transaction than the one injected")
	}
}

// TestWithTransaction tests the WithTransaction helper function.
func TestWithTransaction(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()
	tableName := "test_tx_table"

	// Create the table once for all sub-tests.
	// This simplifies checking for values later.
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS " + tableName + " (value TEXT)")
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	// Helper to check if a value exists in the table
	checkValueExists := func(value string) bool {
		var count int
		query := "SELECT COUNT(*) FROM " + tableName + " WHERE value = ?"
		err := db.QueryRow(query, value).Scan(&count)
		if err != nil && err != sql.ErrNoRows {
			t.Fatalf("failed to query for value: %v", err)
		}
		return count > 0
	}

	t.Run("commit on success", func(t *testing.T) {
		const committedValue = "i-was-committed"

		err := litestore.WithTransaction(ctx, db, func(txCtx context.Context) error {
			tx, ok := litestore.GetTx(txCtx)
			if !ok {
				return errors.New("failed to get transaction from context")
			}
			_, err := tx.ExecContext(txCtx, "INSERT INTO "+tableName+" (value) VALUES (?)", committedValue)
			return err
		})

		if err != nil {
			t.Fatalf("WithTransaction returned an unexpected error: %v", err)
		}

		if !checkValueExists(committedValue) {
			t.Error("value was not committed to the database")
		}
	})

	t.Run("rollback on error", func(t *testing.T) {
		const rolledBackValue = "i-should-be-rolled-back"
		txErr := errors.New("something went wrong")

		err := litestore.WithTransaction(ctx, db, func(txCtx context.Context) error {
			tx, ok := litestore.GetTx(txCtx)
			if !ok {
				return errors.New("failed to get transaction from context")
			}
			_, err := tx.ExecContext(txCtx, "INSERT INTO "+tableName+" (value) VALUES (?)", rolledBackValue)
			if err != nil {
				return err
			}
			return txErr
		})

		if !errors.Is(err, txErr) {
			t.Fatalf("WithTransaction did not return the expected error. Got: %v, Want: %v", err, txErr)
		}

		if checkValueExists(rolledBackValue) {
			t.Error("value was committed to the database but should have been rolled back")
		}
	})
}
