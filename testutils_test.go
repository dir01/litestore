package litestore_test

import (
	"database/sql"
	"fmt"
	"testing"
)

// setupTestDB creates an in-memory SQLite database for testing.
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s/test.db?_journal_mode=WAL", t.TempDir()))
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close db: %v", err)
		}
	}

	return db, cleanup
}
