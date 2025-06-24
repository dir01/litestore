package litestore_test

import (
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
)

// setupTestDB creates an in-memory SQLite database for testing.
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
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

// mkEntityID will return unique entity id
func mkEntityID() string {
	_entityIDCounter.Add(1)
	return fmt.Sprintf("%d", _entityIDCounter.Load())
}

var _entityIDCounter atomic.Uint32
