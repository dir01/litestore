package litestore_test

import (
	"context"
	"slices"
	"testing"

	"github.com/dir01/litestore"
)

type IndexedEntity struct {
	ID       string `litestore:"key"`
	Email    string `json:"email"`
	Name     string `json:"name"`
	Category string `json:"category"`
	Value    int    `json:"value"`
}

func TestIndexCreation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create store with indexes
	store, err := litestore.NewStore[IndexedEntity](ctx, db, "indexed_entities",
		litestore.WithIndex("email"),
		litestore.WithIndex("name"))
	if err != nil {
		t.Fatalf("failed to create store with indexes: %v", err)
	}
	defer store.Close()

	// Query SQLite master table to verify indexes exist
	rows, err := db.QueryContext(ctx, `
		SELECT name FROM sqlite_master 
		WHERE type='index' AND tbl_name='indexed_entities' 
		AND name LIKE 'idx_indexed_entities_%'
		ORDER BY name`)
	if err != nil {
		t.Fatalf("failed to query indexes: %v", err)
	}
	defer rows.Close()

	var indexNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan index name: %v", err)
		}
		indexNames = append(indexNames, name)
	}

	// Should find exactly the indexes we specified
	expectedIndexes := []string{"idx_indexed_entities_email", "idx_indexed_entities_name"}
	if !slices.Equal(indexNames, expectedIndexes) {
		t.Errorf("unexpected indexes: got %v, want %v", indexNames, expectedIndexes)
	}
}

func TestIndexCreationWithNoIndexes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create store with no indexes
	store, err := litestore.NewStore[IndexedEntity](ctx, db, "no_indexes")
	if err != nil {
		t.Fatalf("failed to create store with no indexes: %v", err)
	}
	defer store.Close()

	// Query for indexes - should find none
	rows, err := db.QueryContext(ctx, `
		SELECT name FROM sqlite_master 
		WHERE type='index' AND tbl_name='no_indexes' 
		AND name LIKE 'idx_no_indexes_%'`)
	if err != nil {
		t.Fatalf("failed to query indexes: %v", err)
	}
	defer rows.Close()

	var indexNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan index name: %v", err)
		}
		indexNames = append(indexNames, name)
	}

	if len(indexNames) != 0 {
		t.Errorf("expected no indexes, but found %v", indexNames)
	}
}

func TestIndexCreationWithInvalidField(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Try to create store with invalid field
	_, err := litestore.NewStore[IndexedEntity](ctx, db, "invalid_field", litestore.WithIndex("nonexistent_field"))
	if err == nil {
		t.Fatal("expected error for invalid field, but got none")
	}

	expectedError := "invalid index field: 'nonexistent_field' is not a valid key for this entity"
	if err.Error() != "creating indexes for invalid_field: "+expectedError {
		t.Errorf("unexpected error: got %v, want error containing %v", err, expectedError)
	}
}

func TestIndexCreationWithKeyField(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create store with "key" field in index list (should be ignored)
	store, err := litestore.NewStore[IndexedEntity](ctx, db, "with_key_field",
		litestore.WithIndex("key"),
		litestore.WithIndex("email"))
	if err != nil {
		t.Fatalf("failed to create store with key field: %v", err)
	}
	defer store.Close()

	// Query for indexes - should only find email index, not key
	rows, err := db.QueryContext(ctx, `
		SELECT name FROM sqlite_master 
		WHERE type='index' AND tbl_name='with_key_field' 
		AND name LIKE 'idx_with_key_field_%'`)
	if err != nil {
		t.Fatalf("failed to query indexes: %v", err)
	}
	defer rows.Close()

	var indexNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan index name: %v", err)
		}
		indexNames = append(indexNames, name)
	}

	expectedIndexes := []string{"idx_with_key_field_email"}
	if !slices.Equal(indexNames, expectedIndexes) {
		t.Errorf("unexpected indexes: got %v, want %v", indexNames, expectedIndexes)
	}
}

func TestIndexCreationIdempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create first store with indexes
	store1, err := litestore.NewStore[IndexedEntity](ctx, db, "idempotent", litestore.WithIndex("email"))
	if err != nil {
		t.Fatalf("failed to create first store: %v", err)
	}
	store1.Close()

	// Create second store with same indexes - should not error
	store2, err := litestore.NewStore[IndexedEntity](ctx, db, "idempotent", litestore.WithIndex("email"))
	if err != nil {
		t.Fatalf("failed to create second store: %v", err)
	}
	defer store2.Close()

	// Should still have exactly one index
	var count int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master 
		WHERE type='index' AND tbl_name='idempotent' 
		AND name LIKE 'idx_idempotent_%'`).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count indexes: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 index after recreating store, got %d", count)
	}
}

func TestRegularStoreStillWorks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create regular store (no indexes)
	store, err := litestore.NewStore[IndexedEntity](ctx, db, "regular")
	if err != nil {
		t.Fatalf("failed to create regular store: %v", err)
	}
	defer store.Close()

	// Should work fine and have no custom indexes
	entity := &IndexedEntity{
		Email: "test@example.com",
		Name:  "Test User",
	}

	if err := store.Save(ctx, entity); err != nil {
		t.Fatalf("failed to save entity: %v", err)
	}

	retrieved, err := store.GetOne(ctx, litestore.Filter{Key: "email", Op: litestore.OpEq, Value: "test@example.com"})
	if err != nil {
		t.Fatalf("failed to get entity: %v", err)
	}

	if retrieved.Email != "test@example.com" {
		t.Errorf("unexpected retrieved email: got %s, want test@example.com", retrieved.Email)
	}
}
