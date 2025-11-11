package litestore_test

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/dir01/litestore"
)

func TestStore_WithKey_Save_GetOne_Delete(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewStore[TestPersonWithKey](t.Context(), db, "test_entities")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close store: %v", err)
		}
	}()

	ctx := t.Context()

	t.Run("save new entity and get it", func(t *testing.T) {
		entity := &TestPersonWithKey{Name: "first", Key: "notkey", ID: "notkey", Category: "A", IsActive: true, Value: 100}

		// ID should be empty initially
		if entity.K != "" {
			t.Fatalf("expected initial ID to be empty, got %s", entity.K)
		}

		if err := s.Save(ctx, entity); err != nil {
			t.Fatalf("failed to save entity: %v", err)
		}

		// ID should be populated now
		if entity.K == "" {
			t.Fatal("expected ID to be populated by Save, but it's empty")
		}

		// Get it back
		got, err := s.GetOne(ctx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entity.K})
		if err != nil {
			t.Fatalf("failed to get entity back: %v", err)
		}

		if !reflect.DeepEqual(got, *entity) {
			t.Errorf("retrieved entity does not match saved one.\ngot:  %+v\nwant: %+v", got, *entity)
		}
	})

	t.Run("save entity with predefined key", func(t *testing.T) {
		entity := &TestPersonWithKey{
			K:        "predefined-key-123",
			Key:      "notkey",
			ID:       "notkey",
			Name:     "predefined",
			Category: "B",
			IsActive: false,
			Value:    200,
		}

		if err := s.Save(ctx, entity); err != nil {
			t.Fatalf("failed to save entity with predefined key: %v", err)
		}

		// ID should remain unchanged
		if entity.K != "predefined-key-123" {
			t.Errorf("expected ID to remain 'predefined-key-123', got %s", entity.K)
		}

		// Get it back
		got, err := s.GetOne(ctx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entity.K})
		if err != nil {
			t.Fatalf("failed to get entity back: %v", err)
		}

		if !reflect.DeepEqual(got, *entity) {
			t.Errorf("retrieved entity does not match saved one.\ngot:  %+v\nwant: %+v", got, *entity)
		}
	})

	t.Run("update existing entity (upsert behavior)", func(t *testing.T) {
		entity := &TestPersonWithKey{Name: "update-me", Category: "B", IsActive: false, Value: 200}
		if err := s.Save(ctx, entity); err != nil {
			t.Fatalf("failed to save initial entity: %v", err)
		}

		originalID := entity.K
		entity.Name = "updated"
		entity.Value = 250

		if err := s.Save(ctx, entity); err != nil {
			t.Fatalf("failed to update entity: %v", err)
		}

		// ID should remain the same
		if entity.K != originalID {
			t.Errorf("expected ID to remain %s, got %s", originalID, entity.K)
		}

		got, err := s.GetOne(ctx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entity.K})
		if err != nil {
			t.Fatalf("failed to get updated entity: %v", err)
		}

		if got.Name != "updated" || got.Value != 250 {
			t.Errorf("entity was not updated correctly.\ngot:  %+v\nwant: %+v", got, *entity)
		}
	})

	t.Run("delete entity", func(t *testing.T) {
		entity := &TestPersonWithKey{Name: "delete-me", Category: "C", IsActive: true, Value: 300}
		if err := s.Save(ctx, entity); err != nil {
			t.Fatalf("failed to save entity for deletion: %v", err)
		}

		if err := s.Delete(ctx, entity.K); err != nil {
			t.Fatalf("failed to delete entity: %v", err)
		}

		_, err := s.GetOne(ctx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entity.K})
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after deletion, got %v", err)
		}
	})

	t.Run("delete non-existent entity", func(t *testing.T) {
		// Deleting a key that does not exist should not return an error.
		if err := s.Delete(ctx, "non-existent-id"); err != nil {
			t.Fatalf("expected no error when deleting non-existent entity, got %v", err)
		}
	})
}

func TestStore_WithKey_GetOne_Errors(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewStore[TestPersonWithKey](t.Context(), db, "test_entities_getone")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close store: %v", err)
		}
	}()

	ctx := t.Context()

	// Setup data
	entities := []*TestPersonWithKey{
		{Name: "one", Category: "A", Value: 10},
		{Name: "two", Category: "A", Value: 20},
	}
	for _, e := range entities {
		if err := s.Save(ctx, e); err != nil {
			t.Fatalf("failed to save entity: %v", err)
		}
	}

	t.Run("get one with no results", func(t *testing.T) {
		_, err := s.GetOne(ctx, litestore.Filter{Key: "name", Op: litestore.OpEq, Value: "non-existent"})
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})

	t.Run("get one with multiple results", func(t *testing.T) {
		_, err := s.GetOne(ctx, litestore.Filter{Key: "category", Op: litestore.OpEq, Value: "A"})
		if err == nil {
			t.Fatal("expected an error for multiple results, got nil")
		}
		expectedErr := "expected one result, but found multiple"
		if err.Error() != expectedErr {
			t.Fatalf("expected error message '%s', got '%s'", expectedErr, err.Error())
		}
	})
}
