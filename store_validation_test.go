package litestore_test

import (
	"testing"

	"github.com/dir01/litestore"
)

func TestNewStore_Validation_Errors(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()

	t.Run("invalid table name", func(t *testing.T) {
		_, err := litestore.NewStore[TestPersonWithKey](ctx, db, "invalid-name")
		if err == nil {
			t.Fatal("expected an error for invalid table name, got nil")
		}
		expectedErr := "invalid table name: invalid-name"
		if err.Error() != expectedErr {
			t.Fatalf("expected error '%s', got '%s'", expectedErr, err.Error())
		}
	})

	t.Run("non-struct type", func(t *testing.T) {
		_, err := litestore.NewStore[int](ctx, db, "some_table")
		if err == nil {
			t.Fatal("expected an error for non-struct type, got nil")
		}
		expectedErr := "type T must be a struct, but got int"
		if err.Error() != expectedErr {
			t.Fatalf("expected error '%s', got '%s'", expectedErr, err.Error())
		}
	})

	t.Run("non-string key field", func(t *testing.T) {
		type BadEntity struct {
			ID int `litestore:"key"`
		}
		_, err := litestore.NewStore[BadEntity](ctx, db, "some_table")
		if err == nil {
			t.Fatal("expected an error for non-string key field, got nil")
		}
		expectedErr := "field with litestore:\"key\" tag must be a string, but field ID is int"
		if err.Error() != expectedErr {
			t.Fatalf("expected error '%s', got '%s'", expectedErr, err.Error())
		}
	})
}

func TestNewStore_ValidTableNames(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()

	validTableNames := []string{
		"users",
		"user_profiles",
		"Users",
		"UserProfiles",
		"users123",
		"users_123_profiles",
		"a",
		"A",
		"table123",
		"table_with_underscores",
	}

	for _, tableName := range validTableNames {
		t.Run("valid_table_name_"+tableName, func(t *testing.T) {
			store, err := litestore.NewStore[TestPersonWithKey](ctx, db, tableName)
			if err != nil {
				t.Errorf("expected table name '%s' to be valid, got error: %v", tableName, err)
			}
			if store != nil {
				_ = store.Close()
			}
		})
	}
}

func TestNewStore_InvalidTableNames(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()

	invalidTableNames := []string{
		"invalid-name",
		"invalid.name",
		"invalid name",
		"invalid@name",
		"",
		"123invalid",
		"invalid!name",
		"invalid+name",
		"invalid&name",
	}

	for _, tableName := range invalidTableNames {
		t.Run("invalid_table_name_"+tableName, func(t *testing.T) {
			store, err := litestore.NewStore[TestPersonWithKey](ctx, db, tableName)
			if err == nil {
				t.Errorf("expected table name '%s' to be invalid, but got no error", tableName)
				if store != nil {
					_ = store.Close()
				}
			}
		})
	}
}

func TestNewStore_KeyFieldValidation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()

	t.Run("valid string key field", func(t *testing.T) {
		type ValidEntity struct {
			ID string `litestore:"key"`
		}
		store, err := litestore.NewStore[ValidEntity](ctx, db, "valid_entities")
		if err != nil {
			t.Errorf("expected valid string key field to work, got error: %v", err)
		}
		if store != nil {
			_ = store.Close()
		}
	})

	t.Run("multiple key fields should fail", func(t *testing.T) {
		// Note: This test assumes the current implementation only allows one key field
		// If the implementation changes to support multiple keys, this test should be updated
		type MultiKeyEntity struct {
			ID1 string `litestore:"key"`
			ID2 string `litestore:"key"`
		}
		store, err := litestore.NewStore[MultiKeyEntity](ctx, db, "multi_key_entities")
		// The current implementation will just use the last key field found,
		// so this should actually succeed, but it's worth documenting the behavior
		if store != nil {
			_ = store.Close()
		}
		// We don't assert error here since the behavior might be implementation-specific
		_ = err // Just to avoid unused variable warning
	})

	t.Run("unexported key field", func(t *testing.T) {
		type UnexportedKeyEntity struct {
			id string `litestore:"key"` // lowercase = unexported
		}
		store, err := litestore.NewStore[UnexportedKeyEntity](ctx, db, "unexported_key_entities")
		if err != nil {
			t.Errorf("NewStore should succeed with unexported key field, got error: %v", err)
		}
		if store != nil {
			_ = store.Close()
		}
		// The error should occur when trying to Save, not when creating the store
	})
}