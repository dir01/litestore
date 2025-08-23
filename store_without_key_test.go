package litestore_test

import (
	"reflect"
	"testing"

	"github.com/dir01/litestore"
)

func TestStore_WithoutKey_Save_GetOne(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewStore[TestPersonNoKey](t.Context(), db, "test_entities_no_key")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close store: %v", err)
		}
	}()

	ctx := t.Context()

	t.Run("save entity without key field", func(t *testing.T) {
		entity := &TestPersonNoKey{Info: "some info", Data: 123}
		if err := s.Save(ctx, entity); err != nil {
			t.Fatalf("failed to save entity with no key field: %v", err)
		}

		// We can't know the generated key, but we can query for the content.
		got, err := s.GetOne(ctx, litestore.Filter{Key: "info", Op: litestore.OpEq, Value: "some info"})
		if err != nil {
			t.Fatalf("failed to get entity by content: %v", err)
		}

		if !reflect.DeepEqual(got, *entity) {
			t.Errorf("retrieved entity does not match saved one.\ngot:  %+v\nwant: %+v", got, *entity)
		}
	})

	t.Run("save multiple entities without key field creates separate records", func(t *testing.T) {
		// Save the same entity multiple times - should create multiple records
		entity1 := &TestPersonNoKey{Info: "duplicate info", Data: 100}
		entity2 := &TestPersonNoKey{Info: "duplicate info", Data: 100}

		if err := s.Save(ctx, entity1); err != nil {
			t.Fatalf("failed to save first entity: %v", err)
		}

		if err := s.Save(ctx, entity2); err != nil {
			t.Fatalf("failed to save second entity: %v", err)
		}

		// Should find multiple results for the same content
		query := &litestore.Query{
			Predicate: litestore.Filter{Key: "info", Op: litestore.OpEq, Value: "duplicate info"},
		}
		seq, err := s.Iter(ctx, query)
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		var results []TestPersonNoKey
		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			results = append(results, entity)
		}

		if len(results) != 2 {
			t.Errorf("expected 2 results for duplicate content, got %d", len(results))
		}
	})

	t.Run("query by different fields", func(t *testing.T) {
		entities := []*TestPersonNoKey{
			{Info: "user1", Data: 10},
			{Info: "user2", Data: 20},
			{Info: "user3", Data: 10}, // Same data as user1
		}

		for _, e := range entities {
			if err := s.Save(ctx, e); err != nil {
				t.Fatalf("failed to save entity: %v", err)
			}
		}

		// Query by info field
		got, err := s.GetOne(ctx, litestore.Filter{Key: "info", Op: litestore.OpEq, Value: "user2"})
		if err != nil {
			t.Fatalf("failed to get entity by info: %v", err)
		}
		if got.Info != "user2" || got.Data != 20 {
			t.Errorf("wrong entity retrieved by info.\ngot:  %+v\nwant: info=user2, data=20", got)
		}

		// Query by data field - should find multiple results
		query := &litestore.Query{
			Predicate: litestore.Filter{Key: "data", Op: litestore.OpEq, Value: 10},
		}
		seq, err := s.Iter(ctx, query)
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		var results []TestPersonNoKey
		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			results = append(results, entity)
		}

		if len(results) != 2 {
			t.Errorf("expected 2 results for data=10, got %d", len(results))
		}

		// Verify both results have correct data
		for _, result := range results {
			if result.Data != 10 {
				t.Errorf("expected data=10, got data=%d", result.Data)
			}
		}
	})

}

func TestKeyMayBeAddedLater(t *testing.T) {

	type StructNoKey struct {
		Name string `json:"name"`
	}
	type StructWithKey struct {
		SomeField string `json:"some_field" litestore:"key"`
		Name      string `json:"name"`
	}

	db, cleanup := setupTestDB(t)
	defer cleanup()

	storeNoKey, err := litestore.NewStore[StructNoKey](t.Context(), db, "test")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}
	storeWithKey, err := litestore.NewStore[StructWithKey](t.Context(), db, "test")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}

	err = storeNoKey.Save(t.Context(), &StructNoKey{Name: "foo"})
	if err != nil {
		t.Fatalf("failed to save entityNoKey to storeNoKey: %v", err)
	}

	entityWithKey, err := storeWithKey.GetOne(t.Context(), litestore.Filter{Key: "name", Op: litestore.OpEq, Value: "foo"})
	if err != nil {
		t.Fatalf("failed to get entityWithKey from a storeWithKey: %v", err)
	}

	if entityWithKey.SomeField == "" {
		t.Fatal("expected entityWithKey to have key filled in")
	}
}
