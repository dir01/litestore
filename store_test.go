package litestore_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/dir01/litestore"
)

// TestEntity has an ID field.
type TestEntity struct {
	ID       string `litestore:"id"`
	Name     string `json:"name"`
	Category string `json:"category"`
	IsActive bool   `json:"is_active"`
	Value    int    `json:"value"`
}

// TestEntityNoID does not have an ID field.
type TestEntityNoID struct {
	Info string `json:"info"`
	Data int    `json:"data"`
}

func TestStore_Save_GetOne_Delete(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewStore[TestEntity](context.Background(), db, "test_entities")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	t.Run("save new entity and get it", func(t *testing.T) {
		entity := &TestEntity{Name: "first", Category: "A", IsActive: true, Value: 100}

		// ID should be empty initially
		if entity.ID != "" {
			t.Fatalf("expected initial ID to be empty, got %s", entity.ID)
		}

		if err := s.Save(ctx, entity); err != nil {
			t.Fatalf("failed to save entity: %v", err)
		}

		// ID should be populated now
		if entity.ID == "" {
			t.Fatal("expected ID to be populated by Save, but it's empty")
		}

		// Get it back
		got, err := s.GetOne(ctx, litestore.Filter{Key: "ID", Op: litestore.OpEq, Value: entity.ID})
		if err != nil {
			t.Fatalf("failed to get entity back: %v", err)
		}

		if !reflect.DeepEqual(got, *entity) {
			t.Errorf("retrieved entity does not match saved one.\ngot:  %+v\nwant: %+v", got, *entity)
		}
	})

	t.Run("update existing entity", func(t *testing.T) {
		entity := &TestEntity{Name: "update-me", Category: "B", IsActive: false, Value: 200}
		if err := s.Save(ctx, entity); err != nil {
			t.Fatalf("failed to save initial entity: %v", err)
		}

		entity.Name = "updated"
		entity.Value = 250
		if err := s.Save(ctx, entity); err != nil {
			t.Fatalf("failed to update entity: %v", err)
		}

		got, err := s.GetOne(ctx, litestore.Filter{Key: "ID", Op: litestore.OpEq, Value: entity.ID})
		if err != nil {
			t.Fatalf("failed to get updated entity: %v", err)
		}

		if got.Name != "updated" || got.Value != 250 {
			t.Errorf("entity was not updated correctly.\ngot:  %+v\nwant: %+v", got, *entity)
		}
	})

	t.Run("delete entity", func(t *testing.T) {
		entity := &TestEntity{Name: "delete-me", Category: "C", IsActive: true, Value: 300}
		if err := s.Save(ctx, entity); err != nil {
			t.Fatalf("failed to save entity for deletion: %v", err)
		}

		if err := s.Delete(ctx, entity.ID); err != nil {
			t.Fatalf("failed to delete entity: %v", err)
		}

		_, err := s.GetOne(ctx, litestore.Filter{Key: "ID", Op: litestore.OpEq, Value: entity.ID})
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after deletion, got %v", err)
		}
	})
}

func TestStore_Save_NoID(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewStore[TestEntityNoID](context.Background(), db, "test_entities_no_id")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	entity := &TestEntityNoID{Info: "some info", Data: 123}
	if err := s.Save(ctx, entity); err != nil {
		t.Fatalf("failed to save entity with no ID field: %v", err)
	}

	// We can't know the ID, but we can query for the content.
	got, err := s.GetOne(ctx, litestore.Filter{Key: "info", Op: litestore.OpEq, Value: "some info"})
	if err != nil {
		t.Fatalf("failed to get entity by content: %v", err)
	}

	if !reflect.DeepEqual(got, *entity) {
		t.Errorf("retrieved entity does not match saved one.\ngot:  %+v\nwant: %+v", got, *entity)
	}
}

func TestStore_GetOne_Errors(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewStore[TestEntity](context.Background(), db, "test_entities_getone")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Setup data
	entities := []*TestEntity{
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

func TestStore_Iter(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewStore[TestEntity](context.Background(), db, "test_entities_iter")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Setup data
	entitiesToSave := []*TestEntity{
		{Name: "alice", Category: "A", IsActive: true, Value: 30},
		{Name: "bob", Category: "A", IsActive: true, Value: 45},
		{Name: "charlie", Category: "B", IsActive: false, Value: 35},
		{Name: "david", Category: "B", IsActive: true, Value: 35},
	}
	savedEntities := make(map[string]TestEntity)
	for _, e := range entitiesToSave {
		if err := s.Save(ctx, e); err != nil {
			t.Fatalf("failed to save entity: %v", err)
		}
		savedEntities[e.ID] = *e
	}

	compareResults := func(t *testing.T, got, want []TestEntity) {
		t.Helper()
		if len(got) != len(want) {
			t.Errorf("length mismatch, got %d, want %d", len(got), len(want))
			t.Logf("got: %+v", got)
			t.Logf("want: %+v", want)
			return
		}
		sort.Slice(got, func(i, j int) bool { return got[i].ID < got[j].ID })
		sort.Slice(want, func(i, j int) bool { return want[i].ID < want[j].ID })
		if !reflect.DeepEqual(got, want) {
			t.Errorf("results do not match.\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	t.Run("simple AND query", func(t *testing.T) {
		var results []TestEntity
		p := litestore.AndPredicates(
			litestore.Filter{Key: "is_active", Op: litestore.OpEq, Value: true},
			litestore.Filter{Key: "value", Op: litestore.OpGTE, Value: 35},
		)
		seq, err := s.Iter(ctx, p)
		if err != nil {
			t.Fatalf("Iter failed: %v", err)
		}
		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			results = append(results, entity)
		}

		var expected []TestEntity
		for _, e := range savedEntities {
			if e.IsActive && e.Value >= 35 {
				expected = append(expected, e)
			}
		}
		compareResults(t, results, expected)
	})

	t.Run("composite (A AND B) OR C query", func(t *testing.T) {
		var results []TestEntity
		p := litestore.OrPredicates(
			litestore.AndPredicates(
				litestore.Filter{Key: "is_active", Op: litestore.OpEq, Value: true},
				litestore.Filter{Key: "value", Op: litestore.OpLT, Value: 35},
			),
			litestore.Filter{Key: "name", Op: litestore.OpEq, Value: "charlie"},
		)
		seq, err := s.Iter(ctx, p)
		if err != nil {
			t.Fatalf("Iter failed: %v", err)
		}
		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			results = append(results, entity)
		}

		var expected []TestEntity
		for _, e := range savedEntities {
			if (e.IsActive && e.Value < 35) || e.Name == "charlie" {
				expected = append(expected, e)
			}
		}
		compareResults(t, results, expected)
	})

	t.Run("nil predicate returns all", func(t *testing.T) {
		var results []TestEntity
		seq, err := s.Iter(ctx, nil)
		if err != nil {
			t.Fatalf("Iter failed: %v", err)
		}
		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			results = append(results, entity)
		}

		var expected []TestEntity
		for _, e := range savedEntities {
			expected = append(expected, e)
		}
		compareResults(t, results, expected)
	})

	t.Run("break stops iteration", func(t *testing.T) {
		var processedIDs []string
		p := litestore.Filter{Key: "category", Op: litestore.OpEq, Value: "A"} // Should match 2 entities

		seq, err := s.Iter(ctx, p)
		if err != nil {
			t.Fatalf("Iter failed: %v", err)
		}

		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			processedIDs = append(processedIDs, entity.ID)
			if len(processedIDs) == 1 {
				break
			}
		}

		if len(processedIDs) != 1 {
			t.Errorf(
				"expected iteration to stop after 1 item, but processed %d",
				len(processedIDs),
			)
		}
	})

	t.Run("query with invalid operator", func(t *testing.T) {
		p := litestore.Filter{Key: "value", Op: "INVALID", Value: 10}
		seq, err := s.Iter(ctx, p)
		if err == nil {
			t.Fatal("expected an error for invalid operator, got nil")
		}
		if seq != nil {
			t.Fatal("expected a nil iterator when an error occurs")
		}
	})
}
