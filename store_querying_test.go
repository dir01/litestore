package litestore_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/dir01/litestore"
)

func TestStore_Querying_Iter(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewStore[TestPersonWithKey](t.Context(), db, "test_entities_iter")
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
	entitiesToSave := []*TestPersonWithKey{
		{Name: "alice", Category: "A", IsActive: true, Value: 30},
		{Name: "bob", Category: "A", IsActive: true, Value: 45},
		{Name: "charlie", Category: "B", IsActive: false, Value: 35},
		{Name: "david", Category: "B", IsActive: true, Value: 35},
	}
	savedEntities := make(map[string]TestPersonWithKey)
	for _, e := range entitiesToSave {
		if err := s.Save(ctx, e); err != nil {
			t.Fatalf("failed to save entity: %v", err)
		}
		savedEntities[e.K] = *e
	}

	compareResults := func(t *testing.T, got, want []TestPersonWithKey) {
		t.Helper()
		if len(got) != len(want) {
			t.Errorf("length mismatch, got %d, want %d", len(got), len(want))
			t.Logf("got: %+v", got)
			t.Logf("want: %+v", want)
			return
		}
		sort.Slice(got, func(i, j int) bool { return got[i].K < got[j].K })
		sort.Slice(want, func(i, j int) bool { return want[i].K < want[j].K })
		if !reflect.DeepEqual(got, want) {
			t.Errorf("results do not match.\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	t.Run("simple AND query", func(t *testing.T) {
		var results []TestPersonWithKey
		p := litestore.AndPredicates(
			litestore.Filter{Key: "is_active", Op: litestore.OpEq, Value: true},
			litestore.Filter{Key: "value", Op: litestore.OpGTE, Value: 35},
		)
		q := &litestore.Query{Predicate: p}
		seq, err := s.Iter(ctx, q)
		if err != nil {
			t.Fatalf("Iter failed: %v", err)
		}
		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			results = append(results, entity)
		}

		var expected []TestPersonWithKey
		for _, e := range savedEntities {
			if e.IsActive && e.Value >= 35 {
				expected = append(expected, e)
			}
		}
		compareResults(t, results, expected)
	})

	t.Run("composite (A AND B) OR C query", func(t *testing.T) {
		var results []TestPersonWithKey
		p := litestore.OrPredicates(
			litestore.AndPredicates(
				litestore.Filter{Key: "is_active", Op: litestore.OpEq, Value: true},
				litestore.Filter{Key: "value", Op: litestore.OpLT, Value: 35},
			),
			litestore.Filter{Key: "name", Op: litestore.OpEq, Value: "charlie"},
		)
		q := &litestore.Query{Predicate: p}
		seq, err := s.Iter(ctx, q)
		if err != nil {
			t.Fatalf("Iter failed: %v", err)
		}
		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			results = append(results, entity)
		}

		var expected []TestPersonWithKey
		for _, e := range savedEntities {
			if (e.IsActive && e.Value < 35) || e.Name == "charlie" {
				expected = append(expected, e)
			}
		}
		compareResults(t, results, expected)
	})

	t.Run("nil predicate returns all", func(t *testing.T) {
		var results []TestPersonWithKey
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

		var expected []TestPersonWithKey
		for _, e := range savedEntities {
			expected = append(expected, e)
		}
		compareResults(t, results, expected)
	})

	t.Run("break stops iteration", func(t *testing.T) {
		var processedIDs []string
		p := litestore.Filter{Key: "category", Op: litestore.OpEq, Value: "A"} // Should match 2 entities
		q := &litestore.Query{Predicate: p}
		seq, err := s.Iter(ctx, q)
		if err != nil {
			t.Fatalf("Iter failed: %v", err)
		}

		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			processedIDs = append(processedIDs, entity.K)
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

	t.Run("query with order and limit", func(t *testing.T) {
		var results []TestPersonWithKey
		q := &litestore.Query{
			Predicate: litestore.Filter{Key: "category", Op: litestore.OpEq, Value: "A"}, // alice, bob
			OrderBy: []litestore.OrderBy{
				{Key: "value", Direction: litestore.OrderDesc},
			},
			Limit: 1,
		}
		seq, err := s.Iter(ctx, q)
		if err != nil {
			t.Fatalf("Iter failed: %v", err)
		}
		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			results = append(results, entity)
		}

		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		// bob has value 45, alice has 30. DESC should return bob.
		if results[0].Name != "bob" {
			t.Errorf("expected bob, got %s", results[0].Name)
		}
	})

	t.Run("query with order by key", func(t *testing.T) {
		// get all entities and sort by ID descending
		var ids []string
		for _, e := range savedEntities {
			ids = append(ids, e.K)
		}
		sort.Strings(ids)
		// expected order is descending
		wantOrder := []string{ids[3], ids[2], ids[1], ids[0]}

		q := &litestore.Query{
			OrderBy: []litestore.OrderBy{
				{Key: "key", Direction: litestore.OrderDesc},
			},
		}
		seq, err := s.Iter(ctx, q)
		if err != nil {
			t.Fatalf("Iter failed: %v", err)
		}

		var gotOrder []string
		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			gotOrder = append(gotOrder, entity.K)
		}

		if !reflect.DeepEqual(gotOrder, wantOrder) {
			t.Errorf("incorrect order. got: %v, want: %v", gotOrder, wantOrder)
		}
	})
}

func TestStore_Querying_ErrorCases(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewStore[TestPersonWithKey](t.Context(), db, "test_entities_errors")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close store: %v", err)
		}
	}()

	ctx := t.Context()

	t.Run("query with invalid operator", func(t *testing.T) {
		p := litestore.Filter{Key: "value", Op: "INVALID", Value: 10}
		q := &litestore.Query{Predicate: p}
		seq, err := s.Iter(ctx, q)
		if err == nil {
			t.Fatal("expected an error for invalid operator, got nil")
		}
		if seq != nil {
			t.Fatal("expected a nil iterator when an error occurs")
		}
	})

	t.Run("query with invalid order by key", func(t *testing.T) {
		q := &litestore.Query{
			OrderBy: []litestore.OrderBy{
				{Key: "name;--", Direction: litestore.OrderAsc},
			},
		}
		_, err := s.Iter(ctx, q)
		if err == nil {
			t.Fatal("expected error for invalid order by key, but got nil")
		}
		expectedErr := "building query: invalid character in order by key: name;--"
		if err.Error() != expectedErr {
			t.Errorf("wrong error message. \ngot: %s\nwant: %s", err.Error(), expectedErr)
		}
	})

	t.Run("query with invalid order direction", func(t *testing.T) {
		q := &litestore.Query{
			OrderBy: []litestore.OrderBy{
				{Key: "name", Direction: "INVALID"},
			},
		}
		_, err := s.Iter(ctx, q)
		if err == nil {
			t.Fatal("expected error for invalid order direction, but got nil")
		}
		expectedErr := "building query: invalid order direction: INVALID"
		if err.Error() != expectedErr {
			t.Errorf("wrong error message. \ngot: %s\nwant: %s", err.Error(), expectedErr)
		}
	})
}

func TestStore_Querying_FilterOperators(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewStore[TestPersonWithKey](t.Context(), db, "test_filter_ops")
	if err != nil {
		t.Fatalf("failed to create new store: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close store: %v", err)
		}
	}()

	ctx := t.Context()

	// Setup test data
	testEntities := []*TestPersonWithKey{
		{Name: "alice", Value: 10},
		{Name: "bob", Value: 20},
		{Name: "charlie", Value: 30},
		{Name: "david", Value: 20}, // Same value as bob
	}

	for _, e := range testEntities {
		if err := s.Save(ctx, e); err != nil {
			t.Fatalf("failed to save test entity: %v", err)
		}
	}

	tests := []struct {
		name          string
		filter        litestore.Filter
		expectedNames []string
	}{
		{
			name:          "OpEq - exact match",
			filter:        litestore.Filter{Key: "value", Op: litestore.OpEq, Value: 20},
			expectedNames: []string{"bob", "david"},
		},
		{
			name:          "OpGT - greater than",
			filter:        litestore.Filter{Key: "value", Op: litestore.OpGT, Value: 20},
			expectedNames: []string{"charlie"},
		},
		{
			name:          "OpGTE - greater than or equal",
			filter:        litestore.Filter{Key: "value", Op: litestore.OpGTE, Value: 20},
			expectedNames: []string{"bob", "charlie", "david"},
		},
		{
			name:          "OpLT - less than",
			filter:        litestore.Filter{Key: "value", Op: litestore.OpLT, Value: 20},
			expectedNames: []string{"alice"},
		},
		{
			name:          "OpLTE - less than or equal",
			filter:        litestore.Filter{Key: "value", Op: litestore.OpLTE, Value: 20},
			expectedNames: []string{"alice", "bob", "david"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &litestore.Query{Predicate: tt.filter}
			seq, err := s.Iter(ctx, q)
			if err != nil {
				t.Fatalf("Iter failed: %v", err)
			}

			var resultNames []string
			for entity, err := range seq {
				if err != nil {
					t.Fatalf("iteration failed: %v", err)
				}
				resultNames = append(resultNames, entity.Name)
			}

			sort.Strings(resultNames)
			sort.Strings(tt.expectedNames)

			if !reflect.DeepEqual(resultNames, tt.expectedNames) {
				t.Errorf("filter %s: expected names %v, got %v",
					tt.name, tt.expectedNames, resultNames)
			}
		})
	}
}
