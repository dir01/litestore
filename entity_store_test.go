package litestore_test

import (
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/dir01/litestore"

	_ "github.com/mattn/go-sqlite3"
)

type FakeUser struct {
	Username  string `json:"username"`
	Email     string `json:"email"`
	IsPremium bool   `json:"is_premium"`
	Age       int    `json:"age"`
}

func TestEntityStore_Get_Set(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewEntityStore[*FakeUser](db, "fake_users")
	if err != nil {
		t.Fatalf("failed to create new storage: %v", err)
	}

	t.Run("Get on empty storage", func(t *testing.T) {
		userID := mkEntityID()

		user, err := s.Get(t.Context(), userID)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if user != nil {
			t.Fatalf("expected nil user for non-existent key, got %v", user)
		}
	})

	t.Run("Get after Set", func(t *testing.T) {
		userID := mkEntityID()
		initialUser := &FakeUser{Username: "someusername", Age: 30}

		err := s.Set(t.Context(), userID, initialUser)
		if err != nil {
			t.Fatalf("failed to .Set() user: %v", err)
		}

		got, err := s.Get(t.Context(), userID)
		if err != nil {
			t.Fatalf("failed to .Get() user data after .Set(): %v", err)
		}

		if !reflect.DeepEqual(got, initialUser) {
			t.Errorf(
				"retrieved data does not match initial data.\ngot:  %v\nwant: %v",
				got, initialUser,
			)
		}
	})

	t.Run("Set overwrites data", func(t *testing.T) {
		userID := mkEntityID()
		firstUser := &FakeUser{Username: "foouser", Email: "foo@example.com", IsPremium: false, Age: 25}
		secondUser := &FakeUser{Username: "baruser", Email: "bar@example.com", IsPremium: true, Age: 52}

		err := s.Set(t.Context(), userID, firstUser)
		if err != nil {
			t.Fatalf("failed to initially .Set() user: %v", err)
		}

		err = s.Set(t.Context(), userID, secondUser)
		if err != nil {
			t.Fatalf("failed to secondarily .Set() user: %v", err)
		}

		got, err := s.Get(t.Context(), userID)
		if err != nil {
			t.Fatalf("failed to .Get() user data after .Set(): %v", err)
		}

		if !reflect.DeepEqual(got, secondUser) {
			t.Errorf(
				"retrieved user does not match expected data.\ngot:  %v\nwant: %v",
				got, secondUser,
			)
		}
	})
}

func TestEntityStore_Update(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewEntityStore[*FakeUser](db, "fake_users")
	if err != nil {
		t.Fatalf("failed to create new storage: %v", err)
	}

	t.Run("Update merges data", func(t *testing.T) {
		userID := mkEntityID()
		user := &FakeUser{Username: "foouser", Email: "foo@example.com", IsPremium: true, Age: 40}

		err := s.Set(t.Context(), userID, user)
		if err != nil {
			t.Fatalf("failed to initially .Set() user: %v", err)
		}

		err = s.Update(t.Context(), userID, map[string]any{"username": "baruser", "age": 41})
		if err != nil {
			t.Fatalf("failed to .Update() user: %v", err)
		}

		got, err := s.Get(t.Context(), userID)
		if err != nil {
			t.Fatalf("failed to get user data after update: %v", err)
		}

		expectedUser := &FakeUser{
			Username:  "baruser",
			Email:     "foo@example.com",
			IsPremium: true,
			Age:       41,
		}

		if !reflect.DeepEqual(got, expectedUser) {
			t.Errorf(
				"retrieved user does not match expected data.\ngot:  %v\nwant: %v",
				got, expectedUser,
			)
		}
	})
}

func TestEntityStore_ForEach(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewEntityStore[*FakeUser](db, "query_users")
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	// Setup test data
	users := map[string]*FakeUser{
		"user1": {Username: "alice", IsPremium: true, Age: 30},
		"user2": {Username: "bob", IsPremium: true, Age: 45},
		"user3": {Username: "charlie", IsPremium: false, Age: 35},
		"user4": {Username: "david", IsPremium: true, Age: 35},
	}
	for id, data := range users {
		if err := s.Set(t.Context(), id, data); err != nil {
			t.Fatalf("failed to setup user %s: %v", id, err)
		}
	}

	type result struct {
		Key  string
		User *FakeUser
	}

	// Helper to compare slices of results without order sensitivity
	compareResults := func(t *testing.T, got, want []result) {
		t.Helper()
		if len(got) != len(want) {
			t.Errorf("length mismatch, got %d, want %d", len(got), len(want))
			t.Logf("got: %v", got)
			t.Logf("want: %v", want)
			return
		}
		sort.Slice(got, func(i, j int) bool { return got[i].Key < got[j].Key })
		sort.Slice(want, func(i, j int) bool { return want[i].Key < want[j].Key })
		if !reflect.DeepEqual(got, want) {
			t.Errorf("results do not match.\ngot:  %v\nwant: %v", got, want)
		}
	}

	t.Run("simple AND query", func(t *testing.T) {
		var results []result
		p := litestore.AndPredicates(
			litestore.Filter{Key: "is_premium", Op: litestore.OpEq, Value: true},
			litestore.Filter{Key: "age", Op: litestore.OpGTE, Value: 35},
		)
		err := s.ForEach(t.Context(), p, func(key string, user *FakeUser) error {
			results = append(results, result{Key: key, User: user})
			return nil
		})
		if err != nil {
			t.Fatalf("ForEach failed: %v", err)
		}
		expected := []result{
			{Key: "user2", User: users["user2"]},
			{Key: "user4", User: users["user4"]},
		}
		compareResults(t, results, expected)
	})

	t.Run("composite (A AND B) OR C query", func(t *testing.T) {
		var results []result
		p := litestore.OrPredicates(
			litestore.AndPredicates(
				litestore.Filter{Key: "is_premium", Op: litestore.OpEq, Value: true},
				litestore.Filter{Key: "age", Op: litestore.OpLT, Value: 35},
			),
			litestore.Filter{Key: "username", Op: litestore.OpEq, Value: "charlie"},
		)
		err := s.ForEach(t.Context(), p, func(key string, user *FakeUser) error {
			results = append(results, result{Key: key, User: user})
			return nil
		})
		if err != nil {
			t.Fatalf("ForEach failed: %v", err)
		}
		// Expects alice (premium and age < 35) and charlie (username is charlie)
		expected := []result{
			{Key: "user1", User: users["user1"]},
			{Key: "user3", User: users["user3"]},
		}
		compareResults(t, results, expected)
	})

	t.Run("nil predicate returns all", func(t *testing.T) {
		var results []result
		err := s.ForEach(t.Context(), nil, func(key string, user *FakeUser) error {
			results = append(results, result{Key: key, User: user})
			return nil
		})
		if err != nil {
			t.Fatalf("ForEach failed: %v", err)
		}
		expected := []result{
			{Key: "user1", User: users["user1"]},
			{Key: "user2", User: users["user2"]},
			{Key: "user3", User: users["user3"]},
			{Key: "user4", User: users["user4"]},
		}
		compareResults(t, results, expected)
	})

	t.Run("callback error stops iteration", func(t *testing.T) {
		var processedKeys []string
		stopErr := errors.New("stop iteration")
		p := litestore.Filter{Key: "is_premium", Op: litestore.OpEq, Value: true} // Should match 3 users

		err := s.ForEach(t.Context(), p, func(key string, user *FakeUser) error {
			processedKeys = append(processedKeys, key)
			if len(processedKeys) == 2 {
				return stopErr
			}
			return nil
		})

		if !errors.Is(err, stopErr) {
			t.Fatalf("expected error '%v', but got '%v'", stopErr, err)
		}

		if len(processedKeys) != 2 {
			t.Errorf(
				"expected iteration to stop after 2 items, but processed %d",
				len(processedKeys),
			)
		}
	})

	t.Run("query with invalid operator", func(t *testing.T) {
		p := litestore.Filter{Key: "age", Op: "INVALID", Value: 10}
		err := s.ForEach(t.Context(), p, func(key string, user *FakeUser) error {
			t.Error("callback should not be called for invalid query")
			return nil
		})
		if err == nil {
			t.Fatal("expected an error for invalid operator, got nil")
		}
	})
}
