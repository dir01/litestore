package litestore_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/dir01/litestore"

	_ "github.com/mattn/go-sqlite3"
)

type FakePost struct {
	Title string `json:"title"`
	Slug  string `json:"slug"`
}

func TestEntityStore_Transactions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := litestore.NewEntityStore[*FakePost](db, "fake_posts")
	if err != nil {
		t.Fatalf("failed to create new storage: %v", err)
	}

	defer s.Close()

	t.Run("Get and Set on transaction commit", func(t *testing.T) {
		// Test plan is the following:
		// - transaction modifies data
		// - data is not seen outside the transaction
		// - data is seen from inside the transaction
		// - after commit, data is now seen outside the transaction
		postID := mkEntityID()
		fakePost := &FakePost{Title: "sometitle"}

		ctxNoTx := t.Context()

		tx, err := db.BeginTx(ctxNoTx, nil)
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err.Error())
		}

		ctxWithTx := litestore.InjectTx(ctxNoTx, tx)

		// transaction modifies data
		if err := s.Set(ctxWithTx, postID, fakePost); err != nil {
			t.Fatalf("failed to .Set() fake post in transaction: %v", err)
		}

		// data is not seen outside the transaction
		if post, err := s.Get(ctxNoTx, postID); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if post != nil {
			t.Fatalf("expected nil post outside transaction, got %v", post)
		}

		// data is seen from inside the transaction
		if post, err := s.Get(ctxWithTx, postID); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if post == nil {
			t.Fatalf("expected non-nil post from inside the transaction, got %v", post)
		} else if !reflect.DeepEqual(post, fakePost) {
			t.Fatalf("expected to get the same post from inside the transaction.\nwanted:\n%+v\n\ngot:\n%+v", fakePost, post)
		}

		// transaction is commited
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit transaction")
		}

		// data is now seen from outside the transaction as well
		if post, err := s.Get(ctxNoTx, postID); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if post == nil {
			t.Fatalf("expected non-nil post from inside the transaction, got %v", post)
		} else if !reflect.DeepEqual(post, fakePost) {
			t.Fatalf("expected to get the same post from inside the transaction.\nwanted:\n%+v\n\ngot:\n%+v", fakePost, post)
		}
	})

	t.Run("Get and Set on transaction rollback", func(t *testing.T) {
		// Test plan is the following:
		// - transaction modifies data
		// - data is not seen outside the transaction
		// - after rollback, data is still not seen outside the transaction
		postID := mkEntityID()
		fakePost := &FakePost{Title: "sometitle"}

		ctxNoTx := t.Context()

		tx, err := db.BeginTx(ctxNoTx, nil)
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err.Error())
		}

		ctxWithTx := litestore.InjectTx(ctxNoTx, tx)

		// transaction modifies data
		if err := s.Set(ctxWithTx, postID, fakePost); err != nil {
			t.Fatalf("failed to .Set() fake post in transaction: %v", err)
		}

		// data is not seen outside the transaction
		if post, err := s.Get(ctxNoTx, postID); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if post != nil {
			t.Fatalf("expected nil post outside transaction, got %v", post)
		}

		// transaction is rolled back
		if err := tx.Rollback(); err != nil {
			t.Fatalf("failed to commit transaction")
		}

		// data is still not seen outside the transaction
		if post, err := s.Get(ctxNoTx, postID); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if post != nil {
			t.Fatalf("expected nil post outside transaction, got %v", post)
		}
	})

	t.Run("Update on transaction commit", func(t *testing.T) {
		// Test plan is the following:
		// - transaction modifies data
		// - data is not seen outside the transaction
		// - data is seen from inside the transaction
		// - after commit, data is now seen outside the transaction
		postID := mkEntityID()
		expectedPost := &FakePost{Title: "sometitle"}

		ctxNoTx := t.Context()

		tx, err := db.BeginTx(ctxNoTx, nil)
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err.Error())
		}

		ctxWithTx := litestore.InjectTx(ctxNoTx, tx)

		// transaction modifies data
		if err := s.Update(ctxWithTx, postID, map[string]any{"title": "sometitle"}); err != nil {
			t.Fatalf("failed to .Update() fake post in transaction: %v", err)
		}

		// data is not seen outside the transaction
		if post, err := s.Get(ctxNoTx, postID); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if post != nil {
			t.Fatalf("expected nil post outside transaction, got %v", post)
		}

		// data is seen from inside the transaction
		if post, err := s.Get(ctxWithTx, postID); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if post == nil {
			t.Fatalf("expected non-nil post from inside the transaction, got %v", post)
		} else if !reflect.DeepEqual(post, expectedPost) {
			t.Fatalf("expected to get the same post from inside the transaction.\nwanted:\n%+v\n\ngot:\n%+v", expectedPost, post)
		}

		// transaction is commited
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit transaction")
		}

		// data is now seen from outside the transaction as well
		if post, err := s.Get(ctxNoTx, postID); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if post == nil {
			t.Fatalf("expected non-nil post from inside the transaction, got %v", post)
		} else if !reflect.DeepEqual(post, expectedPost) {
			t.Fatalf("expected to get the same post from inside the transaction.\nwanted:\n%+v\n\ngot:\n%+v", expectedPost, post)
		}
	})

	t.Run("Update on transaction rollback", func(t *testing.T) {
		// Test plan is the following:
		// - transaction modifies data
		// - data is not seen outside the transaction
		// - after rollback, data is still not seen outside the transaction
		postID := mkEntityID()

		ctxNoTx := t.Context()

		tx, err := db.BeginTx(ctxNoTx, nil)
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err.Error())
		}

		ctxWithTx := litestore.InjectTx(ctxNoTx, tx)

		// transaction modifies data
		if err := s.Update(ctxWithTx, postID, map[string]any{"title": "sometitle"}); err != nil {
			t.Fatalf("failed to .Update() fake post in transaction: %v", err)
		}

		// data is not seen outside the transaction
		if post, err := s.Get(ctxNoTx, postID); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if post != nil {
			t.Fatalf("expected nil post outside transaction, got %v", post)
		}

		// transaction is rolled back
		if err := tx.Rollback(); err != nil {
			t.Fatalf("failed to commit transaction")
		}

		// data is still not seen outside the transaction
		if post, err := s.Get(ctxNoTx, postID); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if post != nil {
			t.Fatalf("expected nil post outside transaction, got %v", post)
		}
	})

	t.Run("Iter passes transaction context", func(t *testing.T) {
		// Test plan:
		// - Create an item outside a transaction.
		// - Start a transaction and update the item.
		// - Call Iter within the transaction. It should see the *updated* item.
		// - Commit the transaction.
		// - Call Iter outside the transaction. It should see the updated item.
		postID := mkEntityID()

		originalPost := &FakePost{Title: "my-title"}
		updatedPost := &FakePost{Title: "my-updated-title"}

		if err := s.Set(t.Context(), postID, originalPost); err != nil {
			t.Fatalf("failed to Set fake post: %v", err)
		}

		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err.Error())
		}
		ctxWithTx := litestore.InjectTx(context.Background(), tx)

		// Update the post inside the transaction
		if err := s.Update(ctxWithTx, postID, map[string]any{"title": updatedPost.Title}); err != nil {
			t.Fatalf("failed to update post in transaction: %v", err)
		}

		// Iter inside the transaction should see the change
		seq, err := s.Iter(ctxWithTx, nil)
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}
		var foundInTx bool
		for pair, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			if pair.Key == postID {
				if !reflect.DeepEqual(pair.Data, updatedPost) {
					t.Fatalf("expected to see updated post inside transaction.\nwanted:\n%+v\n\ngot:\n%+v", updatedPost, pair.Data)
				}
				foundInTx = true
			}
		}
		if !foundInTx {
			t.Fatal("post not found inside transaction iterator")
		}

		// Commit the transaction
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit transaction")
		}

		// Iter outside the transaction should now see the change
		seq, err = s.Iter(t.Context(), nil)
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}
		var foundAfterTx bool
		for pair, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			if pair.Key == postID {
				if !reflect.DeepEqual(pair.Data, updatedPost) {
					t.Fatalf("expected to see updated post after commit.\nwanted:\n%+v\n\ngot:\n%+v", updatedPost, pair.Data)
				}
				foundAfterTx = true
			}
		}
		if !foundAfterTx {
			t.Fatal("post not found after transaction iterator")
		}
	})
}
