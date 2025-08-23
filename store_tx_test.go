package litestore_test

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/dir01/litestore"
)

func TestStore_Transactions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	t.Run("Save on transaction commit", func(t *testing.T) {
		s, err := litestore.NewStore[TestPersonWithKey](t.Context(), db, "tx_save_commit")
		if err != nil {
			t.Fatalf("failed to create new store: %v", err)
		}
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("failed to close store: %v", err)
			}
		}()

		entity := &TestPersonWithKey{Name: "tx-commit"}
		ctxNoTx := t.Context()

		// Start transaction
		tx, err := db.BeginTx(ctxNoTx, nil)
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err.Error())
		}
		ctxWithTx := litestore.InjectTx(ctxNoTx, tx)

		// Save inside transaction
		if err := s.Save(ctxWithTx, entity); err != nil {
			t.Fatalf("failed to Save in transaction: %v", err)
		}
		entityKey := entity.K

		// Not visible outside tx
		_, err = s.GetOne(ctxNoTx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entityKey})
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected ErrNoRows outside tx, got %v", err)
		}

		// Visible inside tx
		gotInTx, err := s.GetOne(ctxWithTx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entityKey})
		if err != nil {
			t.Fatalf("expected to get entity inside tx, got err: %v", err)
		}
		if !reflect.DeepEqual(gotInTx, *entity) {
			t.Errorf("got wrong entity inside tx. \ngot: %+v\nwant: %+v", gotInTx, *entity)
		}

		// Commit
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit tx: %v", err)
		}

		// Visible outside tx after commit
		gotAfterTx, err := s.GetOne(ctxNoTx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entityKey})
		if err != nil {
			t.Fatalf("expected to get entity after commit, got err: %v", err)
		}
		if !reflect.DeepEqual(gotAfterTx, *entity) {
			t.Errorf("got wrong entity after commit. \ngot: %+v\nwant: %+v", gotAfterTx, *entity)
		}
	})

	t.Run("Save on transaction rollback", func(t *testing.T) {
		s, err := litestore.NewStore[TestPersonWithKey](t.Context(), db, "tx_save_rollback")
		if err != nil {
			t.Fatalf("failed to create new store: %v", err)
		}
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("failed to close store: %v", err)
			}
		}()

		entity := &TestPersonWithKey{Name: "tx-rollback"}
		ctxNoTx := t.Context()

		// Start transaction
		tx, err := db.BeginTx(ctxNoTx, nil)
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err.Error())
		}
		ctxWithTx := litestore.InjectTx(ctxNoTx, tx)

		// Save inside transaction
		if err := s.Save(ctxWithTx, entity); err != nil {
			t.Fatalf("failed to Save in transaction: %v", err)
		}
		entityID := entity.K

		// Rollback
		if err := tx.Rollback(); err != nil {
			t.Fatalf("failed to rollback tx: %v", err)
		}

		// Not visible outside tx after rollback
		_, err = s.GetOne(ctxNoTx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entityID})
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected ErrNoRows after rollback, got %v", err)
		}
	})

	t.Run("Delete on transaction commit", func(t *testing.T) {
		s, err := litestore.NewStore[TestPersonWithKey](t.Context(), db, "tx_delete_commit")
		if err != nil {
			t.Fatalf("failed to create new store: %v", err)
		}
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("failed to close store: %v", err)
			}
		}()

		entity := &TestPersonWithKey{Name: "tx-delete-commit"}
		ctxNoTx := t.Context()
		if err := s.Save(ctxNoTx, entity); err != nil {
			t.Fatalf("failed to save pre-test entity: %v", err)
		}
		entityID := entity.K

		// Start transaction
		tx, err := db.BeginTx(ctxNoTx, nil)
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err.Error())
		}
		ctxWithTx := litestore.InjectTx(ctxNoTx, tx)

		// Delete inside transaction
		if err := s.Delete(ctxWithTx, entityID); err != nil {
			t.Fatalf("failed to Delete in transaction: %v", err)
		}

		// Visible outside tx (still)
		_, err = s.GetOne(ctxNoTx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entityID})
		if err != nil {
			t.Fatalf("expected entity to exist outside tx before commit, got err: %v", err)
		}

		// Not visible inside tx
		_, err = s.GetOne(ctxWithTx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entityID})
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected ErrNoRows inside tx after delete, got %v", err)
		}

		// Commit
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit tx: %v", err)
		}

		// Not visible outside tx after commit
		_, err = s.GetOne(ctxNoTx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entityID})
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected ErrNoRows after commit, got %v", err)
		}
	})

	t.Run("Delete on transaction rollback", func(t *testing.T) {
		s, err := litestore.NewStore[TestPersonWithKey](t.Context(), db, "tx_delete_rollback")
		if err != nil {
			t.Fatalf("failed to create new store: %v", err)
		}
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("failed to close store: %v", err)
			}
		}()

		entity := &TestPersonWithKey{Name: "tx-delete-rollback"}
		ctxNoTx := t.Context()
		if err := s.Save(ctxNoTx, entity); err != nil {
			t.Fatalf("failed to save pre-test entity: %v", err)
		}
		entityID := entity.K

		// Start transaction
		tx, err := db.BeginTx(ctxNoTx, nil)
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err.Error())
		}
		ctxWithTx := litestore.InjectTx(ctxNoTx, tx)

		// Delete inside transaction
		if err := s.Delete(ctxWithTx, entityID); err != nil {
			t.Fatalf("failed to Delete in transaction: %v", err)
		}

		// Rollback
		if err := tx.Rollback(); err != nil {
			t.Fatalf("failed to rollback tx: %v", err)
		}

		// Still visible outside tx after rollback
		_, err = s.GetOne(ctxNoTx, litestore.Filter{Key: "k", Op: litestore.OpEq, Value: entityID})
		if err != nil {
			t.Fatalf("expected entity to exist after rollback, got err: %v", err)
		}
	})

	t.Run("Iter sees transactional state", func(t *testing.T) {
		s, err := litestore.NewStore[TestPersonWithKey](t.Context(), db, "tx_iter_state")
		if err != nil {
			t.Fatalf("failed to create new store: %v", err)
		}
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("failed to close store: %v", err)
			}
		}()

		ctxNoTx := t.Context()
		e1 := &TestPersonWithKey{Name: "iter-tx-1"}
		if err := s.Save(ctxNoTx, e1); err != nil {
			t.Fatalf("failed to save entity: %v", err)
		}

		tx, err := db.BeginTx(ctxNoTx, nil)
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err.Error())
		}
		ctxWithTx := litestore.InjectTx(ctxNoTx, tx)
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				t.Logf("failed to rollback tx: %v", err)
			}
		}() // Ensure rollback happens even on test failure

		// Add a new one and delete the old one
		e2 := &TestPersonWithKey{Name: "iter-tx-2"}
		if err := s.Save(ctxWithTx, e2); err != nil {
			t.Fatalf("failed to save entity in tx: %v", err)
		}
		if err := s.Delete(ctxWithTx, e1.K); err != nil {
			t.Fatalf("failed to delete entity in tx: %v", err)
		}

		// Iter inside tx should see only e2
		seq, err := s.Iter(ctxWithTx, nil)
		if err != nil {
			t.Fatalf("Iter failed: %v", err)
		}
		var results []TestPersonWithKey
		for entity, err := range seq {
			if err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			results = append(results, entity)
		}

		if len(results) != 1 {
			t.Fatalf("expected 1 result in tx, got %d", len(results))
		}
		if results[0].K != e2.K {
			t.Fatalf("expected to find e2 in tx, but got ID %s", results[0].K)
		}
	})
}
