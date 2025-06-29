package litestore_test

// import (
// 	"context"
// 	"errors"
// 	"reflect"
// 	"testing"
// 	"time"

// 	"github.com/dir01/litestore"
// )

// func TestRecordsStore_Transactions(t *testing.T) {
// 	db, cleanup := setupTestDB(t)
// 	defer cleanup()

// 	chatStore, err := litestore.NewRecordStore[ChatMessage](db, "chat", "chat")
// 	if err != nil {
// 		t.Fatalf("failed to create chat storage: %v", err)
// 	}
// 	defer chatStore.Close()

// 	ctx := t.Context()
// 	userID := mkEntityID()

// 	t.Run("Add and List on transaction commit", func(t *testing.T) {
// 		retrieved, err := chatStore.List(ctx, userID, 10) // limit > num items
// 		if err != nil {
// 			t.Fatalf("failed to get user items: %v", err)
// 		}

// 		if len(retrieved) != len(messages) {
// 			t.Fatalf("expected %d messages, got %d", len(messages), len(retrieved))
// 		}

// 		// Items should be returned in reverse order of insertion
// 		expected := []ChatMessage{messages[2], messages[1], messages[0]}
// 		if !reflect.DeepEqual(retrieved, expected) {
// 			t.Errorf("retrieved messages do not match expected order.\ngot:  %v\nwant: %v", retrieved, expected)
// 		}
// 	})

// 	t.Run("get limited items", func(t *testing.T) {
// 		retrieved, err := chatStore.List(ctx, userID, 2)
// 		if err != nil {
// 			t.Fatalf("failed to get user items: %v", err)
// 		}

// 		if len(retrieved) != 2 {
// 			t.Fatalf("expected 2 messages, got %d", len(retrieved))
// 		}

// 		// Should be the last two inserted messages, in reverse order
// 		expected := []ChatMessage{messages[2], messages[1]}
// 		if !reflect.DeepEqual(retrieved, expected) {
// 			t.Errorf("retrieved messages do not match expected order.\ngot:  %v\nwant: %v", retrieved, expected)
// 		}
// 	})

// 	t.Run("get items of other type", func(t *testing.T) {
// 		retrieved, err := logStore.List(ctx, userID, 10)
// 		if err != nil {
// 			t.Fatalf("failed to get user items: %v", err)
// 		}

// 		if len(retrieved) != 1 {
// 			t.Fatalf("expected 1 log entry, got %d", len(retrieved))
// 		}

// 		if !reflect.DeepEqual(retrieved[0], logEntry) {
// 			t.Errorf("retrieved log entry does not match.\ngot:  %v\nwant: %v", retrieved[0], logEntry)
// 		}
// 	})

// 	t.Run("get items for user with no items", func(t *testing.T) {
// 		otherUserID := mkEntityID()
// 		retrieved, err := chatStore.List(ctx, otherUserID, 10)
// 		if err != nil {
// 			t.Fatalf("failed to get user items: %v", err)
// 		}

// 		if len(retrieved) != 0 {
// 			t.Fatalf("expected 0 messages, got %d", len(retrieved))
// 		}
// 	})

// 	t.Run("context cancellation stops iteration", func(t *testing.T) {
// 		// Add many more messages to ensure the loop is slow enough to be interrupted.
// 		for i := 0; i < 500; i++ {
// 			err := chatStore.Add(ctx, userID, ChatMessage{Role: "user", Content: "spam"})
// 			if err != nil {
// 				t.Fatalf("failed to add spam message: %v", err)
// 			}
// 		}

// 		cancelCtx, cancel := context.WithCancel(ctx)
// 		defer cancel()

// 		go func() {
// 			// Give the List operation a moment to start, then cancel it.
// 			time.Sleep(1 * time.Millisecond)
// 			cancel()
// 		}()

// 		// The limit is high to ensure we would be looping for a while.
// 		_, err := chatStore.List(cancelCtx, userID, 1000)

// 		if !errors.Is(err, context.Canceled) {
// 			t.Fatalf("expected error '%v', but got '%v'", context.Canceled, err)
// 		}
// 	})
// }
