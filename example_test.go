package litestore_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/dir01/litestore"
)

// User represents a user in our system.
type User struct {
	ID    string `litestore:"key"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// LoginEvent represents a login event for a user.
// It doesn't have its own ID, but is associated with a User.
type LoginEvent struct {
	UserID    string `json:"user_id"`
	Timestamp string `json:"timestamp"`
	IPAddress string `json:"ip_address"`
}

func Example() {
	// For this example, we'll create a temporary database file.
	// In a real application, you would provide a path to a persistent file.
	dbFile := "example.db"
	defer func() {
		if err := os.Remove(dbFile); err != nil {
			log.Printf("failed to remove db file: %v", err)
		}
	}()

	// Open the SQLite database.
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("failed to close db: %v", err)
		}
	}()

	ctx := context.Background()

	// Create a store for User entities.
	// The table "users" will be created if it doesn't exist.
	// For better performance on email lookups, you could add an index:
	// userStore, err := litestore.NewStore[User](ctx, db, "users", litestore.WithIndex("email"))
	userStore, err := litestore.NewStore[User](ctx, db, "users")
	if err != nil {
		log.Fatalf("failed to create user store: %v", err)
	}
	defer func() {
		if err := userStore.Close(); err != nil {
			log.Printf("failed to close user store: %v", err)
		}
	}()

	// Create a store for LoginEvent entities.
	// We'll use a separate table for these.
	eventStore, err := litestore.NewStore[LoginEvent](ctx, db, "login_events")
	if err != nil {
		log.Fatalf("failed to create event store: %v", err)
	}
	defer func() {
		if err := eventStore.Close(); err != nil {
			log.Printf("failed to close event store: %v", err)
		}
	}()

	// --- Create a new user ---
	newUser := &User{
		Name:  "Alice",
		Email: "alice@example.com",
	}
	// The key field is empty, so Save will generate a new UUID and set it.
	if err := userStore.Save(ctx, newUser); err != nil {
		log.Fatalf("failed to save user: %v", err)
	}
	fmt.Printf("Saved user '%s' with ID: %s\n", newUser.Name, newUser.ID)

	// --- Record some login events for the new user ---
	event1 := &LoginEvent{
		UserID:    newUser.ID,
		Timestamp: "2023-10-27T10:00:00Z",
		IPAddress: "192.0.2.1",
	}
	if err := eventStore.Save(ctx, event1); err != nil {
		log.Fatalf("failed to save login event: %v", err)
	}

	event2 := &LoginEvent{
		UserID:    newUser.ID,
		Timestamp: "2023-10-27T12:30:00Z",
		IPAddress: "203.0.113.5",
	}
	if err := eventStore.Save(ctx, event2); err != nil {
		log.Fatalf("failed to save login event: %v", err)
	}
	fmt.Println("Saved 2 login events for Alice.")

	// --- Retrieve a user by their email ---
	// GetOne will return an error if more than one user matches.
	p := litestore.Filter{Key: "email", Op: litestore.OpEq, Value: "alice@example.com"}
	retrievedUser, err := userStore.GetOne(ctx, p)
	if err != nil {
		log.Fatalf("failed to get user by email: %v", err)
	}
	fmt.Printf("Retrieved user: %s (%s)\n", retrievedUser.Name, retrievedUser.Email)

	// --- Iterate over all login events for that user ---
	fmt.Printf("Login events for user ID %s:\n", retrievedUser.ID)
	eventFilter := litestore.Filter{Key: "user_id", Op: litestore.OpEq, Value: retrievedUser.ID}
	eventSeq, err := eventStore.Iter(ctx, &litestore.Query{Predicate: eventFilter})
	if err != nil {
		log.Fatalf("failed to create iterator for events: %v", err)
	}

	for event, err := range eventSeq {
		if err != nil {
			log.Fatalf("failed during event iteration: %v", err)
		}
		fmt.Printf("- At %s from %s\n", event.Timestamp, event.IPAddress)
	}

	// --- Use a transaction to save a user and an event atomically ---
	fmt.Println("Performing transactional save...")
	err = litestore.WithTransaction(ctx, db, func(txCtx context.Context) error {
		// Create another user
		bob := &User{Name: "Bob", Email: "bob@example.com"}
		if err := userStore.Save(txCtx, bob); err != nil {
			return fmt.Errorf("failed to save bob in tx: %w", err)
		}

		// And an event for Bob
		bobEvent := &LoginEvent{
			UserID:    bob.ID,
			Timestamp: "2023-10-28T09:00:00Z",
			IPAddress: "198.51.100.10",
		}
		if err := eventStore.Save(txCtx, bobEvent); err != nil {
			return fmt.Errorf("failed to save bob's event in tx: %w", err)
		}

		// If this function returns an error, the transaction will be rolled back.
		// If it returns nil, it will be committed.
		return nil
	})

	if err != nil {
		log.Fatalf("transaction failed: %v", err)
	}
	fmt.Println("Transaction committed successfully.")

	// Verify Bob was saved
	bob, err := userStore.GetOne(ctx, litestore.Filter{Key: "name", Op: litestore.OpEq, Value: "Bob"})
	if err != nil {
		log.Fatalf("failed to get Bob after transaction: %v", err)
	}
	fmt.Printf("Found user '%s' after transaction.\n", bob.Name)

	// Note: The output of this example is not checked because it contains
	// non-deterministic UUIDs. The output on a typical run would look like this:
	//
	// Saved user 'Alice' with ID: <some-uuid>
	// Saved 2 login events for Alice.
	// Retrieved user: Alice (alice@example.com)
	// Login events for user ID <some-uuid>:
	// - At 2023-10-27T10:00:00Z from 192.0.2.1
	// - At 2023-10-27T12:30:00Z from 203.0.113.5
	// Performing transactional save...
	// Transaction committed successfully.
	// Found user 'Bob' after transaction.
}
