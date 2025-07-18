package litestore_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/dir01/litestore"
	_ "github.com/mattn/go-sqlite3"
)

// ExampleUser represents a user entity for the example.
// It's managed by an EntityStore.
type ExampleUser struct {
	Username      string    `json:"username"`
	LastMessageAt time.Time `json:"last_message_at"`
	FollowUpSent  bool      `json:"follow_up_sent"`
}

// ExampleLogEntry represents a record of a message associated with a user for the example.
// It's managed by a RecordStore.
type ExampleLogEntry struct {
	UserID  string `json:"user_id"`
	Content string `json:"content"`
	Author  string `json:"author"` // "user" or "bot"
	Sent    bool   `json:"sent"`   // True if sent, false if drafted
}

// This function demonstrates a complete workflow using an EntityStore and a RecordStore
// to find users who need a follow-up message, and then creating that message for them
// within a transaction.
func Example_followUpScenario() {
	ctx := context.Background()
	fmt.Println("Setting up database and stores...")

	// For this example, we create a temporary database.
	db, cleanup, err := setupExampleDB()
	if err != nil {
		log.Printf("failed to setup example db: %v", err)
		return
	}
	defer cleanup()

	// Create a store for User entities.
	userStore, err := litestore.NewEntityStore[ExampleUser](db, "users")
	if err != nil {
		log.Printf("Failed to create user store: %v", err)
		return
	}
	defer userStore.Close()

	// Create a store for LogEntry records.
	logStore, err := litestore.NewRecordStore[ExampleLogEntry](db, "logs", "chat_message")
	if err != nil {
		log.Printf("Failed to create log store: %v", err)
		return
	}
	defer logStore.Close()

	// Use a fixed time for deterministic output.
	mockNow := time.Date(2023, 10, 27, 15, 0, 0, 0, time.UTC)

	fmt.Println("Seeding database with test data...")
	if err := seedData(ctx, userStore, logStore, mockNow); err != nil {
		log.Printf("Failed to seed data: %v", err)
		return
	}

	fmt.Println("\n--- Initial System State ---")
	if err := printSystemState(ctx, userStore, logStore); err != nil {
		log.Printf("Failed to print system state: %v", err)
		return
	}

	fmt.Println("\n>>> Running follow-up logic...")
	if err := sendFollowUps(ctx, db, userStore, logStore, mockNow); err != nil {
		log.Printf("Failed to run follow-up logic: %v", err)
		return
	}
	fmt.Println("<<< Follow-up logic complete.")

	fmt.Println("\n--- Final System State ---")
	if err := printSystemState(ctx, userStore, logStore); err != nil {
		log.Printf("Failed to print system state: %v", err)
		return
	}

	// Output:
	// Setting up database and stores...
	// Seeding database with test data...
	//
	// --- Initial System State ---
	// User: Alice    (ID: user1) | FollowUpSent: false | LastMessageAt: 26 Oct 23 03:00 UTC
	//   -> Log: Author: user, Sent: true , Content: "the weather"
	// User: Bob      (ID: user2) | FollowUpSent: false | LastMessageAt: 27 Oct 23 03:00 UTC
	//   -> Log: Author: user, Sent: true , Content: "the weather"
	// User: Charlie  (ID: user3) | FollowUpSent: true  | LastMessageAt: 25 Oct 23 15:00 UTC
	//   -> Log: Author: user, Sent: true , Content: "the weather"
	// User: Diana    (ID: user4) | FollowUpSent: false | LastMessageAt: 23 Oct 23 15:00 UTC
	//   -> Log: Author: user, Sent: true , Content: "the weather"
	//
	// >>> Running follow-up logic...
	// Found user 'Alice' who needs a follow-up. Processing...
	// Successfully created follow-up for user 'Alice'.
	// <<< Follow-up logic complete.
	//
	// --- Final System State ---
	// User: Alice    (ID: user1) | FollowUpSent: true  | LastMessageAt: 26 Oct 23 03:00 UTC
	//   -> Log: Author: bot , Sent: false, Content: "Hi! We were recently talking about 'the weather'. I would like to follow up..."
	//   -> Log: Author: user, Sent: true , Content: "the weather"
	// User: Bob      (ID: user2) | FollowUpSent: false | LastMessageAt: 27 Oct 23 03:00 UTC
	//   -> Log: Author: user, Sent: true , Content: "the weather"
	// User: Charlie  (ID: user3) | FollowUpSent: true  | LastMessageAt: 25 Oct 23 15:00 UTC
	//   -> Log: Author: user, Sent: true , Content: "the weather"
	// User: Diana    (ID: user4) | FollowUpSent: false | LastMessageAt: 23 Oct 23 15:00 UTC
	//   -> Log: Author: user, Sent: true , Content: "the weather"
}

// sendFollowUps implements the core user story.
func sendFollowUps(
	ctx context.Context,
	db *sql.DB,
	userStore *litestore.EntityStore[*ExampleUser],
	logStore *litestore.RecordStore[ExampleLogEntry],
	now time.Time,
) error {
	twentyFourHoursAgo := now.Add(-24 * time.Hour)
	seventyTwoHoursAgo := now.Add(-72 * time.Hour)

	p := litestore.AndPredicates(
		litestore.Filter{Key: "last_message_at", Op: litestore.OpLTE, Value: twentyFourHoursAgo},
		litestore.Filter{Key: "last_message_at", Op: litestore.OpGTE, Value: seventyTwoHoursAgo},
		litestore.Filter{Key: "follow_up_sent", Op: litestore.OpEq, Value: false},
	)

	seq, err := userStore.Iter(ctx, p)
	if err != nil {
		return err
	}

	for pair, err := range seq {
		if err != nil {
			return fmt.Errorf("iteration failed: %w", err)
		}
		userID, user := pair.Key, pair.Data

		fmt.Printf("Found user '%s' who needs a follow-up. Processing...\n", user.Username)

		txErr := litestore.WithTransaction(ctx, db, func(txCtx context.Context) error {
			logs, err := logStore.List(txCtx, userID, 1)
			if err != nil {
				return fmt.Errorf("could not fetch last message for user %s: %w", userID, err)
			}
			if len(logs) == 0 {
				return nil // Nothing to do for this user.
			}
			lastLog := logs[0]

			newLog := ExampleLogEntry{
				Content: fmt.Sprintf(
					"Hi! We were recently talking about '%s'. I would like to follow up...",
					lastLog.Content,
				),
				Author: "bot",
				Sent:   false,
			}

			if err := logStore.Add(txCtx, userID, newLog); err != nil {
				return fmt.Errorf("could not add follow-up message for user %s: %w", userID, err)
			}

			update := map[string]any{"follow_up_sent": true}
			if err := userStore.Update(txCtx, userID, update); err != nil {
				return fmt.Errorf("could not update user %s: %w", userID, err)
			}

			fmt.Printf("Successfully created follow-up for user '%s'.\n", user.Username)
			return nil
		})

		if txErr != nil {
			// If a transaction for one user fails, we stop the whole process.
			// Depending on requirements, one might just log the error and continue.
			return fmt.Errorf("transaction for user %s failed: %w", userID, txErr)
		}
	}
	return nil
}

// --- Test Data and Helpers ---

func seedData(
	ctx context.Context,
	userStore *litestore.EntityStore[*ExampleUser],
	logStore *litestore.RecordStore[ExampleLogEntry],
	now time.Time,
) error {
	users := map[string]*ExampleUser{
		"user1": {Username: "Alice", LastMessageAt: now.Add(-36 * time.Hour), FollowUpSent: false},
		"user2": {Username: "Bob", LastMessageAt: now.Add(-12 * time.Hour), FollowUpSent: false},
		"user3": {Username: "Charlie", LastMessageAt: now.Add(-48 * time.Hour), FollowUpSent: true},
		"user4": {Username: "Diana", LastMessageAt: now.Add(-96 * time.Hour), FollowUpSent: false},
	}

	for id, user := range users {
		if err := userStore.Set(ctx, id, user); err != nil {
			return err
		}
		logEntry := ExampleLogEntry{Content: "the weather", Author: "user", Sent: true}
		if err := logStore.Add(ctx, id, logEntry); err != nil {
			return err
		}
	}
	return nil
}

func printSystemState(
	ctx context.Context,
	userStore *litestore.EntityStore[*ExampleUser],
	logStore *litestore.RecordStore[ExampleLogEntry],
) error {
	type userState struct {
		id   string
		user *ExampleUser
		logs []ExampleLogEntry
	}
	var states []userState

	// Collect all users first to allow for sorting.
	seq, err := userStore.Iter(ctx, nil)
	if err != nil {
		return err
	}
	for pair, err := range seq {
		if err != nil {
			return fmt.Errorf("iteration failed: %w", err)
		}
		logs, err := logStore.List(ctx, pair.Key, 5)
		if err != nil {
			return err
		}
		states = append(states, userState{id: pair.Key, user: pair.Data, logs: logs})
	}

	// Sort by ID to ensure deterministic output.
	sort.Slice(states, func(i, j int) bool {
		return states[i].id < states[j].id
	})

	for _, s := range states {
		fmt.Printf(
			"User: %-8s (ID: %s) | FollowUpSent: %-5t | LastMessageAt: %s\n",
			s.user.Username, s.id, s.user.FollowUpSent, s.user.LastMessageAt.Format(time.RFC822),
		)
		for _, entry := range s.logs {
			fmt.Printf("  -> Log: Author: %-4s, Sent: %-5t, Content: \"%s\"\n",
				entry.Author, entry.Sent, entry.Content)
		}
	}
	return nil
}

// setupExampleDB creates a temporary database in a temp directory for the example.
func setupExampleDB() (*sql.DB, func(), error) {
	dir, err := os.MkdirTemp("", "litestore_example")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s/test.db?_journal_mode=WAL", dir))
	if err != nil {
		os.RemoveAll(dir) // Clean up if open fails
		return nil, nil, fmt.Errorf("failed to open sqlite: %w", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			log.Printf("failed to close db: %v", err)
		}
		os.RemoveAll(dir)
	}

	return db, cleanup, nil
}
