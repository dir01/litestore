# litestore

`litestore` is a lightweight, schemaless, and typesafe storage layer for Go, built on top of SQLite. It provides a simple and intuitive API for persisting Go structs as JSON data, without the complexity of a full-featured ORM.

[![Go Reference](https://pkg.go.dev/badge/github.com/dir01/litestore.svg)](https://pkg.go.dev/github.com/dir01/litestore)

## Key Features

*   **Schemaless & Typesafe**: Store your Go structs directly without defining a schema, while still benefiting from Go's type safety.
*   **Simple API**: A minimal and opinionated API for common CRUD operations.
*   **Flexible Querying**: Build complex queries using a simple and composable predicate system.
*   **Transactional Support**: Execute multiple operations in a single, atomic transaction.
*   **Zero-Dependency**: The entire database is a single file on disk, making it perfect for simple applications, command-line tools, and prototypes.

## Getting Started

First, add `litestore` to your project:

```sh
go get github.com/dir01/litestore
```

Next, you can use `litestore` to save and retrieve your Go structs:

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/dir01/litestore"
	"_github.com/mattn/go-sqlite3"
)

// User represents a user in our system.
type User struct {
	ID    string `litestore:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func main() {
	// Open the SQLite database.
	db, err := sql.Open("sqlite3", "example.db")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a store for User entities.
	userStore, err := litestore.NewStore[User](ctx, db, "users")
	if err != nil {
		log.Fatalf("failed to create user store: %v", err)
	}
	defer userStore.Close()

	// --- Create a new user ---
	newUser := &User{
		Name:  "Alice",
		Email: "alice@example.com",
	}
	if err := userStore.Save(ctx, newUser); err != nil {
		log.Fatalf("failed to save user: %v", err)
	}
	fmt.Printf("Saved user '%s' with ID: %s\n", newUser.Name, newUser.ID)

	// --- Retrieve a user by their email ---
	retrievedUser, err := userStore.GetOne(ctx, litestore.Filter{Key: "email", Op: litestore.OpEq, Value: "alice@example.com"})
	if err != nil {
		log.Fatalf("failed to get user by email: %v", err)
	}
	fmt.Printf("Retrieved user: %s (%s)\n", retrievedUser.Name, retrievedUser.Email)
}
```

## Querying

`litestore` provides a flexible querying system that allows you to build complex queries using a simple and composable predicate system.

### Filters

A `Filter` represents a single condition in a query. For example, to find all users with the name "Alice", you would use the following filter:

```go
litestore.Filter{Key: "name", Op: litestore.OpEq, Value: "Alice"}
```

### Combining Predicates

You can combine multiple predicates using `AndPredicates` and `OrPredicates` to create more complex queries. For example, to find all users with the name "Alice" who are also active, you would use the following query:

```go
litestore.AndPredicates(
	litestore.Filter{Key: "name", Op: litestore.OpEq, Value: "Alice"},
	litestore.Filter{Key: "is_active", Op: litestore.OpEq, Value: true},
)
```

### Ordering and Limiting

You can also order and limit your query results using the `OrderBy` and `Limit` fields of the `Query` struct.

```go
q := &litestore.Query{
	Predicate: litestore.Filter{Key: "category", Op: litestore.OpEq, Value: "A"},
	OrderBy: []litestore.OrderBy{
		{Key: "value", Direction: litestore.OrderDesc},
	},
	Limit: 10,
}
```

## Transactions

`litestore` supports transactions, allowing you to execute multiple operations in a single, atomic transaction. The `WithTransaction` function provides a simple and convenient way to work with transactions:

```go
err = litestore.WithTransaction(ctx, db, func(txCtx context.Context) error {
	// Create a new user
	bob := &User{Name: "Bob", Email: "bob@example.com"}
	if err := userStore.Save(txCtx, bob); err != nil {
		return fmt.Errorf("failed to save bob in tx: %w", err)
	}

	// If this function returns an error, the transaction will be rolled back.
	// If it returns nil, it will be committed.
	return nil
})
```
