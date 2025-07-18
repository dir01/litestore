package litestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// RecordStore stores collections of items of a specific type `T`,
// associated with an entity ID.
type RecordStore[T any] struct {
	db         *sql.DB
	tableName  string
	recordType string

	// Prepared statements
	addStmt  *sql.Stmt
	listStmt *sql.Stmt
}

// NewRecordStore creates a new RecordsStore instance for a given table and record type.
// All records managed by this store will be of type T and stored with the given recordType.
func NewRecordStore[T any](ctx context.Context, db *sql.DB, tableName string, recordType string) (*RecordStore[T], error) {
	if !validTableName.MatchString(tableName) {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}
	store := &RecordStore[T]{
		db:         db,
		tableName:  tableName,
		recordType: recordType,
	}

	if err := store.init(ctx); err != nil {
		return nil, err
	}
	if err := store.prepareStatements(ctx); err != nil {
		_ = store.Close() // Attempt to clean up any statements that were prepared
		return nil, fmt.Errorf("preparing statements for %s/%s: %w", tableName, recordType, err)
	}
	return store, nil
}

// Close releases the prepared statements. It should be called when the store is no longer needed.
func (r *RecordStore[T]) Close() error {
	var errStrings []string

	stmts := []*sql.Stmt{r.addStmt, r.listStmt}
	for _, stmt := range stmts {
		if stmt != nil {
			if err := stmt.Close(); err != nil {
				errStrings = append(errStrings, err.Error())
			}
		}
	}

	if len(errStrings) > 0 {
		return fmt.Errorf("errors while closing statements: %s", strings.Join(errStrings, "; "))
	}

	return nil
}

// Add adds a new item to an entity's collection of records.
func (r *RecordStore[T]) Add(ctx context.Context, entityID string, item T) error {
	dataBytes, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}

	_, err = r.addStmt.ExecContext(ctx, entityID, r.recordType, string(dataBytes))
	if err != nil {
		return fmt.Errorf("inserting record for entity %s: %w", entityID, err)
	}
	return nil
}

// List retrieves a collection of items for a given entity and record type.
func (r *RecordStore[T]) List(ctx context.Context, entityID string, limit int) ([]T, error) {
	rows, err := r.listStmt.QueryContext(ctx, entityID, r.recordType, limit)
	if err != nil {
		return nil, fmt.Errorf("querying records for entity %s: %w", entityID, err)
	}
	defer rows.Close()

	var results []T
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		var jsonStr string
		if err := rows.Scan(&jsonStr); err != nil {
			return nil, fmt.Errorf("scanning record row: %w", err)
		}

		var item T
		if err := json.Unmarshal([]byte(jsonStr), &item); err != nil {
			return nil, fmt.Errorf("unmarshaling record: %w", err)
		}
		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (r *RecordStore[T]) init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			entity_id TEXT NOT NULL,
			record_type TEXT NOT NULL,
			json TEXT NOT NULL
		)`, r.tableName)
	if _, err := r.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("creating records table %s: %w", r.tableName, err)
	}
	return nil
}

func (r *RecordStore[T]) prepareStatements(ctx context.Context) (err error) {
	// Prepare Add
	queryAdd := fmt.Sprintf(`
		INSERT INTO %s (entity_id, record_type, json)
		VALUES (?, ?, ?)
	`, r.tableName)
	if r.addStmt, err = r.db.PrepareContext(ctx, queryAdd); err != nil {
		return fmt.Errorf("preparing add statement: %w", err)
	}

	// Prepare List
	queryList := fmt.Sprintf(`
		SELECT json FROM %s
		WHERE entity_id = ? AND record_type = ?
		ORDER BY id DESC
		LIMIT ?
	`, r.tableName)
	if r.listStmt, err = r.db.PrepareContext(ctx, queryList); err != nil {
		return fmt.Errorf("preparing list statement: %w", err)
	}

	return nil
}
