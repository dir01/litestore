package litestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"regexp"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var validTableName = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// Pair holds a key-data pair returned by an iterator.
type Pair[T any] struct {
	Key  string
	Data T
}

// EntityStore provides a key-value store for a specific entity type, backed by a dedicated SQLite table.
type EntityStore[T any] struct {
	db         *sql.DB
	tableName  string
	recordType string

	// Prepared statements
	getStmt          *sql.Stmt
	setStmt          *sql.Stmt
	updateSelectStmt *sql.Stmt
	updateUpsertStmt *sql.Stmt
}

// NewEntityStore creates a new EntityStore instance for a given table name.
// The table name must be a valid SQL identifier.
func NewEntityStore[T any](db *sql.DB, tableName string, recordType string) (*EntityStore[T], error) {
	if !validTableName.MatchString(tableName) {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	store := &EntityStore[T]{db: db, tableName: tableName}
	ctx := context.Background()

	if err := store.init(ctx); err != nil {
		return nil, err
	}
	if err := store.prepareStatements(ctx); err != nil {
		// Attempt to clean up any statements that were prepared before the error
		_ = store.Close()
		return nil, fmt.Errorf("preparing statements for %s: %w", tableName, err)
	}
	return store, nil
}

// Close releases the prepared statements. It should be called when the store is no longer needed.
func (e *EntityStore[T]) Close() error {
	var errStrings []string
	stmts := []*sql.Stmt{e.getStmt, e.setStmt, e.updateSelectStmt, e.updateUpsertStmt}
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

// Get returns a record by key. If the key is not found, it returns the zero value for T and no error.
func (e *EntityStore[T]) Get(ctx context.Context, key string) (T, error) {
	stmt := e.getStmt
	if tx, ok := GetTx(ctx); ok {
		stmt = tx.StmtContext(ctx, stmt)
	}

	var zero T
	var jsonData []byte
	err := stmt.QueryRowContext(ctx, key).Scan(&jsonData)
	if err != nil {
		if err == sql.ErrNoRows {
			return zero, nil
		}
		return zero, fmt.Errorf("querying entity data for key %s: %w", key, err)
	}

	var result T
	if err := json.Unmarshal(jsonData, &result); err != nil {
		return zero, fmt.Errorf("unmarshaling entity data for key %s: %w", key, err)
	}

	return result, nil
}

// Set completely overwrites a record. If the key exists, it's updated; otherwise, it's created.
func (e *EntityStore[T]) Set(ctx context.Context, key string, newRecord T) error {
	stmt := e.setStmt
	if tx, ok := GetTx(ctx); ok {
		stmt = tx.StmtContext(ctx, stmt)
	}

	newBytes, err := json.Marshal(newRecord)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	_, err = stmt.ExecContext(ctx, key, newBytes)
	if err != nil {
		return fmt.Errorf("upserting data for key %s: %w", key, err)
	}

	return nil
}

// Update performs a partial update of a record's JSON data.
func (e *EntityStore[T]) Update(ctx context.Context, key string, partial map[string]any) error {
	if len(partial) == 0 {
		return nil
	}

	var tx *sql.Tx
	isExternalTx := false

	if externalTx, ok := GetTx(ctx); ok {
		tx = externalTx
		isExternalTx = true
	} else {
		newTx, err := e.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("beginning transaction for key %s: %w", key, err)
		}

		tx = newTx

		defer func() {
			if rErr := newTx.Rollback(); rErr != nil && rErr != sql.ErrTxDone {
				log.Printf("failed to rollback transaction for key %s: %v", key, rErr)
			}
		}()
	}

	// Use transaction-specific statements from the prepared ones.
	// These are automatically closed when the transaction is committed or rolled back.
	txUpdateSelectStmt := tx.StmtContext(ctx, e.updateSelectStmt)
	txUpdateUpsertStmt := tx.StmtContext(ctx, e.updateUpsertStmt)

	currentData := make(map[string]any)

	var jsonData []byte
	err := txUpdateSelectStmt.QueryRowContext(ctx, key).Scan(&jsonData)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("querying entity data for key %s in transaction: %w", key, err)
	} else if err == nil {
		if err := json.Unmarshal(jsonData, &currentData); err != nil {
			return fmt.Errorf("unmarshaling existing data for key %s: %w", key, err)
		}
	}

	for k, v := range partial {
		currentData[k] = v
	}

	mergedData, err := json.Marshal(currentData)
	if err != nil {
		return fmt.Errorf("marshaling merged data for key %s: %w", key, err)
	}

	_, err = txUpdateUpsertStmt.ExecContext(ctx, key, string(mergedData))
	if err != nil {
		return fmt.Errorf("upserting data for key %s: %w", key, err)
	}

	if isExternalTx {
		return nil
	}

	return tx.Commit()
}

// Iter returns an iterator over entities that match the given predicate.
// If the predicate is nil, it iterates over all entities.
// The iterator yields a Pair and an error for each item.
func (e *EntityStore[T]) Iter(
	ctx context.Context,
	p Predicate,
) (iter.Seq2[Pair[T], error], error) {
	var queryBuilder strings.Builder
	args := []any{}

	queryBuilder.WriteString(fmt.Sprintf("SELECT key, json FROM %s", e.tableName))

	// Build the WHERE clause from the predicate tree.
	if p != nil {
		whereClause, whereArgs, buildErr := e.buildWhereClause(p)
		if buildErr != nil {
			return nil, buildErr
		}
		if whereClause != "" {
			queryBuilder.WriteString(" WHERE ")
			queryBuilder.WriteString(whereClause)
			args = append(args, whereArgs...)
		}
	}

	var rows *sql.Rows
	var queryErr error

	if tx, ok := GetTx(ctx); ok {
		rows, queryErr = tx.QueryContext(ctx, queryBuilder.String(), args...)
	} else {
		rows, queryErr = e.db.QueryContext(ctx, queryBuilder.String(), args...)
	}

	if queryErr != nil {
		return nil, fmt.Errorf("querying entity data with predicate: %w", queryErr)
	}

	seq := func(yield func(Pair[T], error) bool) {
		defer rows.Close()
		var zero Pair[T]

		for rows.Next() {
			if err := ctx.Err(); err != nil {
				yield(zero, err)
				return
			}
			var key, jsonData string

			if scanErr := rows.Scan(&key, &jsonData); scanErr != nil {
				yield(zero, fmt.Errorf("scanning entity data row: %w", scanErr))
				return
			}

			var t T
			if unmarshalErr := json.Unmarshal([]byte(jsonData), &t); unmarshalErr != nil {
				yield(zero, fmt.Errorf("unmarshaling entity data for key %q: %w", key, unmarshalErr))
				return
			}

			if !yield(Pair[T]{Key: key, Data: t}, nil) {
				return
			}
		}

		if iterErr := rows.Err(); iterErr != nil {
			yield(zero, fmt.Errorf("during row iteration: %w", iterErr))
		}
	}

	return seq, nil
}

// buildWhereClause recursively walks the predicate tree to build the SQL query.
func (e *EntityStore[T]) buildWhereClause(p Predicate) (string, []any, error) {
	switch v := p.(type) {
	case Filter:
		switch v.Op {
		case OpEq, OpNEq, OpGT, OpGTE, OpLT, OpLTE:
			// Valid operator
		default:
			return "", nil, fmt.Errorf("unsupported query operator: %s", v.Op)
		}
		sql := fmt.Sprintf("json_extract(json, ?) %s ?", v.Op)
		args := []any{"$." + v.Key, v.Value}
		return sql, args, nil

	case And:
		return e.joinPredicates(v.Predicates, "AND")

	case Or:
		return e.joinPredicates(v.Predicates, "OR")

	default:
		return "", nil, fmt.Errorf("unknown predicate type: %T", p)
	}
}

func (e *EntityStore[T]) joinPredicates(preds []Predicate, joiner string) (string, []any, error) {
	if len(preds) == 0 {
		return "", nil, nil
	}

	var clauses []string
	var allArgs []any

	for _, pred := range preds {
		clause, args, err := e.buildWhereClause(pred)
		if err != nil {
			return "", nil, err
		}
		clauses = append(clauses, clause)
		allArgs = append(allArgs, args...)
	}

	// Wrap each sub-clause in parentheses and join them.
	// e.g., ((clause1) AND (clause2))
	return fmt.Sprintf("(%s)", strings.Join(clauses, ") "+joiner+" (")), allArgs, nil
}

func (e *EntityStore[T]) init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			key TEXT NOT NULL,
			record_type TEXT NOT NULL,
			json TEXT NOT NULL,
			PRIMARY KEY (key, record_type)
		)`, e.tableName)
	_, err := e.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("creating record table %s: %w", e.tableName, err)
	}

	return nil
}

func (e *EntityStore[T]) prepareStatements(ctx context.Context) (err error) {
	// Prepare Get
	queryGet := fmt.Sprintf("SELECT json FROM %s WHERE record_type = ? and key = ?", e.tableName)
	if e.getStmt, err = e.db.PrepareContext(ctx, queryGet); err != nil {
		return fmt.Errorf("preparing get statement: %w", err)
	}

	// Prepare Set
	querySet := fmt.Sprintf(`
		INSERT INTO %s (key, record_type, json)
		VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			json = excluded.json
	`, e.tableName)
	if e.setStmt, err = e.db.PrepareContext(ctx, querySet); err != nil {
		return fmt.Errorf("preparing set statement: %w", err)
	}

	// Prepare Update (select part)
	queryUpdateSelect := fmt.Sprintf("SELECT json FROM %s WHERE key = ?", e.tableName)
	if e.updateSelectStmt, err = e.db.PrepareContext(ctx, queryUpdateSelect); err != nil {
		return fmt.Errorf("preparing update-select statement: %w", err)
	}

	// Prepare Update (upsert part)
	queryUpdateUpsert := fmt.Sprintf(`
		INSERT INTO %s (key, json)
		VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET
			json = excluded.json
	`, e.tableName)
	if e.updateUpsertStmt, err = e.db.PrepareContext(ctx, queryUpdateUpsert); err != nil {
		return fmt.Errorf("preparing update-upsert statement: %w", err)
	}

	return nil
}
