package litestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"
	"reflect"
	"regexp"
	"strings"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

var validTableNameRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// Store provides a key-value store for a specific entity type `T`.
// `T` must be a struct. If it has a field tagged with `litestore:"id"`,
// that field is used as the primary key.
type Store[T any] struct {
	db        *sql.DB
	tableName string

	// idField holds information about the `litestore:"id"` tagged field.
	// It is nil if no such field is present.
	idField *reflect.StructField

	// Prepared statements
	saveStmt   *sql.Stmt
	deleteStmt *sql.Stmt
}

// NewStore creates a new Store instance for a given table name.
// The generic type `T` must be a struct. If it contains a string field
// with the struct tag `litestore:"id"`, this field will be used as the
// primary key. If the tag is omitted, IDs are generated automatically on Save.
func NewStore[T any](ctx context.Context, db *sql.DB, tableName string) (*Store[T], error) {
	if !validTableNameRe.MatchString(tableName) {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	var zero T
	typ := reflect.TypeOf(zero)
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("type T must be a struct, but got %s", typ.Kind())
	}

	var idField *reflect.StructField
	for i := range typ.NumField() {
		field := typ.Field(i)
		if tag := field.Tag.Get("litestore"); tag == "id" {
			if field.Type.Kind() != reflect.String {
				return nil, fmt.Errorf("field with litestore:\"id\" tag must be a string, but field %s is %s", field.Name, field.Type.Kind())
			}
			f := field
			idField = &f
			break
		}
	}

	store := &Store[T]{
		db:        db,
		tableName: tableName,
		idField:   idField,
	}

	if err := store.init(ctx); err != nil {
		return nil, err
	}
	if err := store.prepareStatements(ctx); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("preparing statements for %s: %w", tableName, err)
	}
	return store, nil
}

// Close releases the prepared statements. It should be called when the store is no longer needed.
func (s *Store[T]) Close() error {
	var errStrings []string
	stmts := []*sql.Stmt{s.saveStmt, s.deleteStmt}
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

// Save stores an entity in the database.
// It takes a pointer to the entity to allow setting the ID if a tagged field is present.
// If the entity has a `litestore:"id"` field, Save acts as an "upsert":
// - If the ID field is empty, a new UUID is generated and set on the struct.
// - The entity is saved using the value of the ID field as the key.
// If the entity has no `litestore:"id"` field, a new UUID is generated for each
// Save call, effectively always inserting a new record. The generated ID is not
// set on the struct.
func (s *Store[T]) Save(ctx context.Context, entity *T) error {
	stmt := s.saveStmt
	if tx, ok := GetTx(ctx); ok {
		stmt = tx.StmtContext(ctx, stmt)
	}

	var id string

	if s.idField != nil {
		// An ID field is present on the struct.
		entityValue := reflect.ValueOf(entity).Elem()
		idFieldValue := entityValue.FieldByIndex(s.idField.Index)

		id = idFieldValue.String()
		if id == "" {
			id = uuid.NewString()
			if !idFieldValue.CanSet() {
				return fmt.Errorf("cannot set ID on unexported field %s", s.idField.Name)
			}
			idFieldValue.SetString(id)
		}
	} else {
		// No ID field, so we always generate a new ID for insertion.
		id = uuid.NewString()
	}

	dataBytes, err := json.Marshal(entity)
	if err != nil {
		return fmt.Errorf("failed to marshal entity: %w", err)
	}

	_, err = stmt.ExecContext(ctx, id, dataBytes)
	if err != nil {
		return fmt.Errorf("saving entity with id %s: %w", id, err)
	}

	return nil
}

// Delete removes an entity from the store by its key.
func (s *Store[T]) Delete(ctx context.Context, key string) error {
	stmt := s.deleteStmt
	if tx, ok := GetTx(ctx); ok {
		stmt = tx.StmtContext(ctx, stmt)
	}

	_, err := stmt.ExecContext(ctx, key)
	if err != nil {
		return fmt.Errorf("deleting entity with key %s: %w", key, err)
	}

	return nil
}

// GetOne retrieves a single entity that matches the given predicate.
// If no entity matches, it returns the zero value of T and an error.
// If multiple entities match, it returns the first one found.
func (s *Store[T]) GetOne(ctx context.Context, p Predicate) (T, error) {
	var zero T
	var queryBuilder strings.Builder
	args := []any{}

	queryBuilder.WriteString(fmt.Sprintf("SELECT json FROM %s", s.tableName))

	if p != nil {
		whereClause, whereArgs, err := s.buildWhereClause(p)
		if err != nil {
			return zero, err
		}
		if whereClause != "" {
			queryBuilder.WriteString(" WHERE ")
			queryBuilder.WriteString(whereClause)
			args = append(args, whereArgs...)
		}
	}
	queryBuilder.WriteString(" LIMIT 1")

	var row *sql.Row
	if tx, ok := GetTx(ctx); ok {
		row = tx.QueryRowContext(ctx, queryBuilder.String(), args...)
	} else {
		row = s.db.QueryRowContext(ctx, queryBuilder.String(), args...)
	}

	var jsonData []byte
	if err := row.Scan(&jsonData); err != nil {
		if err == sql.ErrNoRows {
			return zero, fmt.Errorf("no entity found matching predicate: %w", err)
		}
		return zero, fmt.Errorf("querying single entity: %w", err)
	}

	var result T
	if err := json.Unmarshal(jsonData, &result); err != nil {
		return zero, fmt.Errorf("unmarshaling entity: %w", err)
	}

	return result, nil
}

// Iter returns an iterator over entities that match the given predicate.
// If the predicate is nil, it iterates over all entities.
// The iterator yields an entity and an error for each item.
func (s *Store[T]) Iter(ctx context.Context, p Predicate) (iter.Seq2[T, error], error) {
	var queryBuilder strings.Builder
	args := []any{}

	queryBuilder.WriteString(fmt.Sprintf("SELECT json FROM %s", s.tableName))

	if p != nil {
		whereClause, whereArgs, err := s.buildWhereClause(p)
		if err != nil {
			return nil, err
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
		rows, queryErr = s.db.QueryContext(ctx, queryBuilder.String(), args...)
	}

	if queryErr != nil {
		return nil, fmt.Errorf("querying entities with predicate: %w", queryErr)
	}

	seq := func(yield func(T, error) bool) {
		defer rows.Close()
		var zero T

		for rows.Next() {
			if err := ctx.Err(); err != nil {
				yield(zero, err)
				return
			}
			var jsonData string
			if scanErr := rows.Scan(&jsonData); scanErr != nil {
				yield(zero, fmt.Errorf("scanning entity data row: %w", scanErr))
				return
			}

			var t T
			if unmarshalErr := json.Unmarshal([]byte(jsonData), &t); unmarshalErr != nil {
				yield(zero, fmt.Errorf("unmarshaling entity data: %w", unmarshalErr))
				return
			}

			if !yield(t, nil) {
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
func (s *Store[T]) buildWhereClause(p Predicate) (string, []any, error) {
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
		return s.joinPredicates(v.Predicates, "AND")

	case Or:
		return s.joinPredicates(v.Predicates, "OR")

	default:
		return "", nil, fmt.Errorf("unknown predicate type: %T", p)
	}
}

func (s *Store[T]) joinPredicates(preds []Predicate, joiner string) (string, []any, error) {
	if len(preds) == 0 {
		return "", nil, nil
	}

	var clauses []string
	var allArgs []any

	for _, pred := range preds {
		clause, args, err := s.buildWhereClause(pred)
		if err != nil {
			return "", nil, err
		}
		clauses = append(clauses, clause)
		allArgs = append(allArgs, args...)
	}

	return fmt.Sprintf("(%s)", strings.Join(clauses, ") "+joiner+" (")), allArgs, nil
}

func (s *Store[T]) init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			key TEXT PRIMARY KEY,
			json TEXT NOT NULL
		)`, s.tableName)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("creating table %s: %w", s.tableName, err)
	}
	return nil
}

func (s *Store[T]) prepareStatements(ctx context.Context) (err error) {
	// Prepare Save
	querySave := fmt.Sprintf(`
		INSERT INTO %s (key, json)
		VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET
			json = excluded.json
	`, s.tableName)
	if s.saveStmt, err = s.db.PrepareContext(ctx, querySave); err != nil {
		return fmt.Errorf("preparing save statement: %w", err)
	}

	// Prepare Delete
	queryDelete := fmt.Sprintf("DELETE FROM %s WHERE key = ?", s.tableName)
	if s.deleteStmt, err = s.db.PrepareContext(ctx, queryDelete); err != nil {
		return fmt.Errorf("preparing delete statement: %w", err)
	}

	return nil
}
