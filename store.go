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

	// validJSONKeys holds the set of JSON keys for type T.
	validJSONKeys map[string]struct{}

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
	validJSONKeys := make(map[string]struct{})

	for i := range typ.NumField() {
		field := typ.Field(i)

		if tag := field.Tag.Get("litestore"); tag == "id" {
			if field.Type.Kind() != reflect.String {
				return nil, fmt.Errorf("field with litestore:\"id\" tag must be a string, but field %s is %s", field.Name, field.Type.Kind())
			}
			f := field
			idField = &f
		}

		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}
		jsonName, _, _ := strings.Cut(jsonTag, ",")
		if jsonName == "" {
			jsonName = field.Name
		}
		validJSONKeys[jsonName] = struct{}{}
	}

	store := &Store[T]{
		db:            db,
		tableName:     tableName,
		idField:       idField,
		validJSONKeys: validJSONKeys,
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
// It returns sql.ErrNoRows if no entity is found, or an error if more than one is found.
func (s *Store[T]) GetOne(ctx context.Context, p Predicate) (T, error) {
	var zero T
	// We only need to know if there is 0, 1, or >1 result.
	// Limiting to 2 is an optimization.
	q := &Query{Predicate: p, Limit: 2}
	seq, err := s.Iter(ctx, q)
	if err != nil {
		return zero, err
	}

	var result T
	var iterErr error
	count := 0

	for entity, err := range seq {
		if err != nil {
			iterErr = err
			break
		}
		if count == 0 {
			result = entity
		}
		count++
		if count > 1 {
			break
		}
	}

	if iterErr != nil {
		return zero, fmt.Errorf("iteration failed while getting one: %w", iterErr)
	}

	if count == 0 {
		return zero, fmt.Errorf("no entity found matching predicate: %w", sql.ErrNoRows)
	}

	if count > 1 {
		return zero, fmt.Errorf("expected one result, but found multiple")
	}

	return result, nil
}

// Iter returns an iterator over entities that match a given query.
// If the query is nil, it iterates over all entities.
// The iterator yields an entity and an error for each item.
func (s *Store[T]) Iter(ctx context.Context, q *Query) (iter.Seq2[T, error], error) {
	if q == nil {
		// To simplify logic, a nil query is equivalent to an empty query.
		q = &Query{}
	}

	querySQL, args, err := q.build(s.tableName, s.validJSONKeys)
	if err != nil {
		return nil, fmt.Errorf("building query: %w", err)
	}

	var rows *sql.Rows
	var queryErr error

	if tx, ok := GetTx(ctx); ok {
		rows, queryErr = tx.QueryContext(ctx, querySQL, args...)
	} else {
		rows, queryErr = s.db.QueryContext(ctx, querySQL, args...)
	}

	if queryErr != nil {
		return nil, fmt.Errorf("querying entities with predicate: %w", queryErr)
	}

	seq := func(yield func(T, error) bool) {
		defer func() {
			_ = rows.Close()
		}()
		var zero T

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
