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
// `T` must be a struct. If it has a field tagged with `litestore:"key"`,
// that field is used as the primary key.
type Store[T any] struct {
	db        *sql.DB
	tableName string

	// keyField holds information about the `litestore:"key"` tagged field.
	// It is nil if no such field is present.
	keyField *reflect.StructField

	// keyFieldJSONName holds the JSON key name for the key field.
	// Empty string if no key field is present.
	keyFieldJSONName string

	// validJSONKeys holds the set of JSON keys for type T.
	validJSONKeys map[string]struct{}

	// Prepared statements
	saveStmt   *sql.Stmt
	deleteStmt *sql.Stmt
}

// StoreOption defines a configuration option for Store creation.
type StoreOption func(*storeConfig)

// storeConfig holds configuration options for Store creation.
type storeConfig struct {
	indexFields []string
}

// WithIndex adds a JSON field to be indexed for improved query performance.
// Multiple WithIndex options can be specified to index multiple fields.
func WithIndex(fieldName string) StoreOption {
	return func(config *storeConfig) {
		config.indexFields = append(config.indexFields, fieldName)
	}
}

// NewStore creates a new Store instance for a given table name.
// The generic type `T` must be a struct. If it contains a string field
// with the struct tag `litestore:"key"`, this field will be used as the
// primary key. If the tag is omitted, key will be generated automatically on Save.
//
// Options can be provided to configure the store:
//   - WithIndex("fieldName"): Create an index on the specified JSON field
func NewStore[T any](ctx context.Context, db *sql.DB, tableName string, options ...StoreOption) (*Store[T], error) {
	config := &storeConfig{}
	for _, option := range options {
		option(config)
	}

	return newStore[T](ctx, db, tableName, config.indexFields)
}

func newStore[T any](ctx context.Context, db *sql.DB, tableName string, indexFields []string) (*Store[T], error) {
	if !validTableNameRe.MatchString(tableName) {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	var zero T
	typ := reflect.TypeOf(zero)
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("type T must be a struct, but got %s", typ.Kind())
	}

	var keyField *reflect.StructField
	var keyFieldJSONName string
	validJSONKeys := make(map[string]struct{})

	for i := range typ.NumField() {
		field := typ.Field(i)

		jsonTag := field.Tag.Get("json")
		jsonName := ""
		if jsonTag != "-" {
			jsonName, _, _ = strings.Cut(jsonTag, ",")
			if jsonName == "" {
				jsonName = field.Name
			}
			validJSONKeys[jsonName] = struct{}{}
		}

		if tag := field.Tag.Get("litestore"); tag == "key" {
			if field.Type.Kind() != reflect.String {
				return nil, fmt.Errorf("field with litestore:\"key\" tag must be a string, but field %s is %s", field.Name, field.Type.Kind())
			}
			f := field
			keyField = &f
			keyFieldJSONName = jsonName
		}
	}

	store := &Store[T]{
		db:               db,
		tableName:        tableName,
		keyField:         keyField,
		keyFieldJSONName: keyFieldJSONName,
		validJSONKeys:    validJSONKeys,
	}

	if err := store.init(ctx); err != nil {
		return nil, err
	}
	if err := store.createIndexes(ctx, indexFields); err != nil {
		return nil, fmt.Errorf("creating indexes for %s: %w", tableName, err)
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
// It takes a pointer to the entity to allow setting the key if a tagged field is present.
// If the entity has a `litestore:"key"` field, Save acts as an "upsert":
// - If the key field is empty, a new UUID is generated and set on the struct.
// - The entity is saved using the value of the key field as the key.
// If the entity has no `litestore:"key"` field, a new UUID is generated for each
// Save call, effectively always inserting a new record. The generated ID is not
// set on the struct.
func (s *Store[T]) Save(ctx context.Context, entity *T) error {
	stmt := s.saveStmt
	if tx, ok := GetTx(ctx); ok {
		stmt = tx.StmtContext(ctx, stmt)
	}

	var key string

	if s.keyField != nil {
		// A key field is present on the struct.
		entityValue := reflect.ValueOf(entity).Elem()
		keyFieldValue := entityValue.FieldByIndex(s.keyField.Index)

		key = keyFieldValue.String()
		if key == "" {
			key = uuid.NewString()
			if !keyFieldValue.CanSet() {
				return fmt.Errorf("cannot set key on unexported field %s", s.keyField.Name)
			}
			keyFieldValue.SetString(key)
		}
	} else {
		// No key field, so we always generate a new ID for insertion.
		key = uuid.NewString()
	}

	dataBytes, err := json.Marshal(entity)
	if err != nil {
		return fmt.Errorf("failed to marshal entity: %w", err)
	}

	_, err = stmt.ExecContext(ctx, key, dataBytes)
	if err != nil {
		return fmt.Errorf("saving entity with id %s: %w", key, err)
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

	querySQL, args, err := q.build(s.tableName, s.validJSONKeys, s.keyFieldJSONName)
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

			// If the struct has a key field, populate it with the database key
			if s.keyField != nil {
				entityValue := reflect.ValueOf(&t).Elem()
				keyFieldValue := entityValue.FieldByIndex(s.keyField.Index)
				if keyFieldValue.CanSet() {
					keyFieldValue.SetString(key)
				}
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

func (s *Store[T]) createIndexes(ctx context.Context, indexFields []string) error {
	if len(indexFields) == 0 {
		return nil
	}

	// Validate that all index fields are valid JSON keys for this type
	for _, field := range indexFields {
		if s.keyFieldJSONName != "" && field == s.keyFieldJSONName {
			// Skip key field - it's already indexed as primary key
			continue
		}

		// Only validate top-level keys. Nested keys (e.g. 'a.b') are not validated.
		if !strings.Contains(field, ".") {
			if _, ok := s.validJSONKeys[field]; !ok {
				return fmt.Errorf("invalid index field: '%s' is not a valid key for this entity", field)
			}
		}

		// Validate field name for SQL safety (similar to query.go validation)
		if strings.ContainsAny(field, ";)") {
			return fmt.Errorf("invalid character in index field: %s", field)
		}
	}

	// Create indexes for each field
	for _, field := range indexFields {
		if s.keyFieldJSONName != "" && field == s.keyFieldJSONName {
			continue // Skip key field - it's already indexed as primary key
		}

		indexName := fmt.Sprintf("idx_%s_%s", s.tableName, field)
		jsonPath := "$." + field
		createIndexSQL := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(json_extract(json, '%s'))", indexName, s.tableName, jsonPath)

		if _, err := s.db.ExecContext(ctx, createIndexSQL); err != nil {
			return fmt.Errorf("creating index %s: %w", indexName, err)
		}
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
