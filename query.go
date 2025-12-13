package litestore

import (
	"fmt"
	"reflect"
	"strings"
)

// Query encapsulates all parts of a database query.
type Query struct {
	Predicate Predicate
	OrderBy   []OrderBy
	Limit     int
}

// OrderDirection defines the sorting direction.
type OrderDirection string

const (
	OrderAsc  OrderDirection = "ASC"
	OrderDesc OrderDirection = "DESC"
)

// OrderBy specifies a field to sort the results by.
type OrderBy struct {
	// Key is the field name to sort by. It can be a top-level property (e.g., 'name'),
	// or a nested JSON path (e.g., 'user.name'). If the entity has a key field,
	// you can use its JSON field name to sort by the primary key.
	Key       string
	Direction OrderDirection
}

// build constructs the SQL query string and arguments.
// It assumes q is not nil.
// keyFieldName is the JSON key name for the primary key field (empty string if no key field).
func (q *Query) build(tableName string, validKeys map[string]struct{}, keyFieldName string) (string, []any, error) {
	var queryBuilder strings.Builder
	args := []any{}

	queryBuilder.WriteString(fmt.Sprintf("SELECT key, json FROM %s", tableName))

	if q.Predicate != nil {
		whereClause, whereArgs, err := buildWhereClause(q.Predicate, validKeys, keyFieldName)
		if err != nil {
			return "", nil, err
		}
		if whereClause != "" {
			queryBuilder.WriteString(" WHERE ")
			queryBuilder.WriteString(whereClause)
			args = append(args, whereArgs...)
		}
	}

	if len(q.OrderBy) > 0 {
		var orderClauses []string
		for _, o := range q.OrderBy {
			if o.Direction != OrderAsc && o.Direction != OrderDesc {
				return "", nil, fmt.Errorf("invalid order direction: %s", o.Direction)
			}
			// Check if this is ordering by the primary key field
			if keyFieldName != "" && o.Key == keyFieldName {
				// Use the key column directly for better performance
				orderClauses = append(orderClauses, fmt.Sprintf("key %s", o.Direction))
			} else {
				if strings.ContainsAny(o.Key, ";)") {
					return "", nil, fmt.Errorf("invalid character in order by key: %s", o.Key)
				}
				// Only validate top-level keys. Nested keys (e.g. 'a.b') are not validated.
				if !strings.Contains(o.Key, ".") {
					if _, ok := validKeys[o.Key]; !ok {
						return "", nil, fmt.Errorf("invalid order by key: '%s' is not a valid key for this entity", o.Key)
					}
				}
				orderClauses = append(orderClauses, fmt.Sprintf("json_extract(json, ?) %s", o.Direction))
				args = append(args, "$."+o.Key)
			}
		}
		queryBuilder.WriteString(" ORDER BY ")
		queryBuilder.WriteString(strings.Join(orderClauses, ", "))
	}

	if q.Limit > 0 {
		queryBuilder.WriteString(" LIMIT ?")
		args = append(args, q.Limit)
	}

	return queryBuilder.String(), args, nil
}

// Predicate represents a part of a query's WHERE clause.
// It's a "closed" interface, meaning only types within this package can implement it.
type Predicate interface {
	isPredicate()
}

// Operator defines the comparison operator for a query filter.
type Operator string

// Supported query operators.
const (
	OpEq    Operator = "="
	OpNEq   Operator = "!="
	OpGT    Operator = ">"
	OpGTE   Operator = ">="
	OpLT    Operator = "<"
	OpLTE   Operator = "<="
	OpIn    Operator = "IN"
	OpNotIn Operator = "NOT IN"
)

// Filter is a Predicate that represents a single condition (e.g., 'level > 10').
type Filter struct {
	Key   string
	Op    Operator
	Value any
}

func (Filter) isPredicate() {}

// And is a Predicate that joins multiple predicates with AND.
type And struct {
	Predicates []Predicate
}

func (And) isPredicate() {}

// Or is a Predicate that joins multiple predicates with OR.
type Or struct {
	Predicates []Predicate
}

func (Or) isPredicate() {}

// Helper functions to make building queries more ergonomic.

// AndPredicates combines predicates with a logical AND.
func AndPredicates(preds ...Predicate) And {
	return And{Predicates: preds}
}

// OrPredicates combines predicates with a logical OR.
func OrPredicates(preds ...Predicate) Or {
	return Or{Predicates: preds}
}

// buildWhereClause recursively walks the predicate tree to build the SQL query.
func buildWhereClause(p Predicate, validKeys map[string]struct{}, keyFieldName string) (string, []any, error) {
	switch v := p.(type) {
	case Filter:
		// Handle IN and NOT IN operators
		if v.Op == OpIn || v.Op == OpNotIn {
			// Extract values from any slice type using reflection
			var values []any

			// Check if v.Value is a slice or array using reflection
			rv := reflect.ValueOf(v.Value)
			if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
				return "", nil, fmt.Errorf("%s operator requires a slice value", v.Op)
			}

			// Handle nil slices as an error
			if rv.Kind() == reflect.Slice && rv.IsNil() {
				return "", nil, fmt.Errorf("%s predicate values cannot be nil", v.Op)
			}

			// Convert slice elements to []any
			sliceLen := rv.Len()
			values = make([]any, sliceLen)
			for i := 0; i < sliceLen; i++ {
				values[i] = rv.Index(i).Interface()
			}

			// Empty values slice returns an impossible condition (no results for IN, all results for NOT IN)
			if len(values) == 0 {
				if v.Op == OpIn {
					return "1 = 0", nil, nil
				} else {
					return "1 = 1", nil, nil
				}
			}

			// Build placeholders: "?, ?, ?"
			placeholders := make([]string, len(values))
			for i := range values {
				placeholders[i] = "?"
			}
			inClause := strings.Join(placeholders, ", ")

			// Check if this is a query on the primary key field
			if keyFieldName != "" && v.Key == keyFieldName {
				sql := fmt.Sprintf("key %s (%s)", v.Op, inClause)
				return sql, values, nil
			}

			// Validate top-level keys (skip nested keys)
			if !strings.Contains(v.Key, ".") {
				if _, ok := validKeys[v.Key]; !ok {
					return "", nil, fmt.Errorf("invalid %s key: '%s' is not a valid key for this entity", v.Op, v.Key)
				}
			}

			// JSON field extraction with IN clause
			sql := fmt.Sprintf("json_extract(json, ?) %s (%s)", v.Op, inClause)
			args := []any{"$." + v.Key}
			args = append(args, values...)
			return sql, args, nil
		}

		// Handle regular comparison operators
		switch v.Op {
		case OpEq, OpNEq, OpGT, OpGTE, OpLT, OpLTE:
			// Valid operator
		default:
			return "", nil, fmt.Errorf("unsupported query operator: %s", v.Op)
		}

		// Check if this is a query on the primary key field
		if keyFieldName != "" && v.Key == keyFieldName {
			sql := fmt.Sprintf("key %s ?", v.Op)
			return sql, []any{v.Value}, nil
		}

		// Only validate top-level keys. Nested keys (e.g. 'a.b') are not validated.
		if !strings.Contains(v.Key, ".") {
			if _, ok := validKeys[v.Key]; !ok {
				return "", nil, fmt.Errorf("invalid filter key: '%s' is not a valid key for this entity", v.Key)
			}
		}

		sql := fmt.Sprintf("json_extract(json, ?) %s ?", v.Op)
		args := []any{"$." + v.Key, v.Value}
		return sql, args, nil

	case And:
		return joinPredicates(v.Predicates, "AND", validKeys, keyFieldName)

	case Or:
		return joinPredicates(v.Predicates, "OR", validKeys, keyFieldName)

	default:
		return "", nil, fmt.Errorf("unknown predicate type: %T", p)
	}
}

func joinPredicates(preds []Predicate, joiner string, validKeys map[string]struct{}, keyFieldName string) (string, []any, error) {
	if len(preds) == 0 {
		return "", nil, nil
	}

	var clauses []string
	var allArgs []any

	for _, pred := range preds {
		clause, args, err := buildWhereClause(pred, validKeys, keyFieldName)
		if err != nil {
			return "", nil, err
		}
		clauses = append(clauses, clause)
		allArgs = append(allArgs, args...)
	}

	return fmt.Sprintf("(%s)", strings.Join(clauses, ") "+joiner+" (")), allArgs, nil
}
