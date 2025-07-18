package litestore

import (
	"fmt"
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
	// a nested JSON path (e.g., 'user.name'), or the special value 'key' for the primary key.
	Key       string
	Direction OrderDirection
}

// build constructs the SQL query string and arguments.
// It assumes q is not nil.
func (q *Query) build(tableName string) (string, []any, error) {
	var queryBuilder strings.Builder
	args := []any{}

	queryBuilder.WriteString(fmt.Sprintf("SELECT key, json FROM %s", tableName))

	if q.Predicate != nil {
		whereClause, whereArgs, err := buildWhereClause(q.Predicate)
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
			// We can't use a parameter for the column name in ORDER BY.
			// The key column is safe. JSON paths are also safe when used with json_extract.
			if o.Key == "key" {
				orderClauses = append(orderClauses, fmt.Sprintf("key %s", o.Direction))
			} else {
				if strings.ContainsAny(o.Key, ";)") {
					return "", nil, fmt.Errorf("invalid character in order by key: %s", o.Key)
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

// buildWhereClause recursively walks the predicate tree to build the SQL query.
func buildWhereClause(p Predicate) (string, []any, error) {
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

	case CustomPredicate:
		return v.Clause, v.Args, nil

	case And:
		return joinPredicates(v.Predicates, "AND")

	case Or:
		return joinPredicates(v.Predicates, "OR")

	default:
		return "", nil, fmt.Errorf("unknown predicate type: %T", p)
	}
}

func joinPredicates(preds []Predicate, joiner string) (string, []any, error) {
	if len(preds) == 0 {
		return "", nil, nil
	}

	var clauses []string
	var allArgs []any

	for _, pred := range preds {
		clause, args, err := buildWhereClause(pred)
		if err != nil {
			return "", nil, err
		}
		clauses = append(clauses, clause)
		allArgs = append(allArgs, args...)
	}

	return fmt.Sprintf("(%s)", strings.Join(clauses, ") "+joiner+" (")), allArgs, nil
}
