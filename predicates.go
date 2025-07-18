package litestore

// Predicate represents a part of a query's WHERE clause.
// It's a "closed" interface, meaning only types within this package can implement it.
type Predicate interface {
	isPredicate()
}

// Operator defines the comparison operator for a query filter.
type Operator string

// Supported query operators.
const (
	OpEq  Operator = "="
	OpNEq Operator = "!="
	OpGT  Operator = ">"
	OpGTE Operator = ">="
	OpLT  Operator = "<"
	OpLTE Operator = "<="
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

// CustomPredicate allows for raw SQL clauses in a query.
// Use with caution, as it can be a source of SQL injection if not used with parameterized queries.
type CustomPredicate struct {
	Clause string
	Args   []any
}

func (CustomPredicate) isPredicate() {}

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

// Helper functions to make building queries more ergonomic.

// AndPredicates combines predicates with a logical AND.
func AndPredicates(preds ...Predicate) And {
	return And{Predicates: preds}
}

// OrPredicates combines predicates with a logical OR.
func OrPredicates(preds ...Predicate) Or {
	return Or{Predicates: preds}
}
