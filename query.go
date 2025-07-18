package litestore

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
