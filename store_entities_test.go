package litestore_test

// TestPersonNoKey does not have a key field.
type TestPersonNoKey struct {
	Info string `json:"info"`
	Data int    `json:"data"`
}

// TestPersonWithKey has a key field.
type TestPersonWithKey struct {
	K string `json:"k" litestore:"key"`
	// Key looks like litestore key, but it's not tagged as such, so should be treated as a regular field
	Key string `json:"key"`
	// ID looks like some special field, but it's not tagged as litestore key, so should be treated as a regular field
	ID       string `json:"id"`
	Name     string `json:"name"`
	Category string `json:"category"`
	IsActive bool   `json:"is_active"`
	Value    int    `json:"value"`
}
