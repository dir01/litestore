package litestore_test

// TestPersonNoKey does not have a key field.
type TestPersonNoKey struct {
	Info string `json:"info"`
	Data int    `json:"data"`
}

// TestPersonWithKey has a key field.
type TestPersonWithKey struct {
	ID       string `json:"id" litestore:"key"`
	Name     string `json:"name"`
	Category string `json:"category"`
	IsActive bool   `json:"is_active"`
	Value    int    `json:"value"`
}
