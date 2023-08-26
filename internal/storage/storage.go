package storage

// Key value storage with support of iteration by key prefix.
// Get and set operations are meant to be atomic.
type Storage[V any] interface {
	Get(string) (V, bool)
	Set(string, V)
	Del(string)
	Range(prefix string) Range[string, V]
	ToMap() map[string]V
}

type Range[K comparable, V any] interface {
	Next() bool
	Value() (K, V)
}
