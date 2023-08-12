package storage

type Storage[V any] interface {
	Get(string) (V, bool)
	Set(string, V)
	Range(prefix string) Range[string, V]
	ToMap() map[string]V
}

type Range[K comparable, V any] interface {
	Next() bool
	Value() (K, V)
}
