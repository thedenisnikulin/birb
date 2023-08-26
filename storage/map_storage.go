package storage

import "sync"

func NewSyncMapStorage[V any]() syncMapStorage[V] {
	return syncMapStorage[V]{sync.Map{}}
}

type syncMapStorage[V any] struct {
	inner sync.Map
}

func (s *syncMapStorage[V]) Get(key string) (V, bool) {
	v, ok := s.inner.Load(key)
	return v.(V), ok
}

func (s *syncMapStorage[V]) Set(key string, value V) {
	s.inner.Store(key, value)
}

func (s *syncMapStorage[V]) Del(key string) {
	s.inner.Delete(key)
}

func (*syncMapStorage[V]) Range(prefix string) (V, bool) {
	panic("unimplemented, requires a data structure more complex than a map")
}

func (s *syncMapStorage[V]) ToMap() map[string]V {
	out := make(map[string]V)
	s.inner.Range(func(key, value any) bool {
		out[key.(string)] = value.(V)
		return true
	})
	return out
}
