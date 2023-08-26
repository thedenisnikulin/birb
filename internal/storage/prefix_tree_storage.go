package storage

import (
	"github.com/s0rg/trie"
)

func NewPrefixTreeStorage[V any]() *prefixTreeStorage[V] {
	return &prefixTreeStorage[V]{trie.New[V]()}
}

type prefixTreeStorage[V any] struct {
	inner *trie.Trie[V]
}

func (s *prefixTreeStorage[V]) Get(key string) (V, bool) {
	return s.inner.Find(key)
}

func (s *prefixTreeStorage[V]) Set(key string, value V) {
	s.inner.Add(key, value)
}

func (s *prefixTreeStorage[V]) Del(key string) {
	s.inner.Del(key)
}

func (s *prefixTreeStorage[V]) Range(prefix string) Range[string, V] {
	keys, _ := s.inner.Suggest(prefix)
	return &prefixTreeRange[V]{keys, 0, s}
}

func (s *prefixTreeStorage[V]) ToMap() map[string]V {
	keys, _ := s.inner.Suggest("")
	out := make(map[string]V)
	for _, k := range keys {
		v, _ := s.inner.Find(k)
		out[k] = v
	}

	return out
}

type prefixTreeRange[V any] struct {
	keys       []string
	curr       int
	storageRef *prefixTreeStorage[V]
}

func (r *prefixTreeRange[V]) Value() (string, V) {
	key := r.keys[r.curr]
	r.curr++

	// SAFETY: the key is from r.keys array which
	// the inner storage gave us, we can assume this key exists
	value, _ := r.storageRef.Get(key)
	return key, value
}

func (r *prefixTreeRange[V]) Next() bool {
	return r.curr < len(r.keys)
}
