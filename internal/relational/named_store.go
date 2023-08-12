package relational

import (
	"main/internal/storage"
	"strconv"
)

const (
	PKKey string = "pk"
)

type NamedStore struct {
	name    string
	storage storage.Storage[Record]
	codec   Codec[map[string]any]
}

func (s *NamedStore) Insert(pk []byte, record Record) {
	key := keyFrom(s.name, PKKey, pk)
	s.storage.Set(key, record)
}

func (s *NamedStore) Find(pk []byte) (Record, bool) {
	key := keyFrom(s.name, PKKey, pk)
	return s.storage.Get(key)
}

func (s *NamedStore) FindByIndex(field Field) (Record, bool) {
	idxKey := keyFrom(s.name, field.name, field.value)
	recordKey, ok := s.storage.Get(idxKey)
	if !ok {
		return Record{}, false
	}

	return s.storage.Get(string(recordKey))
}

func (s *NamedStore) AddIndex(fieldName string) {
	rng := s.storage.Range(s.name)
	for rng.Next() {
		key, rec := rng.Value()

		// decode Record into comprehensible type
		recMap, err := s.codec.Decode(rec)
		if err != nil {
			panic("decoding record when adding index: " + err.Error())
		}

		// check index field type: only allow int and string
		var value string
		switch v := recMap[fieldName].(type) {
		case int:
			value = strconv.Itoa(v)
		case string:
			value = v
		default:
			panic("indices are only supported for types int and string")
		}

		// create index: index is basically "a pointer" to the PK key
		indexKey := keyFrom(s.name, fieldName, []byte(value))
		s.storage.Set(indexKey, Record(key))
	}
}

func keyFrom(ns string, field string, value []byte) string {
	return prefixFrom(ns, field) + string(value)
}

func prefixFrom(ns, field string) string {
	return ns + "_" + field + "_"
}

type Field struct {
	name  string
	value []byte
}
