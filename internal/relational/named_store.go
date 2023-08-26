package relational

import (
	"errors"
	"fmt"
	"main/internal/storage"
	"main/pkg/bvalue"
	"main/pkg/codec"
	"reflect"
	"strconv"
)

const (
	PKKey string = "pk"
)

var _ Store[any] = (*NamedStore[any])(nil)
var _ Indexer = (*NamedStore[any])(nil)

// TODO each function should use transaction
// since e.g. every single 'upsert' is a smol tx
type NamedStore[R any] struct {
	name    string
	storage storage.Storage[[]byte]
	codec   codec.Codec[R]
}

func NewNamedStore[R any](
	ns string,
	storage storage.Storage[[]byte],
	codec codec.Codec[R],
) (*NamedStore[R], error) {
	var val R
	if _, err := codec.Encode(val); err != nil {
		return nil, fmt.Errorf("cannot create NamedStore since the record type is not serializable: %w", err)
	}
	return &NamedStore[R]{ns, storage, codec}, nil
}

func (s *NamedStore[R]) Upsert(pk bvalue.Value, record R) {
	key := keyFrom(s.name, PKKey, pk)
	recb, _ := s.codec.Encode(record)
	s.storage.Set(key, recb)
}

func (s *NamedStore[R]) Delete(pk bvalue.Value) {
	key := keyFrom(s.name, PKKey, pk)
	s.storage.Del(key)
}

func (s *NamedStore[R]) Find(pk bvalue.Value) (R, bool) {
	key := keyFrom(s.name, PKKey, pk)
	return find(s.storage, s.codec, key)
}

func (s *NamedStore[R]) FindByIndex(name string, value bvalue.Value) (R, bool) {
	idxKey := keyFrom(s.name, name, value)
	recordKey, ok := s.storage.Get(idxKey)
	if !ok {
		var r R
		return r, false
	}

	return find(s.storage, s.codec, string(recordKey))
}

func (s *NamedStore[R]) AddIndex(fieldName string) error {
	rng := s.storage.Range(s.name)
	for rng.Next() {
		key, recb := rng.Value()

		// decode record into comprehensible type and find field's value
		rec, err := s.codec.Decode(recb)
		if err != nil {
			panic("decoding record when adding index: " + err.Error())
		}

		field, ok := fieldValueByTag(rec, s.codec.Tag(), fieldName)
		if !ok {
			return errors.New("cannot add index for non-existing field")
		}

		// check index field type: only allow int and string
		var value string
		switch v := field.Interface().(type) {
		case int:
			value = strconv.Itoa(v)
		case string:
			value = v
		default:
			panic("indices are only supported for types int and string")
		}

		// create index: index is basically "a pointer" to the PK key
		indexKey := keyFrom(s.name, fieldName, []byte(value))
		s.storage.Set(indexKey, []byte(key))
	}

	return nil
}

func (s *NamedStore[R]) Tx(run func(tx *TxStore[R])) {
}

func fieldValueByTag(v any, tag, tagValue string) (reflect.Value, bool) {
	stype := reflect.TypeOf(v)
	sval := reflect.ValueOf(v)
	for i := 0; i < stype.NumField(); i++ {
		f := stype.Field(i)
		val, ok := f.Tag.Lookup(tag)
		if ok && val == tagValue {
			return sval.FieldByName(f.Name), true
		}
	}

	return reflect.Value{}, false
}

func find[R any](storage storage.Storage[[]byte], codec codec.Codec[R], key string) (R, bool) {
	recb, ok := storage.Get(key)
	if !ok {
		var r R
		return r, false
	}

	rec, _ := codec.Decode(recb)
	return rec, true
}

func keyFrom(ns string, field string, value []byte) string {
	return prefixFrom(ns, field) + string(value)
}

func prefixFrom(ns, field string) string {
	return ns + "_" + field + "_"
}
