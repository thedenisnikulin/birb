package relational

import (
	"birb"
	bval "birb/bvalue"
	"birb/codec"
	"birb/internal"
	"birb/storage"
	"errors"
	"fmt"
	"strconv"
)

var (
	_ birb.Store[any] = (*NamedStore[any])(nil)
	_ birb.Indexer    = (*NamedStore[any])(nil)
)

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

// TODO add to index as well
func (s *NamedStore[R]) Upsert(pk bval.Value, record R) {
	key := internal.Key(s.name, internal.PrimaryKeyTag, pk)
	recb, _ := s.codec.Encode(record)
	s.storage.Set(key, recb)
}

func (s *NamedStore[R]) Delete(pk bval.Value) {
	key := internal.Key(s.name, internal.PrimaryKeyTag, pk)
	s.storage.Del(key)
}

func (s *NamedStore[R]) Find(pk bval.Value) (R, bool) {
	key := internal.Key(s.name, internal.PrimaryKeyTag, pk)
	return internal.Find(s.storage, s.codec, key)
}

func (s *NamedStore[R]) FindByIndex(name string, value bval.Value) (R, bool) {
	idxKey := internal.IndexKey(s.name, name, value)
	recordKey, ok := s.storage.Get(idxKey)
	if !ok {
		var r R
		return r, false
	}

	return internal.Find(s.storage, s.codec, string(recordKey))
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

		field, ok := internal.FieldValueByTag(rec, s.codec.Tag(), fieldName)
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
		indexKey := internal.IndexKey(s.name, fieldName, []byte(value))
		s.storage.Set(indexKey, bval.Value(key))
	}

	return nil
}

// TODO import cycle :(
// func (s *NamedStore[R]) Tx(run func(tx *tx.TxStore[R])) {
// }
