package collection

import (
	bval "birb/bvalue"
	"birb/codec"
	"birb/internal"
	"birb/key"
	k "birb/key"
	"birb/storage"
	"birb/tx"
	"birb/txid"
	"errors"
	"fmt"
	"strconv"

	"github.com/samber/mo"
)

// FIXME import cycle :(
// var (
// 	_ birb.Store[any] = (*NamedStore[any])(nil)
// 	_ birb.Indexer    = (*NamedStore[any])(nil)
// )

// TODO paste this
// save all 'unc' tx with 'txstate' in key being at the beginning (rec_unc_users_...)?
// mb 'rec_unc_{xmin}_{xmax}_users_pk_1'
// and 'rec_com_users_pk_1_{xmin}_{xmax}'

type Store[R any] struct {
	name    string
	storage storage.Storage[[]byte]
	codec   codec.Codec[R]
	txidiss txid.Issuer
}

func New[R any](
	ns string,
	storage storage.Storage[[]byte],
	codec codec.Codec[R],
	txidIssuer txid.Issuer,
) (*Store[R], error) {
	var val R
	if _, err := codec.Encode(val); err != nil {
		return nil, fmt.Errorf("cannot create NamedStore since the record type is not serializable: %w", err)
	}
	return &Store[R]{ns, storage, codec, txidIssuer}, nil
}

// TODO add to index as well
func (s *Store[R]) Upsert(pk bval.Value, record R) {
	id := s.txidiss.Issue()
	key := key.CommittedRec(s.name, "pk", pk, id, mo.Some(txid.Max()))
	recb, _ := s.codec.Encode(record)
	s.storage.Set(key.String(), recb)
}

func (s *Store[R]) Delete(pk bval.Value) {
	id := s.txidiss.Issue()
	key, rec, ok := internal.FindLatestCommitted(
		s.storage, s.codec, "pk", pk, id, s.name)
	if ok {
		keyWithXmax := key
		keyWithXmax.Xmax = id
		recb, _ := s.codec.Encode(rec) // TODO optimize needless encoding
		s.storage.Set(keyWithXmax.String(), recb)
		s.storage.Del(key.String())
	}
}

func (s *Store[R]) Find(pk bval.Value) (R, bool) {
	id := s.txidiss.Issue()
	_, rec, ok := internal.FindLatestCommitted(
		s.storage, s.codec, "pk", pk, id, s.name)
	return rec, ok
}

// FIXME this method was abandoned, needs rework
func (s *Store[R]) FindByIndex(name string, value bval.Value) (R, bool) {
	id := s.txidiss.Issue()
	idxKey := key.CommittedRec(s.name, name, value, id, mo.None[txid.ID]())
	recordKey, ok := s.storage.Get(idxKey.String())
	if !ok {
		var r R
		return r, false
	}

	return internal.FindExact(s.storage, s.codec, string(recordKey))
}

// FIXME this method was abandoned, needs rework
func (s *Store[R]) AddIndex(fieldName string) error {
	rng := s.storage.Range("rec_" + s.name)
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
		indexKey := k.Index(s.name, fieldName, []byte(value), "", txid.ID{}, mo.None[txid.ID]())
		s.storage.Set(indexKey.String(), bval.Value(key))
	}

	return nil
}

// FIXME import cycle :( (f should be func(birb.Store[R]) (should it really?))
func (s *Store[R]) Tx(f func(tx tx.Store[R]) error) error {
	startId := s.txidiss.Issue()
	tx := tx.New(s.name, s.storage, s.codec, startId)

	err := f(tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	endId := s.txidiss.Issue()
	return tx.Commit(endId)
}
