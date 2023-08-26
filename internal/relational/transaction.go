package relational

import (
	"main/internal/storage"
	"main/pkg/bvalue"
	"main/pkg/codec"
	"strconv"
	"strings"
	"time"
)

var _ Store[any] = (*TxStore[any])(nil)

// Data layout patterns
// [key] = [value]	- key for a value
// [key] = [*key]	- key for a ref to other key
//
// Transaction data layout
// suppose we have "users" namespace:
// [users_pk_1] = [*v_users_pk_1_xmin_1234567890]
// [v_users_pk_1_xmin_1234567890] = [value]
// v_users_pk_1_xmin_1234567890 is a current version of the record
type TxStore[R any] struct {
	ns      string
	startTs time.Time
	storage storage.Storage[[]byte]
	codec   codec.Codec[R]
}

func NewTx[R any](ns string, stg storage.Storage[[]byte], codec codec.Codec[R], now time.Time) TxStore[R] {
	return TxStore[R]{
		ns:      ns,
		startTs: now,
		storage: stg,
		codec:   codec,
	}
}

// Finds a record only that which was created before tx started
func (tx *TxStore[R]) Find(pk bvalue.Value) (R, bool) {
	baseKey := key(tx.ns, PKKey, pk)
	rng := tx.storage.Range(baseKey)
	var mostRecentTs int64
	var mostRecentKey string
	for rng.Next() {
		k, _ := rng.Value()
		tsStr, _ := strings.CutPrefix(k, baseKey+"_xmin_")
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			panic("incorrect storage key: must contain a valid timestamp")
		}

		if ts > mostRecentTs && ts <= tx.startTs.Unix() {
			mostRecentTs = ts
			mostRecentKey = k
		}
	}

	if mostRecentKey == "" {
		var r R
		return r, false
	}

	return find(tx.storage, tx.codec, mostRecentKey)
}

func (*TxStore[R]) FindByIndex(name string, value bvalue.Value) (R, bool) {
	panic("unimplemented")
}

// XXX GOLD https://devcenter.heroku.com/articles/postgresql-concurrency
func (*TxStore[R]) Upsert(pk bvalue.Value, record R) {
	panic("unimplemented")
}

func (*TxStore[R]) Delete(pk bvalue.Value) {
	panic("unimplemented")
}

func (tx *TxStore[R]) Commit() error {
	panic("not implemented")
}
func (tx *TxStore[R]) Rollback() {
	panic("not implemented")
}

type TxID struct {
	Id    uint32 // allowed to wrap
	Epoch uint32
}

func (id TxID) Uint64() uint64 {
	return uint64(id.Id<<32 + id.Epoch)
}

func TxIdFromUint64(n uint64) TxID {
	return TxID{Id: uint32(n >> 32), Epoch: uint32(n)}
}
