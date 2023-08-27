/*
# Data layout patterns.

	[key] = [value]	- key for a value
	[key] = [*key]	- key for a ref to other key

# Transaction data layout.

Suppose we have "users" namespace:

	[ptr_users_pk_1] = [*rec_users_pk_1_com_xmin_123]
	[ptr_users_pk_1_com_xmin_123] = [record value]

record key "rec_users_pk_1_com_xmin_123" is a current version of the record,
and the latest committed version. "ptr_users_pk_1" is a pointer key to
a record key.

# Transaction key pattern.

	{val type}_{ns}_{idx field name}_{idx field val}_{tx state}_xmin_{xmin val}_xmax_{xmax val}

where

  - {val type} is {rec|ptr|idx} (record|pointer|index)
  - {tx state} is {unc|com} (uncommitted|committed),
  - {xmin val} and {xmax val} are ordered 64-bit integers.

Example key:

	rec_users_pk_12_com_xmin_1234567890_xmax_9876543210

which means "an *committed* record of *users* namespace which can be found by
*primary key* *12* and valid for all transactions started after xmin 1234567890
and before xmax 9876543210".

If during a transaction with txid X a record is inserted, the key for that
record will look like this:

	rec_users_pk_12_unc_xmin_X_xmax_X

notice equality of xmin and xmax values. This makes the record visible for
current transaction (xmin and xmax are less than current tx's txid, with "less"
defined as [TxID.Less]), but invisible to other transactions.

The idea of such transaction model is stolen from PostgreSQL :P
*/
package tx

import (
	"birb"
	"birb/bvalue"
	"birb/codec"
	"birb/internal"
	"birb/storage"
	"strconv"
	"strings"
	"time"
)

var (
	_ birb.Store[any] = (*TxStore[any])(nil)
	_ birb.Tx         = (*TxStore[any])(nil)
)

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
	baseKey := internal.Key(tx.ns, internal.PrimaryKeyTag, pk)
	rng := tx.storage.Range(baseKey)
	var mostRecentTs int64
	var mostRecentKey string
	for rng.Next() {
		k, _ := rng.Value()
		tsRaw, _ := strings.CutPrefix(k, baseKey+"_xmin_")
		ts, err := strconv.ParseInt(tsRaw, 10, 64)
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

	return internal.Find(tx.storage, tx.codec, mostRecentKey)
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

func (*TxStore[R]) Commit() error {
	panic("not implemented")
}

func (*TxStore[R]) Rollback() {
	panic("not implemented")
}
