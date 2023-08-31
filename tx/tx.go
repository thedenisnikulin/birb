/*
// TODO CHANGE THIS!
# Data layout patterns.

	[key] = [value]	- key for a value
	[key] = [*key]	- key for a ref to other key

# Transaction data layout.

Suppose we have "users" namespace:

	[ptr_users_pk_1] = [*rec_users_pk_1_com_123_000]
	[rec_users_pk_1_com_123_000] = [record value]

record key "rec_users_pk_1_com_123_000" is a current version of the record,
and the latest committed version. "ptr_users_pk_1" is a pointer key to
a record key.

# Transaction key pattern.

	{key type}_{ns}_{idx field name}_{idx field val}_{tx state}_{xmin val}_{xmax val}

where

  - {key type} is {rec|ptr|idx} (record|pointer|index)
  - {tx state} is {unc|com} (uncommitted|committed),
  - {xmin val} and {xmax val} are ordered 64-bit integers, see [txid.ID].

Example key:

	rec_users_pk_12_com_1234567890_9876543210

which means "an *committed* record of *users* namespace which can be found by
*primary key* *12* and valid for all transactions started after xmin 1234567890
and before xmax 9876543210".

If during a transaction with txid X a record is inserted, the key for that
record will look like this:

	rec_users_pk_12_unc_X_X

notice equality of xmin and xmax values. This makes the record visible for
current transaction (xmin and xmax are less than current tx's txid, with "less"
defined as [txid.ID.Less]), but invisible to other transactions.

The idea of such transaction model is stolen from PostgreSQL :P
*/
package tx

import (
	"birb"
	"birb/bvalue"
	"birb/codec"
	"birb/internal"
	"birb/key"
	"birb/storage"
	"birb/txid"

	"github.com/samber/mo"
)

var (
	_ birb.Store[any] = (*TxStore[any])(nil)
	_ birb.Tx         = (*TxStore[any])(nil)
)

type TxStore[R any] struct {
	ns      string
	id      txid.ID
	storage storage.Storage[[]byte]
	codec   codec.Codec[R]
}

func NewTx[R any](ns string, stg storage.Storage[[]byte], codec codec.Codec[R], id txid.ID) TxStore[R] {
	return TxStore[R]{
		ns:      ns,
		id:      id,
		storage: stg,
		codec:   codec,
	}
}

// Finds a record only that which was created before tx started
func (tx *TxStore[R]) Find(pk bvalue.Value) (R, bool) {
	unckey := key.Record(tx.ns, "pk", pk, "unc", tx.id, mo.None[txid.ID]())
	// try to find uncommitted record made by current tx
	if rec, ok := internal.Find(tx.storage, tx.codec, unckey.String()); ok {
		return rec, true
	}

	// otherwise try to find committed latest version of the record
	_, rec, ok := internal.FindCommitedLatestVersion(tx.storage, tx.codec, pk, tx.id, tx.ns)
	return rec, ok
}

func (*TxStore[R]) FindByIndex(name string, value bvalue.Value) (R, bool) {
	panic("unimplemented")
}

// XXX GOLD https://devcenter.heroku.com/articles/postgresql-concurrency
func (tx *TxStore[R]) Upsert(pk bvalue.Value, record R) {
	key := key.Record(tx.ns, "pk", pk, "unc", tx.id, mo.None[txid.ID]())
	recb, _ := tx.codec.Encode(record)
	tx.storage.Set(key.String(), recb)
}

func (tx *TxStore[R]) Delete(pk bvalue.Value) {
	// TODO :
	// 1. set xmax=tx.id for the record
	// 2. what to do for xmin==tx.id==xmax?

	// TODO a
	// change idea of key pointer to just a commited row?
	// if deletion is on 'com' row, then create new 'unc' row with xmax=tx.id
	// if deletion is on 'unc' row, just mark it with xmax=tx.id

	unckey := key.Record(tx.ns, "pk", pk, "unc", tx.id, mo.None[txid.ID]())
	// if we are deleting uncommited record, just set its xmax == tx.id
	if recb, ok := tx.storage.Get(unckey.String()); ok {
		tx.storage.Del(unckey.String())
		unckey.Xmax = tx.id
		tx.storage.Set(unckey.String(), recb)
		return
	}

	// otherwise, make an unc copy of a committed record & mark xmax=tx.id
	// TODO optimize double encoding-decoding
	key, rec, ok := internal.FindCommitedLatestVersion(
		tx.storage, tx.codec, pk, tx.id, tx.ns)
	if ok {
		key.Xmax = tx.id
		recb, _ := tx.codec.Encode(rec)
		tx.storage.Set(unckey.String(), recb)
	}
}

func (tx *TxStore[R]) Commit() error {
	// TODO :
	// 1. commit all rec_unc records with xmin == tx.id (??? && xmax == tx.id ???)
	// save all 'unc' tx with 'txstate' in key being at the beginning (rec_unc_users_...)?
	// mb 'rec_unc_{xmin}_users_pk_1_{xmax}'
	// and 'rec_com_users_pk_1_{xmin}_{xmax}'
	panic("not implemented")
}

func (*TxStore[R]) Rollback() {
	// TODO :
	// 1. delete all rec_unc with xmin == tx.id && xmax == tx.id
	panic("not implemented")
}
