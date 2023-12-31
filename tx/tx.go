/*
// TODO CHANGE THIS, DEPRECATED!
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
PostgreSQL source code, /src/backend/utils/time/tqual.c:155, though
my implementation is a bit simplified.
*/
package tx

import (
	"birb/bvalue"
	"birb/codec"
	"birb/internal"
	"birb/key"
	"birb/storage"
	"birb/txid"

	"github.com/samber/mo"
)

// FIXME import cycle :(
// var (
// 	_ birb.Store[any] = (*TxStore[any])(nil)
// 	_ birb.Tx         = (*TxStore[any])(nil)
// )

// TODO change idea of key pointer to just a committed row?

// Isolation level is by default "read committed"
type Store[R any] struct {
	ns      string
	id      txid.ID
	storage storage.Storage[[]byte]
	codec   codec.Codec[R]
}

func New[R any](ns string, stg storage.Storage[[]byte], codec codec.Codec[R], id txid.ID) Store[R] {
	return Store[R]{
		ns:      ns,
		id:      id,
		storage: stg,
		codec:   codec,
	}
}

// Finds a record only that which was created before tx started
func (tx *Store[R]) Find(pk bvalue.Value) (R, bool) {
	// try to find uncommitted record made by current tx
	// TODO xmax is not necessarily mo.None?
	unckey := key.UncRec(tx.ns, "pk", pk, tx.id, mo.None[txid.ID]())
	if rec, ok := internal.FindExact(tx.storage, tx.codec, unckey.String()); ok {
		return rec, true
	}

	// otherwise try to find committed latest version of the record
	_, rec, ok := internal.FindLatestCommitted(
		tx.storage, tx.codec, "pk", pk, tx.id, tx.ns)
	return rec, ok
}

func (*Store[R]) FindByIndex(name string, value bvalue.Value) (R, bool) {
	panic("unimplemented")
}

// TODO INDICES!
// key=value: meta_users_indices = name,email
// more duty on Upsert and Delete (get fields from meta_users_indices and delete
// records all of indices by them).
// AddByIndex now adds meta_users_indices field

// TODO use sourcegraph cody?

// TODO future: add different indices support (like reindexer does)

// XXX GOLD https://devcenter.heroku.com/articles/postgresql-concurrency
func (tx *Store[R]) Upsert(pk bvalue.Value, record R) {
	key := key.UncRec(tx.ns, "pk", pk, tx.id, mo.Some(txid.Max()))
	recb, _ := tx.codec.Encode(record)
	tx.storage.Set(key.String(), recb)
}

func (tx *Store[R]) Delete(pk bvalue.Value) {
	// if we are deleting uncommitted record, just set its xmax == tx.id
	unckey := key.UncRec(tx.ns, "pk", pk, tx.id, mo.None[txid.ID]())
	if recb, ok := tx.storage.Get(unckey.String()); ok {
		tx.storage.Del(unckey.String())
		unckey.Xmin = tx.id
		unckey.Xmax = tx.id
		tx.storage.Set(unckey.String(), recb)
		return
	}

	// otherwise, make an unc copy of a committed record & mark xmax=tx.id
	// TODO optimize double encoding-decoding prolly
	comkey, rec, ok := internal.FindLatestCommitted(
		tx.storage, tx.codec, "pk", pk, tx.id, tx.ns)
	if ok {
		unckey := comkey.ToUnc()
		unckey.Xmin = tx.id
		unckey.Xmax = tx.id
		recb, _ := tx.codec.Encode(rec)
		tx.storage.Set(unckey.String(), recb)
	}
}

// TODO make concurrent
func (tx *Store[R]) Commit(end txid.ID) error {
	// commit records that were upserted during tx lifetime
	prefixUpserted := key.PrefixUncSameTx("rec", tx.ns, tx.id, mo.Some(txid.Max()))
	rng := tx.storage.Range(prefixUpserted)
	for rng.Next() {
		k, v := rng.Value()
		unckey, err := key.FromStringUnc(k)
		if err != nil {
			panic("converting storage key to key.UncKey: " + err.Error())
		}

		comkey := unckey.ToCom()
		comkey.Xmin = end
		tx.storage.Set(comkey.String(), v)
	}

	// commit records that were deleted during tx lifetime
	prefixDeleted := key.PrefixUncSameTx("rec", tx.ns, tx.id, mo.Some(tx.id))
	rng = tx.storage.Range(prefixDeleted)
	for rng.Next() {
		k, v := rng.Value()
		unckey, err := key.FromStringUnc(k)
		if err != nil {
			panic("converting storage key to key.UncKey: " + err.Error())
		}

		comkey := unckey.ToCom()
		comkey.Xmax = end
		tx.storage.Set(comkey.String(), v)
	}

	return nil
}

func (tx *Store[R]) Rollback() {
	prefixUpserted := key.PrefixUncSameTx("rec", tx.ns, tx.id, mo.None[txid.ID]())
	rng := tx.storage.Range(prefixUpserted)
	for rng.Next() {
		k, _ := rng.Value()
		tx.storage.Del(k)
	}

	prefixDeleted := key.PrefixUncSameTx("rec", tx.ns, tx.id, mo.Some(tx.id))
	rng = tx.storage.Range(prefixDeleted)
	for rng.Next() {
		k, _ := rng.Value()
		tx.storage.Del(k)
	}
}
