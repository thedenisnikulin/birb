package database

import (
	"birb/codec"
	"birb/storage"
	"birb/txid"
)

type Database struct {
	storage    storage.Storage[[]byte]
	txidIssuer *txid.MxIssuer
}

func Use[R any](db *Database, codec codec.Codec[R], ns string) (*NamedStore[R], error) {
	return NewNamedStore(ns, db.storage, codec, db.txidIssuer)
}
