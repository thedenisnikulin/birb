package birb

import (
	"birb/codec"
	"birb/collection"
	"birb/storage"
	"birb/txid"
)

type Database struct {
	storage    storage.Storage[[]byte]
	txidIssuer txid.Issuer
}

func NewDatabase(stg storage.Storage[[]byte], txidiss txid.Issuer) Database {
	return Database{stg, txidiss}
}

func UseCollection[R any](db *Database, codec codec.Codec[R], ns string) (*collection.Store[R], error) {
	return collection.New(ns, db.storage, codec, db.txidIssuer)
}
