package birb

import (
	bval "birb/bvalue"
	"birb/txid"
)

type Store[R any] interface {
	Find(pk bval.Value) (R, bool)
	FindByIndex(name string, value bval.Value) (R, bool)
	Upsert(pk bval.Value, record R)
	Delete(pk bval.Value)
}

type Indexer interface {
	AddIndex(fieldName string) error
}

type Tx interface {
	Commit(end txid.ID) error
	Rollback()
}
