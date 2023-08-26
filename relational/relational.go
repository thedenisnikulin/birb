package relational

import (
	"birb/codec"
	"birb/storage"
)

type RelationalStore struct {
	storage storage.Storage[[]byte]
}

func Use[R any](relStore *RelationalStore, codec codec.Codec[R], ns string) (*NamedStore[R], error) {
	return NewNamedStore(ns, relStore.storage, codec)
}
