package relational

import (
	"main/internal/storage"
	"main/pkg/codec"
)

type RelationalStore struct {
	storage storage.Storage[[]byte]
}

func Use[R any](relStore *RelationalStore, codec codec.Codec[R], ns string) (*NamedStore[R], error) {
	return NewNamedStore[R](ns, relStore.storage, codec)
}
