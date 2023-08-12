package relational

import (
	"main/internal/storage"
)

type Record []byte

type RelationalStore struct {
	storage storage.Storage[Record]
	codec   Codec[map[string]any]
}

func (s *RelationalStore) Use(ns string) *NamedStore {
	return &NamedStore{ns, s.storage, s.codec}
}
