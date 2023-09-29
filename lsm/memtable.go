package lsm

import (
	"github.com/zhangyunhao116/skipmap"
)

func NewMemtable() *Memtable {
	return &Memtable{skipmap.NewString[[]byte](), 0}
}

type Memtable struct {
	skiplist   *skipmap.StringMap[[]byte]
	approxSize int
}

func (m *Memtable) Get(k []byte) ([]byte, error) {
	v, ok := m.skiplist.Load(string(k))
	if !ok {
		return nil, ErrKeyNotFound
	}

	return v, nil
}

func (m *Memtable) Put(k, v []byte) error {
	m.skiplist.Store(string(k), v)
	m.approxSize += len(v)
	return nil
}

func (m *Memtable) Size() int {
	return m.approxSize
}

func (m *Memtable) Clone() *Memtable {
	clone := skipmap.NewString[[]byte]()
	m.skiplist.Range(func(k string, v []byte) bool {
		clone.Store(k, v)
		return true
	})
	size := m.approxSize

	return &Memtable{clone, size}
}

func (m *Memtable) AsReadonly() ReadonlyMemtable {
	return ReadonlyMemtable{*m}
}

type ReadonlyMemtable struct {
	table Memtable
}

func (m ReadonlyMemtable) ToSSTable() (SSTable, error) {
	return SSTableFromReadonlyMemtable(m)
}
