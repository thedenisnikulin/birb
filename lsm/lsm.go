// i mean... can anything out there explain things happening here better than
// rocksdb official docs?
// https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
package lsm

import (
	"bytes"
	"errors"
	"slices"
	"sync"
)

// threshold thing is mostly simplified
type Options struct {
	MemtableThreshold       int // memtable thld == memro table thld == lvl0 sstable thld
	L1TableThreshold        int // Nth level thld (where N>1) is calculated as (N-1)*NonL0ThresholdMultipler
	NonL0ThresholdMultipler int

	MaxMemroTables      int
	MaxL0Tables         int
	MaxL1Tables         int // Nth level len (where N>1) is calculated as (N-1)+MaxNonL0TablesAdder
	MaxNonL0TablesAdder int
}

var DefaultOptions = Options{
	MemtableThreshold:       1 << 20,
	L1TableThreshold:        10 << 20,
	NonL0ThresholdMultipler: 10,

	MaxMemroTables: 2,
	MaxL0Tables:    2,
	MaxL1Tables:    3,
}

type LSMTree struct {
	mem         *Memtable
	wal         *Wal
	rodataGuard *sync.RWMutex // wlocks memro, lvl0, and lvln during compaction
	memro       []*ReadonlyMemtable
	lvl0        []*SSTable
	lvln        [][]*SSTable
	compact     *Compactor
	opt         Options
}

func (tree *LSMTree) Get(k []byte) ([]byte, error) {
	tree.rodataGuard.RLock()
	defer tree.rodataGuard.RUnlock()

	// try find in memtable
	val, err := tree.mem.Get(k)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}

	if err == nil {
		return val, nil
	}

	// try find in readonly memtables
	for _, r := range tree.memro {
		val, err := r.Get(k)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}

		if err == nil {
			return val, nil
		}
	}

	// try find in level 0 sstables
	// level 0 sstables are not sorted by keys so need an O(n) lookup
	for _, sst := range tree.lvl0 {
		val, err := sst.Get(k)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}

		if err == nil {
			return val, nil
		}
	}

	// try find in sstables
	var (
		sst   *SSTable
		found bool
	)
	for _, level := range tree.lvln {
		i, found := slices.BinarySearchFunc(level, k, func(e *SSTable, t []byte) int {
			firstKey := e.index[0].firstKey
			lastKey := e.index[len(e.index)-1].lastKey

			moreThanFirst := bytes.Compare(t, firstKey) > 0
			lessThanLast := bytes.Compare(t, lastKey) < 0

			if !moreThanFirst {
				return -1
			}

			if !lessThanLast {
				return 1
			}

			return 0
		})

		if found {
			sst = level[i]
			found = true
			break
		}
	}

	if !found {
		return nil, ErrKeyNotFound
	}

	return sst.Get(k)
}

func (tree *LSMTree) Put(k, v []byte) error {
	tree.rodataGuard.RLock()
	defer tree.rodataGuard.RUnlock()

	if tree.mem.Size() < tree.opt.MemtableThreshold {
		// best case: just write to memtable.
		// most callers will end up here which is ✨fast✨
		return tree.mem.Put(k, v)
	}

	// worst case: we block here if compaction is still in progress.
	// it means that memtable and memro tables are full
	tree.compact.Waitc() <- struct{}{} /// TODO rename Compactor to PartialCompactor?

	tree.rodataGuard.Lock()
	if len(tree.memro) < tree.opt.MaxMemroTables {
		// ok case: memtable is full but memro tables (readonly memtables)
		// are not, just dump memtable as memro table
		ro := tree.mem.Clone().AsReadonly()
		tree.memro = append(tree.memro, &ro)
		tree.mem = NewMemtable()

		// if we reached max memro limit, trigger the compaction right away so
		// we win time until worst case happens
		if len(tree.memro) == tree.opt.MaxMemroTables {
			tree.compact.Triggerc() <- struct{}{}
		}
	}
	tree.rodataGuard.Unlock()

	return tree.mem.Put(k, v)
}

func (t *LSMTree) Del(k []byte) error {
	panic("unimpl")
}

func (t *LSMTree) Close() error {
	// TODO basically just close all files
	panic("unimpl")
}

func Recover(dir string) *LSMTree {
	panic("unimpl")
}
