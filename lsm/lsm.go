package lsm

import "sync"

// TODO ABOUT LEVEL 0 AND WHY LEVEL 0 IS NOT SORTED:
// The LSM Tree slices the stored data into a series of SSTables (Sorted String Table),
// where the data in an SSTable is an ordered arbitrary byte group (i.e., an arbitrary byte
// string, not a String as in programming languages). Once written to disk, the SSTable
// cannot be modified like a log (this is the origin of the word Log-Structured in the name of
// the Log-Structured Merge Tree). When modifying existing data, the LSM Tree does not
// modify the old data directly, but writes the new data directly to the new SSTable. Similarly,
// when deleting data, LSM Tree does not delete the old data directly, but writes a record
// with the deletion mark of the corresponding data to a new SSTable. In this way, LSM Tree
// writes data to disk with sequential block write operations and no random write operations.

type Options struct {
	MaxMemtableSize    int // memtable size == memro table size == lvl0 sstable size
	MaxLevel1TableSize int // Nth level size (where N>1) is calculated as (N-1)*10

	MaxMemroTables  int
	MaxLevel0Tables int
	MaxLevel1Tables int // Nth level len (where N>1) is calculated as (N-1)+1
}

var DefaultOptions = Options{
	MaxMemtableSize:    1 << 20,
	MaxLevel1TableSize: 10 << 20,

	MaxMemroTables:  2,
	MaxLevel0Tables: 2,
	MaxLevel1Tables: 3,
}

type LSMTree struct {
	mem         *Memtable
	memGuard    *sync.RWMutex // locks mem
	wal         *Wal
	rodataGuard *sync.RWMutex // locks memro, lvl0, and lvln during compaction
	memro       []*ReadonlyMemtable
	lvl0        []*SSTable
	lvln        [][]*SSTable
	compact     *Compactor
	cfg         Options
}

func (t *LSMTree) Get(k []byte) ([]byte, error) {
	panic("unimpl")
}

func (t *LSMTree) Put(k, v []byte) error {
	t.rodataGuard.RLock()
	defer t.rodataGuard.RUnlock()

	if t.mem.Size() < t.cfg.MaxMemtableSize {
		// best case: just write to memtable.
		// most callers will end up here which is ✨fast✨
		return t.mem.Put(k, v)
	}

	t.rodataGuard.Lock()

	// worst case: we block here if compaction is still in progress.
	// it means that memtable and memro tables are full
	t.compact.Waitc <- struct{}{}

	if len(t.memro) < t.cfg.MaxMemroTables {
		// ok case: memtable is full but memro tables (readonly memtables)
		// are not, just dump memtable as memro table
		ro := t.mem.Clone().AsReadonly()
		t.memro = append(t.memro, &ro)
		t.mem = NewMemtable()

		// if we reached max memro limit, trigger the compaction right away so
		// we win time until worst case happens
		if len(t.memro) == t.cfg.MaxMemroTables {
			t.compact.Runc <- struct{}{}
		}
	}
	t.rodataGuard.Unlock()

	return t.mem.Put(k, v)
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
