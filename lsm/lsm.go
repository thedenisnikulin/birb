// i mean... can anything out there explain things happening here better than
// rocksdb official docs?
// https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
// TODO put big comment explaining Put and Get and how they're different and how they work (and other stuff related to lsm tree)
package lsm

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"log/slog"
)

// TODO: left to implement:
// 1. compaction
// 2. deletion
// 3. wal
// 4. iterator
// 5. add logging with slog (log to file and stdout)

// threshold thing is mostly simplified
type Options struct {
	MemtableThreshold    int // memtable thld == memro table thld == lvl0 sstable thld
	L1Threshold          int // Nth level thld (where N>1) is calculated as (N-1)*NonL0ThresholdMultipler
	LNThresholdMultipler int

	BlockThreshold int

	MaxMemroTables   int
	MaxL0Tables      int
	MaxL1Tables      int // Nth level len (where N>1) is calculated as (N-1)+MaxNonL0TablesAdder
	MaxLNTablesAdder int
}

var DefaultOptions = &Options{
	MemtableThreshold:    1 << 20,
	L1Threshold:          10 << 20,
	LNThresholdMultipler: 10,

	BlockThreshold: 1 << 6,

	MaxMemroTables:   2,
	MaxL0Tables:      2,
	MaxL1Tables:      3,
	MaxLNTablesAdder: 2,
}

type LSMTree struct {
	mem         *Memtable // TODO: put wal into Memtable struct?
	wal         *Wal
	rodataGuard *sync.RWMutex // wlocks memro, lvl0, and lvln during compaction
	memro       []*ReadonlyMemtable
	lvl0        []*SSTable
	lvln        [][]*SSTable
	compact     CompactorHandle
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
		i, found := slices.BinarySearchFunc(level, k, func(t *SSTable, k []byte) int {
			moreThanFirst := bytes.Compare(k, t.FirstKey()) > 0
			lessThanLast := bytes.Compare(k, t.LastKey()) < 0

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

	if tree.mem.Size() < tree.opt.MemtableThreshold {
		slog.Debug("putting into memtable")
		// best case: just write to memtable.
		// most callers will end up here which is ✨blazingly fast✨
		tree.rodataGuard.RUnlock()
		return tree.mem.Put(k, v)
	}

	tree.rodataGuard.RUnlock()

	slog.Debug("waiting on compaction")
	// worst case: we block here if compaction is still in progress.
	// it means that memtable and memro tables are full
	tree.compact.Waitc() <- struct{}{} /// TODO rename Compactor to PartialCompactor?

	tree.rodataGuard.Lock()
	if len(tree.memro) < tree.opt.MaxMemroTables {
		slog.Debug("dumping memtable as readonly")
		// ok case: memtable is full but memro tables (readonly memtables)
		// are not, just dump memtable to memro tables
		ro := tree.mem.Clone().AsReadonly() // TODO do i need clone here?
		tree.memro = append(tree.memro, &ro)
		tree.mem = NewMemtable()

		// if we reached max memro limit, trigger the compaction right away so
		// we win time until worst case happens
		if len(tree.memro) == tree.opt.MaxMemroTables {
			slog.Debug("readonly memtable limit reached, triggering compaction")
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

func Recover(ctx context.Context, dir string, opts *Options) (*LSMTree, error) {
	if opts == nil {
		opts = DefaultOptions
	}

	absdir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	// first read manifest file and get paths of wal and sst levels
	f, err := os.Open(path.Join(dir, "MANIFEST"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if errors.Is(err, os.ErrNotExist) {
		slog.Debug("MANIFEST not foudn, creating at " +
			path.Join(absdir, "WAL"))

		if err := os.Mkdir(path.Join(absdir), 0777); err != nil {
			return nil, fmt.Errorf("making dir: %w", err)
		}

		wal, err := os.Create(path.Join(absdir, "WAL"))
		if err != nil {
			return nil, fmt.Errorf("creating wal: %w", err)
		}

		defer wal.Close()

		f, err = os.Create(path.Join(absdir, "MANIFEST"))
		if err != nil {
			return nil, fmt.Errorf("creating manifest: %w", err)
		}

		_, err = fmt.Fprintln(f, path.Join(absdir, "WAL"))
		if err != nil {
			return nil, err
		}
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	if !scanner.Scan() {
		return nil, errors.New("bad manifest")
	}

	walPath := scanner.Text()
	levelPaths := make([][]string, 0)
	for scanner.Scan() {
		line := scanner.Text()
		tablePaths := strings.Split(line, ",")
		levelPaths = append(levelPaths, tablePaths)
	}

	if scanner.Err() != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	// TODO implement wal
	walPath = walPath

	// second read all paths to load sstables
	lvls := make([][]*SSTable, 0, len(levelPaths))
	for _, paths := range levelPaths {
		lvl := make([]*SSTable, 0, len(paths))
		for _, p := range paths {
			f, err := os.OpenFile(p, os.O_RDWR, 0666)
			if err != nil {
				return nil, fmt.Errorf("opening %s: %w", p, err)
			}

			sst, err := SSTableFromFile(f)
			if err != nil {
				return nil, err
			}

			lvl = append(lvl, &sst)
			f.Close()
		}
		lvls = append(lvls, lvl)
	}

	lvl0 := make([]*SSTable, 0)
	if len(lvls) > 0 {
		lvl0 = lvls[0]
	}

	lvln := make([][]*SSTable, 0)
	if len(lvls) > 1 {
		lvln = lvls[1:]
	}

	// third initialize tree and compaction
	compactorHandle := CompactorHandle{
		triggerc: make(chan struct{}),
		waitc:    make(chan struct{}),
	}

	tree := &LSMTree{
		mem:         NewMemtable(),
		wal:         nil,
		rodataGuard: new(sync.RWMutex),
		memro:       make([]*ReadonlyMemtable, 0),
		lvl0:        lvl0,
		lvln:        lvln,
		compact:     compactorHandle,
		opt:         *opts,
	}

	compactor := Compactor{
		handle: compactorHandle,
		tree:   tree,
		opt:    *opts,
	}

	// fourth run compactor in bg and finish
	go compactor.Listen(ctx)

	return tree, nil
}
