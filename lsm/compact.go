package lsm

import (
	"context"
	"log/slog"
	"strconv"
)

type CompactorHandle struct {
	triggerc chan struct{}
	waitc    chan struct{}
}

func (c *CompactorHandle) Triggerc() chan<- struct{} {
	return c.triggerc
}

func (c *CompactorHandle) Waitc() chan<- struct{} {
	return c.waitc
}

// TODO stopping/killing program while compacting will put it int inconsistent
// state, need to think of how to overcome it (or skip it for sake of simplicity?)
type Compactor struct {
	handle CompactorHandle
	tree   *LSMTree
	opt    Options
}

func (c *Compactor) Listen(ctx context.Context) {
	for {
		select {
		case <-c.handle.triggerc:
			slog.Debug("triggered!")
			// TODO can do only partial compaction synchronously, and finish the
			// rest of compaction asynchronously (e.g. memro and L0 are full,
			// we merge L0 and L1, put memro into L0, thus memro is
			// now free and if L1 needs to be merged into L2, we can do it
			// asynchronously), in order to release c.waitc faster
			c.compact()
		case <-c.handle.waitc:
			slog.Debug("go go go")
		case <-ctx.Done():
			return
		}
	}
}

func (c *Compactor) compact() error {
	// if L0 has sufficient space, just dump readonly memtables into it
	if len(c.tree.lvl0)+len(c.tree.memro) <= c.opt.MaxL0Tables {
		for _, r := range c.tree.memro {
			sst, err := SSTableFromReadonlyMemtable(
				*r,
				"SST_L0_"+strconv.Itoa(len(c.tree.lvl0)), // TODO filename
				c.opt,
			)
			if err != nil {
				return err
			}

			c.tree.lvl0 = append(c.tree.lvl0, &sst)
		}

		return nil
	}

	// otherwise, dump L0 into L1 first, then put readonly memtables to L0
	var (
		lvl1     = c.tree.lvln[0] // TODO: ensure no panic
		lvl1Size = uint(0)
		sst1Size = c.opt.L1Threshold / c.opt.MaxL1Tables
	)
	// since L0 isn't fully sorted (data is sorted only in individual
	// tables, key range of one L0 table may overlap with key range of other L0
	// table), we can't merge a L0 table into some exact 1-2 L1 tables (like we
	// do when merging LN tables), one L0 table may eventually be distributed
	// across multiple L1 tables or even whole L1 level.
	// So what we do is just load whole L0 and L1 levels into memory and merge
	// them. It can be done in a more optimized way but this is a toy lsm tree
	// anyway.
	lvl1Mem := make([]*Memtable, 0, len(lvl1))
	for _, sst1 := range lvl1 {
		lvl1Mem = append(lvl1Mem, MemtableFromSSTable(sst1))
	}

	for _, sst0 := range c.tree.lvl0 {
		sst0Mem := MemtableFromSSTable(sst0)
		newLvl1Mem, err := MergeWithMultiple(sst0Mem, lvl1Mem, sst1Size)
		if err != nil {
			return err
		}

		lvl1Mem = newLvl1Mem
	}

	newLvl1 := make([]*SSTable, 0, len(lvl1Mem))
	for i, mem1 := range lvl1Mem {
		sst1, err := SSTableFromReadonlyMemtable(
			mem1.AsReadonly(),
			"SST_L1_"+strconv.Itoa(i), // TODO filename
			c.opt,
		)

		if err != nil {
			return err
		}

		lvl1Size += sst1.Size()
		newLvl1 = append(newLvl1, &sst1)
	}

	c.tree.lvln[0] = newLvl1

	// finish compaction if L1 has space under threshold
	if lvl1Size < uint(c.opt.L1Threshold) {
		return nil
	}

	// otherwise run LN compaction. We run it in background since we freed mem,
	// romem, and L0 space for LSM tree to function properly, there is no need
	// here to block on full compaction
	go c.compactFull()

	return nil
}

func (c *Compactor) compactFull() {

}

// Merges 1 memtable with N memtables producing M memtables where M>=N.
// Result len is M because memtable size is fixed and will likely
// overflow into one other memtable while merging.
func MergeWithMultiple(one *Memtable, other []*Memtable, maxTableSize int) ([]*Memtable, error) {
	// как колбасу
	other = append(other, one)

	bigPile := NewMemtable()
	for _, t := range other {
		t.Range(func(key string, value []byte) bool {
			bigPile.Put([]byte(key), value)
			return true
		})
	}

	// sadly we have to copy the whole pile 😪 (not gonna optimize it tho)
	out := make([]*Memtable, 0, len(other))
	curr := NewMemtable()
	bigPile.Range(func(key string, value []byte) bool {
		curr.Put([]byte(key), value)

		if curr.Size() >= maxTableSize {
			out = append(out, curr)
			curr = NewMemtable()
		}

		return true
	})

	return out, nil
}
