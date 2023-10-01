package lsm

import "context"

// TODO stopping/killing program while compacting will put it int inconsistent
// state, need to think of how to overcome it (or skip it for sake of simplicity?)
type Compactor struct {
	policy   Policy
	triggerc chan struct{}
	waitc    chan struct{}
}

func (c *Compactor) Triggerc() chan<- struct{} {
	return c.triggerc
}

func (c *Compactor) Waitc() chan<- struct{} {
	return c.waitc
}

func (c *Compactor) Run(ctx context.Context) {
	for {
		select {
		case <-c.triggerc:
			// TODO can do only partial compaction synchronously, and finish the
			// rest of compaction asynchronously (e.g. memro and L0 are full,
			// we merge L0 and L1, put memro into L0, thus memro is
			// now free and if L1 needs to be merged into L2, we can do it
			// asynchronously), in order to release c.waitc faster
			c.compact()
		case <-c.waitc:
		case <-ctx.Done():
			return
		}
	}
}

func (c *Compactor) compact() {}

type Policy interface{} // ???
type LeveledPolicy struct{}

// merges two sstables into one
func Merge(lhs *SSTable, rhs *SSTable) (*SSTable, error) {
	panic("unimpl")
}
