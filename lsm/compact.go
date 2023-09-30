package lsm

import "context"

type Compactor struct {
	policy Policy
	runc   chan struct{}
	waitc  chan struct{}
}

func (c *Compactor) Runc() chan<- struct{} {
	return c.runc
}

func (c *Compactor) Waitc() chan<- struct{} {
	return c.waitc
}

func (c *Compactor) Run(ctx context.Context) {
	for {
		select {
		case <-c.runc:
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
