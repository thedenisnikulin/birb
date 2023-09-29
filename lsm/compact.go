package lsm

type Compactor struct {
	policy Policy
	Runc   chan<- struct{}
	Waitc  chan<- struct{}
}

type Policy interface{} // ???
type LeveledPolicy struct{}

// merges two sstables into one
func Merge(lhs *SSTable, rhs *SSTable) (*SSTable, error) {
	panic("unimpl")
}

func Compact()
