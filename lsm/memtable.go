package lsm

import (
	"github.com/zhangyunhao116/skipmap"
)

type Memtable struct {
	skiplist skipmap.StringMap[[]byte]
}
