package txid

import (
	"sync/atomic"
	"time"
)

// Stores only 'xid' part
type AtomicID uint32

func (aid AtomicID) ToID() ID {
	return ID{epoch: uint32(time.Now().Unix()), xid: uint32(aid)}
}

func (aid *AtomicID) Inc() AtomicID {
	return AtomicID(atomic.AddUint32((*uint32)(aid), 1))
}
