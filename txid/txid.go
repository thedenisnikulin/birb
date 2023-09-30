// TODO made it into separate pkg to avoid import cycle,
// may be changed later.
// Change to "txutil"?
package txid

import (
	"strconv"
	"time"
)

// An ID that represents one particular transaction. Ordered, and can be
// compared for order (i.e. partially compared) using [ID.Less] method.
// Zero value is a valid ID.
type ID struct {
	epoch uint32
	xid   uint32 // allowed to wrap
}

func (id ID) Uint64() uint64 {
	return uint64(id.epoch)<<32 + uint64(id.xid)
}

// destructures txid (yep flexing hangul here)
func (id ID) 나뉘다() (epoch uint32, xid uint32) {
	return id.epoch, id.xid
}

// Increments TxID by assigning time.Now() to epoch and adding 1 to xid
func (id ID) Inc() ID {
	return ID{
		epoch: uint32(time.Now().Unix()),
		xid:   id.xid + 1,
	}
}

func (id ID) Less(rhs ID) bool {
	return id.Uint64() <= rhs.Uint64()
}

func (id ID) String() string {
	n := id.Uint64()
	return strconv.FormatUint(n, 16)
}

func FromUint64(n uint64) ID {
	return ID{
		epoch: uint32(n >> 32),
		xid:   uint32(n),
	}
}

func FromString(s string) (ID, error) {
	txidRaw, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return ID{}, err
	}

	return FromUint64(txidRaw), nil
}

func New(t time.Time, xid uint32) ID {
	return ID{epoch: uint32(t.Unix()), xid: xid}
}

func Max() ID {
	return ID{0xFFFFFFFF, 0xFFFFFFFF}
}

func Min() ID {
	return ID{}
}
