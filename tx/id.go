package tx

import (
	"strconv"
	"time"
)

// An ID that represents one particular transaction. Ordered, and can be
// compared for order (i.e. partially compared) using [TxID.Less] method.
type TxID struct {
	epoch uint32
	xid   uint32 // allowed to wrap
}

func (id TxID) Uint64() uint64 {
	return uint64(id.epoch)<<32 + uint64(id.xid)
}

// destructures txid
func (id TxID) 나뉘다() (epoch uint32, xid uint32) {
	return id.epoch, id.xid
}

// Increments TxID by assigning time.Now() to epoch and adding 1 to xid
func (id TxID) Inc() TxID {
	return TxID{
		epoch: uint32(time.Now().Unix()),
		xid:   id.xid + 1,
	}
}

func (id TxID) Less(rhs TxID) bool {
	return id.Uint64() <= rhs.Uint64()
}

func TxIDFromUint64(n uint64) TxID {
	return TxID{
		epoch: uint32(n >> 32),
		xid:   uint32(n),
	}
}

func TxIDFromString(s string) (TxID, error) {
	txidRaw, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return TxID{}, err
	}

	return TxIDFromUint64(txidRaw), nil
}
