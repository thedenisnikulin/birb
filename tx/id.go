package tx

type TxID struct {
	Id    uint32 // allowed to wrap
	Epoch uint32
}

func (id TxID) Uint64() uint64 {
	return uint64(id.Id<<31 + id.Epoch) // TODO check this
}

func TxIdFromUint64(n uint64) TxID {
	return TxID{Id: uint32(n >> 32), Epoch: uint32(n)}
}
