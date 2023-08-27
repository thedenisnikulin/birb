package tx

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTxID(t *testing.T) {
	// arrange
	txid := TxIDFromUint64(0x12345678_87654321)

	// act
	txid64 := txid.Uint64()
	epoch, xid := txid.나뉘다()

	// assert
	assert.Equal(t, txid64, uint64(0x12345678_87654321))
	assert.Equal(t, epoch, uint32(0x12345678))
	assert.Equal(t, xid, uint32(0x87654321))
}

func TestTxIDCmp(t *testing.T) {
	// arrange
	lhs := TxIDFromUint64(0x64EA8560_00000002) // TODO think how better use ordering cos it sucks apparently
	rhs := TxIDFromUint64(0x64EA8560_00000050)

	// act
	less := lhs.Less(rhs)
	lessButActuallyEq := lhs.Less(lhs)

	// assert
	assert.True(t, less)
	assert.True(t, lessButActuallyEq)
}

func TestTxIDInc(t *testing.T) {
	// arrange
	txid := TxIDFromUint64(0x12345678_00000001)
	_, xid := txid.나뉘다()
	now := time.Now().Unix()

	// act
	newTxid := txid.Inc()
	newEpoch, newXid := newTxid.나뉘다()

	// assert
	assert.Equal(t, newEpoch, uint32(now))
	assert.Equal(t, newXid, xid+1)
}
