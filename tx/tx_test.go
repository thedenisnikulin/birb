package tx

import (
	"birb/bvalue"
	"birb/codec"
	"birb/storage"
	"birb/txid"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type product struct {
	Title string
	Price int
}

func TestTx(t *testing.T) {
	// arrange
	now := time.Now()

	stg := storage.NewPrefixTreeStorage[[]byte]()
	codec := codec.NewBsonCodec[product]()
	txnid := txid.New(now, 1)
	txn := New("products", stg, codec, txnid)

	// act
	id := bvalue.FromInt(12)
	txn.Upsert(id, product{Title: "сковорода", Price: 5000})
	txn.Delete(id)
	_, ok := txn.Find(id)

	// assert
	assert.False(t, ok)
}
