package birb

import (
	"birb/bvalue"
	"birb/codec"
	"birb/collection"
	"birb/storage"
	"birb/tx"
	"birb/txid"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func arrange() *collection.Store[product] {
	stg := storage.NewPrefixTreeStorage[[]byte]()
	codec := codec.NewBsonCodec[product]()
	txiss := txid.MxIssuer{}
	database := NewDatabase(stg, &txiss)
	store, err := UseCollection(&database, codec, "products")
	if err != nil {
		panic(err)
	}

	return store
}

type product struct {
	Title string
	Price int
}

// TODO PLAN
// 1. add tests for all TX functionality
// 2. implement indices for tx

func TestTxRollback(t *testing.T) {
	// arrange
	store := arrange()
	id := bvalue.FromInt(12)

	// act

	store.Upsert(id, product{"чайник", 1000})

	store.Tx(func(tx tx.Store[product]) error {
		tx.Upsert(id, product{"сковорода", 5000})
		//txn.Delete(id)
		return errors.New("damn")
	})

	p, ok := store.Find(id)

	// assert
	assert.True(t, ok)
	assert.Equal(t, p.Title, "чайник")
	assert.Equal(t, p.Price, 1000)
}
