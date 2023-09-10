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

func arrange() (*collection.Store[product], storage.Storage[[]byte]) {
	stg := storage.NewPrefixTreeStorage[[]byte]()
	codec := codec.NewBsonCodec[product]()
	txiss := txid.NewAtomicIssuer()
	database := NewDatabase(stg, &txiss)
	store, err := UseCollection(&database, codec, "products")
	if err != nil {
		panic(err)
	}

	return store, stg
}

func debugStg(t *testing.T, stg storage.Storage[[]byte]) {
	c := codec.NewBsonCodec[product]()
	for k, v := range stg.ToMap() {
		v, _ := c.Decode(v)
		t.Logf("[%s]: %+v", k, v)
	}
}

type product struct {
	Title string
	Price int
}

// TODO PLAN
// - [x] add tests for all TX functionality (not all though but sufficient)
// - [ ] implement indices for tx
// - [ ] dead rows cleaning?
// - [ ] raft?

func TestTxRollback(t *testing.T) {
	// arrange
	store, _ := arrange()
	id := bvalue.FromInt(12)

	// act
	store.Upsert(id, product{"чайник", 1000})

	_ = store.Tx(func(tx tx.Store[product]) error {
		tx.Upsert(id, product{"сковорода", 5000})
		return errors.New("damn")
	})

	p, ok := store.Find(id)

	// assert
	assert.True(t, ok)
	assert.Equal(t, "чайник", p.Title)
	assert.Equal(t, 1000, p.Price)
}

func TestCommit(t *testing.T) {
	// arrange
	store, stg := arrange()
	id12 := bvalue.FromInt(12)
	id48 := bvalue.FromInt(48)

	// act
	store.Upsert(id12, product{"сковорода", 5000})
	store.Upsert(id48, product{"чайник", 1000})

	store.Tx(func(tx tx.Store[product]) error {
		tx.Upsert(id12, product{"сковородочка", 4999})
		tx.Delete(id48)
		return nil
	})

	pan, panOk := store.Find(id12)
	_, kettleOk := store.Find(id48)

	// assert
	debugStg(t, stg)

	assert.True(t, panOk)
	assert.Equal(t, product{"сковородочка", 4999}, pan)

	assert.False(t, kettleOk)
}
