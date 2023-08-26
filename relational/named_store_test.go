package relational

import (
	bval "birb/bvalue"
	"birb/codec"
	"birb/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

type user struct {
	Name string `bson:"name"`
	Age  int    `bson:"age"`
}

func TestNamedStore(t *testing.T) {
	// arrange
	prefixTreeStorage := storage.NewPrefixTreeStorage[[]byte]()
	bsonCodec := codec.NewBsonCodec[user]()
	rel := RelationalStore{prefixTreeStorage}

	namedStore, err := Use(&rel, bsonCodec, "users")
	if err != nil {
		panic(err)
	}

	u := user{
		Name: "rwrwrw",
		Age:  21,
	}

	// act
	namedStore.Upsert(bval.FromInt(1), u)
	recByPk, okByPk := namedStore.Find(bval.FromInt(1))

	namedStore.AddIndex("name")
	recByNameIdx, okByNameIdx := namedStore.FindByIndex("name", bval.FromString("rwrwrw"))

	namedStore.AddIndex("age")
	recByAgeIdx, okByAgeIdx := namedStore.FindByIndex("age", bval.FromInt(21))

	for k, v := range prefixTreeStorage.ToMap() {
		t.Logf("[%s]\t= [%s]", k, string(v))
	}

	// assert
	assert.True(t, okByPk)
	assert.Equal(t, u, recByPk)

	assert.True(t, okByNameIdx)
	assert.Equal(t, u, recByNameIdx)

	assert.True(t, okByAgeIdx)
	assert.Equal(t, u, recByAgeIdx)
}
