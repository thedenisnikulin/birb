package relational

import (
	"main/internal/storage"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNamedStore(t *testing.T) {
	// arrange
	prefixTreeStorage := storage.NewPrefixTreeStorage[Record]()
	jsonCodec := NewJsonCodec[map[string]any]()
	rel := RelationalStore{prefixTreeStorage, jsonCodec}

	namedStore := rel.Use("users")

	value := map[string]any{
		"name": "rwrwrw",
		"age":  float64(21),
	}
	valueBytes, _ := jsonCodec.Encode(value)
	record := Record(valueBytes)
	id := []byte(strconv.Itoa(1))

	// act
	namedStore.Insert(id, record)
	foundRecordByPk, okByPk := namedStore.Find(id)
	foundValueByPk, decodeErrByPk := jsonCodec.Decode(foundRecordByPk)

	namedStore.AddIndex("name")
	foundRecordByIdx, okByIdx := namedStore.FindByIndex(Field{"name", []byte("rwrwrw")})
	foundValueByIdx, decodeErrByIdx := jsonCodec.Decode(foundRecordByIdx)

	for k, v := range prefixTreeStorage.ToMap() {
		t.Logf("[%s]\t= [%s]", k, string(v))
	}

	// assert
	assert.True(t, okByPk)
	assert.NoError(t, decodeErrByPk)
	assert.Equal(t, value, foundValueByPk)

	assert.True(t, okByIdx)
	assert.NoError(t, decodeErrByIdx)
	assert.Equal(t, value, foundValueByIdx)
}
