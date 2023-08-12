package relational

import (
	"encoding/json"

	"gopkg.in/mgo.v2/bson"
)

type Codec[T any] struct {
	Encode Encode[T]
	Decode Decode[T]
}

type Encode[T any] func(value T) ([]byte, error)
type Decode[T any] func(data []byte) (T, error)

func JsonEncode[T any](value T) ([]byte, error) {
	return json.Marshal(value)
}

func JsonDecode[T any](data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}

func BsonEncode[T any](value T) ([]byte, error) {
	return bson.Marshal(value)
}

func BsonDecode[T any](data []byte) (T, error) {
	var v T
	err := bson.Unmarshal(data, &v)
	return v, err
}

func NewJsonCodec[T any]() Codec[T] {
	return Codec[T]{Encode: JsonEncode[T], Decode: JsonDecode[T]}
}
