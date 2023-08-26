package codec

import "gopkg.in/mgo.v2/bson"

func BsonEncode[T any](value T) ([]byte, error) {
	return bson.Marshal(value)
}

func BsonDecode[T any](data []byte) (T, error) {
	var v T
	err := bson.Unmarshal(data, &v)
	return v, err
}

func NewBsonCodec[T any]() Codec[T] {
	return Codec[T]{encode: BsonEncode[T], decode: BsonDecode[T], tag: "bson"}
}
