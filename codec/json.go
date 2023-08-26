package codec

import "encoding/json"

func NewJsonCodec[T any]() Codec[T] {
	return Codec[T]{encode: JsonEncode[T], decode: JsonDecode[T], tag: "json"}
}

func JsonEncode[T any](value T) ([]byte, error) {
	return json.Marshal(value)
}

func JsonDecode[T any](data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}
