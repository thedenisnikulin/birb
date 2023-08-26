package internal

import (
	bval "birb/bvalue"
	"birb/codec"
	"birb/storage"
)

const (
	PrimaryKeyTag string = "pk"
)

func IndexKey(ns string, field string, value bval.Value) string {
	return "idx_" + Key(ns, field, value)
}

func Key(ns string, field string, value bval.Value) string {
	return ns + "_" + field + "_" + value.String()
}

func Find[R any](storage storage.Storage[[]byte], codec codec.Codec[R], key string) (R, bool) {
	recb, ok := storage.Get(key)
	if !ok {
		var r R
		return r, false
	}

	rec, _ := codec.Decode(recb)
	return rec, true
}
