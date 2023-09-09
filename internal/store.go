package internal

import (
	"birb/bvalue"
	"birb/codec"
	"birb/key"
	"birb/storage"
	"birb/txid"
)

func FindExact[R any](storage storage.Storage[[]byte], codec codec.Codec[R], key string) (R, bool) {
	recb, ok := storage.Get(key)
	if !ok {
		var r R
		return r, false
	}

	rec, _ := codec.Decode(recb)
	return rec, true
}

func FindLatestCommitted[R any](
	storage storage.Storage[[]byte],
	codec codec.Codec[R],
	fieldName string,
	fieldValue bvalue.Value,
	id txid.ID,
	ns string,
) (key.Key, R, bool) {
	baseKey := "rec_com_" + ns + "_" + fieldName + "_" + fieldValue.String()

	rng := storage.Range(baseKey)
	var latestXmin txid.ID
	var latestKeyRaw string
	var latestKey key.Key
	for rng.Next() {
		keyRaw, _ := rng.Value()

		key, err := key.FromString(keyRaw)
		if err != nil {
			panic("incorrect storage key format")
		}

		validIdRange := key.Xmin.Less(id) && id.Less(key.Xmax)
		if validIdRange && !key.Xmin.Less(latestXmin) {
			latestXmin = key.Xmin
			latestKeyRaw = keyRaw
			latestKey = key
		}
	}

	if latestKeyRaw == "" {
		var r R
		return latestKey, r, false
	}

	recb, ok := FindExact(storage, codec, latestKeyRaw)
	return latestKey, recb, ok
}
