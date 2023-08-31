package internal

import (
	"birb/bvalue"
	"birb/codec"
	"birb/key"
	"birb/storage"
	"birb/txid"
)

func Find[R any](storage storage.Storage[[]byte], codec codec.Codec[R], key string) (R, bool) {
	recb, ok := storage.Get(key)
	if !ok {
		var r R
		return r, false
	}

	rec, _ := codec.Decode(recb)
	return rec, true
}

func FindCommitedLatestVersion[R any](
	storage storage.Storage[[]byte],
	codec codec.Codec[R],
	pk bvalue.Value,
	id txid.ID,
	ns string,
) (key.Key, R, bool) {
	baseKey := "rec_" + ns + "_pk_" + pk.String() + "_com"

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

		if !key.Xmin.Less(latestXmin) && key.Xmin.Less(id) {
			latestXmin = key.Xmin
			latestKeyRaw = keyRaw
			latestKey = key
		}
	}

	if latestKeyRaw == "" {
		var r R
		return latestKey, r, false
	}

	recb, ok := Find(storage, codec, latestKeyRaw)
	return latestKey, recb, ok
}
