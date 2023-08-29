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
) (R, bool) {
	baseKey := "rec_" + ns + "_pk_" + pk.String() + "_com"

	rng := storage.Range(baseKey)
	var latestXmin txid.ID
	var latestKey string
	for rng.Next() {
		keyRaw, _ := rng.Value()

		key, err := key.FromString(keyRaw)
		if err != nil {
			panic("incorrect storage key format")
		}

		if !key.Xmin.Less(latestXmin) && key.Xmin.Less(id) {
			latestXmin = key.Xmin
			latestKey = keyRaw
		}
	}

	if latestKey == "" {
		var r R
		return r, false
	}

	return Find(storage, codec, latestKey)
}
