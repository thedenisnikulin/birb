package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
)

// -------------------------------------------------------------------------------------------
// |         Block Section         |          Meta Section         |          Extra          |
// -------------------------------------------------------------------------------------------
// | data block | ... | data block | meta block | ... | meta block | meta block offset (u32) |
// -------------------------------------------------------------------------------------------
// where data block = [Block]
//
// SSTable has the following layout:
// 1. blocks of data (blocks which contain keys and values),
// 2. block index (first keys of each block for doing binary search when
// needed to find a block with particular key),
// 3. table metadata (data and index offsets and length in the file).
// Only block index is loaded into memory, data blocks are accessed lazily when
// a particular key is requested.
type SSTable struct {
	file  *os.File
	index BlockIndex
}

// TODO implement Get for []SSTable, and bloom filter (for blocks and sstables?)
func (t *SSTable) Get(key []byte) ([]byte, error) {
	i, found := slices.BinarySearchFunc(t.index, key, func(e BlockIndexValue, t []byte) int {
		return bytes.Compare(e.firstKey, t)
	})

	if !found {
		i -= 1
	}

	blockIdx := t.index[i]

	panic("not implemented")
}

func SSTableFromMemtable(mem Memtable) (SSTable, error) {
	mem.skiplist.Range(func(key string, value []byte) bool {
		return true
	})
	panic("not implemented")
}

func SSTableFromFile(file *os.File, blocksLen int) (SSTable, error) {
	stat, err := file.Stat()
	if err != nil {
		return SSTable{}, err
	}

	fileSize := stat.Size()

	metaBuf := make([]byte, 0, 8)
	_, err = file.ReadAt(metaBuf, fileSize-8)
	if err != nil {
		return SSTable{}, err
	}

	meta, err := MetaFromBytes(metaBuf)
	if err != nil {
		return SSTable{}, err
	}

	indexBuf := make([]byte, 0, meta.indexLen)
	_, err = file.ReadAt(indexBuf, int64(meta.indexOffset))
	if err != nil {
		return SSTable{}, err
	}

	// FIXME count by blocksLen
	indexVals := make([]BlockIndexValue, 0)
	for left := meta.indexLen; left > 0; {
		val, read, err := BlockIndexValueFromBytes(indexBuf)
		if err != nil {
			return SSTable{}, err
		}

		indexVals = append(indexVals, val)
		indexBuf = indexBuf[read:]
		left -= uint16(read)
	}

	return SSTable{file, indexVals}, nil
}

type BlockIndex []BlockIndexValue

type BlockIndexValue struct {
	firstKey []byte
	offset   uint16
	len      uint16
}

func BlockIndexValueFromBytes(b []byte) (idxval BlockIndexValue, read int, err error) {
	idxval = BlockIndexValue{}

	if len(b) < 2 {
		return BlockIndexValue{}, 0, fmt.Errorf("invalid BlockIndexValue layout")
	}

	keylen := binary.LittleEndian.Uint16(b)
	b = b[2:]
	idxval.firstKey = b[:keylen]
	b = b[keylen:]

	if len(b) != 4 {
		return BlockIndexValue{}, 0, fmt.Errorf("invalid BlockIndexValue layout")
	}

	idxval.offset = binary.LittleEndian.Uint16(b)
	b = b[2:]
	idxval.len = binary.LittleEndian.Uint16(b)

	return idxval, int(keylen + 2*3), nil
}

type Meta struct {
	dataOffset  uint16
	dataLen     uint16
	indexOffset uint16
	indexLen    uint16
}

func MetaFromBytes(buf []byte) (Meta, error) {
	if len(buf) != 8 {
		return Meta{}, fmt.Errorf("meta must be 8 bytes long")
	}

	meta := Meta{}

	meta.dataOffset = binary.LittleEndian.Uint16(buf)
	buf = buf[2:]
	meta.dataLen = binary.LittleEndian.Uint16(buf)
	buf = buf[2:]
	meta.indexOffset = binary.LittleEndian.Uint16(buf)
	buf = buf[2:]
	meta.indexLen = binary.LittleEndian.Uint16(buf)

	return meta, nil
}
