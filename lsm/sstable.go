package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
)

var (
	ErrKeyNotFound = errors.New("no such key in sstable")
)

type Level struct {
	SSTables []*SSTable
}

// SSTable is a inmem view over on disk sstable.
// SSTable has the following layout:
// 1. blocks of data (blocks which contain keys and values),
// 2. block index (first keys of each block for doing binary search when
// needed to find a block with particular key),
// 3. table metadata (data and index offsets and length in the file).
// SSTable is lazy, which means only block index is loaded into memory, data
// blocks are accessed when a particular key is requested.
type SSTable struct {
	file  *os.File
	index BlockIndex // is nil when not loaded
	meta  SSTableMeta
}

func (t *SSTable) Exist(key []byte) bool {
	panic("unimpl")
}

// TODO implement Get for []SSTable, and bloom filter (for blocks and sstables?)
// TODO implement block cache??
func (t *SSTable) Get(key []byte) ([]byte, error) {
	if t.index == nil {
		sst, err := SSTableFromFile(t.file) // this func looks bad here honestly
		if err != nil {
			return nil, err
		}

		t.index = sst.index
	}

	i, found := slices.BinarySearchFunc(t.index, key, func(e BlockIndexValue, t []byte) int {
		return bytes.Compare(e.firstKey, t)
	})

	if !found {
		i -= 1
	}

	blockIdx := t.index[i]

	reader := io.NewSectionReader(
		t.file,
		int64(blockIdx.offset),
		int64(blockIdx.len))

	block, err := BlockFromBytes(reader)
	if err != nil {
		return nil, err
	}

	i, found = slices.BinarySearchFunc(block.index, key,
		func(e *EntryIndexValue, t []byte) int {
			sr := io.NewSectionReader(block.file, int64(e.offset), int64(e.len))
			key := keyFromSect(sr)
			e.r = sr
			return bytes.Compare(t, key)
		})

	if !found {
		return nil, ErrKeyNotFound
	}

	entryIdx := block.index[i]

	entryBuf := make([]byte, entryIdx.len)
	_, err = entryIdx.r.Read(entryBuf)
	if err != nil {
		return nil, err
	}

	return EntryFromBytes(entryBuf).value, nil
}

func SSTableFromReadonlyMemtable(memro ReadonlyMemtable) (SSTable, error) {
	memro.table.skiplist.Range(func(key string, value []byte) bool {
		return true
	})
	panic("not implemented")
}

func SSTableFromFile(file *os.File) (SSTable, error) {
	stat, err := file.Stat()
	if err != nil {
		return SSTable{}, err
	}

	meta, err := SSTableMetaFromSectReader(
		io.NewSectionReader(file, stat.Size()-MetaSize, MetaSize))
	if err != nil {
		return SSTable{}, err
	}

	index, err := BlockIndexFromSectReader(
		io.NewSectionReader(file, int64(meta.indexOffset), int64(meta.indexLen)), 0)
	if err != nil {
		return SSTable{}, err
	}

	return SSTable{file, index}, nil
}

func BlockIndexFromSectReader(r *io.SectionReader, blocksLen int) (BlockIndex, error) {
	buf := make([]byte, 0, r.Size())
	_, err := r.Read(buf)
	if err != nil {
		return nil, err
	}

	// FIXME count by blocksLen
	indexVals := make(BlockIndex, 0)
	for left := r.Size(); left > 0; {
		val, read, err := BlockIndexValueFromBytes(buf)
		if err != nil {
			return nil, err
		}

		indexVals = append(indexVals, val)
		buf = buf[read:]
		left -= int64(read)
	}

	return indexVals, nil
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

const MetaSize = 8

type Meta struct {
	dataOffset  uint16
	dataLen     uint16
	indexOffset uint16
	indexLen    uint16
}

type BlockMeta struct {
	Meta
}

type SSTableMeta struct {
	Meta
	firstKey []byte // TODO meta will be variable length then T_T
}

func SSTableMetaFromSectReader(r *io.SectionReader) (Meta, error) {
	if r.Size() != MetaSize {
		return Meta{}, fmt.Errorf("meta must be 8 bytes long")
	}

	buf := make([]byte, 0, MetaSize)
	_, err := r.Read(buf)
	if err != nil {
		return Meta{}, err
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
