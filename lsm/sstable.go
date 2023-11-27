package lsm

import (
	"birb/pkg/byteutil"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
)

var (
	ErrKeyNotFound = errors.New("no such key found")
)

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
	index SSTIndex // is nil when not loaded
	meta  Meta
}

func (t *SSTable) Size() uint {
	return uint(t.meta.DataLen) + uint(t.meta.IndexLen) + MetaSize
}

func (t *SSTable) FirstKey() []byte {
	return t.index[0].firstKey
}

func (t *SSTable) LastKey() []byte {
	return t.index[len(t.index)-1].lastKey
}

func (t *SSTable) InRange(k []byte) bool {
	moreThanFirst := bytes.Compare(k, t.FirstKey()) >= 0
	lessThanLast := bytes.Compare(k, t.LastKey()) <= 0
	return moreThanFirst && lessThanLast
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

	// find block in sst index
	i, found := slices.BinarySearchFunc(t.index, key, func(e SSTIndexEntry, t []byte) int {
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

	block, err := BlockFromSectReader(reader)
	if err != nil {
		return nil, err
	}

	// find entry in block index
	i, found = slices.BinarySearchFunc(block.index, key,
		func(e *BlockIndexValue, t []byte) int {
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

	return EntryFromBytes(entryBuf).Value, nil
}

// TODO: localize Options
func SSTableFromReadonlyMemtable(
	memro ReadonlyMemtable,
	path string,
	opts Options,
) (SSTable, error) {
	table := byteutil.NewSeqWriter[byte]()
	tableIndex := byteutil.NewSeqWriter[byte]()

	block := byteutil.NewSeqWriter[byte]()
	blockIndex := byteutil.NewSeqWriter[byte]()
	var firstBlockKey *string

	memro.Range(func(key string, value []byte) bool {
		if firstBlockKey == nil {
			firstBlockKey = &key
		}

		startOffset := block.Offset()
		// write entry
		block.Write(byteutil.Uint16ToByteSlice(uint16(len(key))))
		block.Write([]byte(key))
		block.Write(byteutil.Uint16ToByteSlice(uint16(len(value))))
		block.Write(value)
		// write index
		blockIndex.Write(byteutil.Uint16ToByteSlice(
			uint16(table.Offset() + block.Offset())))
		blockIndex.Write(byteutil.Uint16ToByteSlice(
			uint16(block.Offset() - startOffset)))

		if block.Len() >= opts.BlockThreshold {
			tableStartOffset := table.Offset()
			meta := Meta{
				DataOffset:  uint16(tableStartOffset),
				DataLen:     uint16(block.Len()),
				IndexOffset: uint16(tableStartOffset + block.Len()),
				IndexLen:    uint16(blockIndex.Len()),
			}
			// write block data
			table.Write(block.Slice())
			// write block index
			table.Write(blockIndex.Slice())
			// write block meta
			table.Write(byteutil.Uint16ToByteSlice(meta.DataOffset))
			table.Write(byteutil.Uint16ToByteSlice(meta.DataLen))
			table.Write(byteutil.Uint16ToByteSlice(meta.IndexOffset))
			table.Write(byteutil.Uint16ToByteSlice(meta.IndexLen))

			// write entry to table index: offset, len, first key, last key
			tableIndex.Write(byteutil.Uint16ToByteSlice(uint16(tableStartOffset)))
			tableIndex.Write(byteutil.Uint16ToByteSlice(uint16(block.Len())))

			tableIndex.Write(
				byteutil.Uint16ToByteSlice(uint16(len(*firstBlockKey))))
			tableIndex.Write([]byte(*firstBlockKey))

			tableIndex.Write(byteutil.Uint16ToByteSlice(uint16(len(key))))
			tableIndex.Write([]byte(key))

			firstBlockKey = nil
			block = byteutil.NewSeqWriter[byte]()
			blockIndex = byteutil.NewSeqWriter[byte]()
		}

		return true
	})

	file, err := os.Create(path)
	if err != nil {
		return SSTable{}, err
	}

	r := bytes.NewReader(tableIndex.Slice())
	sr := io.NewSectionReader(r, 0, int64(r.Len()))
	index, err := SSTIndexFromSectReader(sr, opts.BlockThreshold)
	if err != nil {
		return SSTable{}, err
	}

	meta := Meta{
		DataOffset:  0,
		DataLen:     uint16(table.Len()),
		IndexOffset: uint16(table.Len()),
		IndexLen:    uint16(tableIndex.Len()),
	}

	return SSTable{file, index, meta}, nil
}

func SSTableFromFile(file *os.File) (SSTable, error) {
	stat, err := file.Stat()
	if err != nil {
		return SSTable{}, err
	}

	meta, err := MetaFromSectReader(
		io.NewSectionReader(file, stat.Size()-MetaSize, MetaSize))
	if err != nil {
		return SSTable{}, err
	}

	index, err := SSTIndexFromSectReader(
		io.NewSectionReader(file, int64(meta.IndexOffset), int64(meta.IndexLen)), 0)
	if err != nil {
		return SSTable{}, err
	}

	return SSTable{file, index, meta}, nil
}

// TODO: blocksLen (pass it as opts?)
func SSTIndexFromSectReader(r *io.SectionReader, blocksLen int) (SSTIndex, error) {
	buf := make([]byte, 0, r.Size())
	_, err := r.Read(buf)
	if err != nil {
		return nil, err
	}

	// FIXME: count by blocksLen
	indexVals := make(SSTIndex, 0)
	for left := r.Size(); left > 0; {
		val, read, err := SSTIndexValueFromBytes(buf)
		if err != nil {
			return nil, err
		}

		indexVals = append(indexVals, val)
		buf = buf[read:]
		left -= int64(read)
	}

	return indexVals, nil
}

type SSTIndex []SSTIndexEntry

type SSTIndexEntry struct {
	offset   uint16
	len      uint16
	firstKey []byte
	lastKey  []byte
}

func SSTIndexValueFromBytes(b []byte) (idxval SSTIndexEntry, read int, err error) {
	idxval = SSTIndexEntry{}

	// read offset and len
	idxval.offset = binary.LittleEndian.Uint16(b)
	b = b[2:]
	idxval.len = binary.LittleEndian.Uint16(b)
	b = b[2:]

	// read first and last keys
	firstKey, fread := keyFromBytes(b)
	b = b[read:]
	lastKey, lread := keyFromBytes(b)

	idxval.firstKey = firstKey
	idxval.lastKey = lastKey

	return idxval, int(fread + lread + 2 + 2), nil
}

const MetaSize = 8

type Meta struct {
	DataOffset  uint16
	DataLen     uint16
	IndexOffset uint16
	IndexLen    uint16
	//LastKeyOffset uint16
}

func MetaFromSectReader(r *io.SectionReader) (Meta, error) {
	if r.Size() != MetaSize {
		return Meta{}, fmt.Errorf("meta must be 8 bytes long")
	}

	buf := make([]byte, 0, MetaSize)
	_, err := r.Read(buf)
	if err != nil {
		return Meta{}, err
	}

	meta := Meta{}

	meta.DataOffset = binary.LittleEndian.Uint16(buf)
	buf = buf[2:]
	meta.DataLen = binary.LittleEndian.Uint16(buf)
	buf = buf[2:]
	meta.IndexOffset = binary.LittleEndian.Uint16(buf)
	buf = buf[2:]
	meta.IndexLen = binary.LittleEndian.Uint16(buf)
	//buf = buf[2:]
	//meta.LastKeyOffset = binary.LittleEndian.Uint16(buf)

	return meta, nil
}
