package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"
)

// Block is lazy, which means only block entry index is loaded into memory,
// entries are accessed on request.
type Block struct {
	file  *io.SectionReader
	index BlockIndex // TODO make it sparse? (not every entry in index)
}

func BlockFromSectReader(r *io.SectionReader) (Block, error) {
	metaBuf := make([]byte, MetaSize)
	_, err := r.ReadAt(metaBuf, r.Size()-MetaSize)
	if err != nil {
		return Block{}, err
	}

	meta, err := MetaFromBytes(metaBuf)
	if err != nil {
		return Block{}, err
	}

	index := make(BlockIndex, 0, meta.indexLen/4) // each entry is 4 bytes long
	endoff := meta.indexOffset + meta.indexLen
	for off := meta.indexOffset; off < endoff; off = +4 {
		buf := [4]byte{}
		_, err := r.ReadAt(buf[:], int64(off))
		if err != nil {
			return Block{}, err
		}

		idxval, err := EntryIndexValueFromBytes(buf[:])
		if err != nil {
			return Block{}, err
		}

		index = append(index, &idxval)
	}

	return Block{r, index}, nil
}

type BlockIndex []*BlockIndexValue

func EntryIndexValueFromBytes(buf []byte) (idxval BlockIndexValue, err error) {
	if len(buf) < 4 {
		return BlockIndexValue{}, fmt.Errorf("invalid entry index value length")
	}

	off := binary.LittleEndian.Uint16(buf)
	len := binary.LittleEndian.Uint16(buf[2:])
	return BlockIndexValue{offset: off, len: len}, nil
}

type BlockIndexValue struct {
	offset uint16 // TODO: remove offset and len in favor of r?
	len    uint16
	r      *io.SectionReader // a ready to read reader with .offset and .len
}

// TODO deprecate in favor of Block
// on disk DeprecatedBlock representation:
// ---------------------------------------------------------------------
// |          data         |          offsets          |      meta     |
// |-----------------------|---------------------------|---------------|
// |entry|entry|entry|entry|offset|offset|offset|offset|num_of_elements|
// ---------------------------------------------------------------------
// TODO add bloom filter
type DeprecatedBlock struct {
	entries []Entry
}

// on disk Entry representation:
// -----------------------------------------------------------------------
// |                           Entry #1                            | ... |
// -----------------------------------------------------------------------
// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
// -----------------------------------------------------------------------
type Entry struct {
	Key   []byte
	Value []byte
}

func (entry Entry) Bytes() []byte {
	if len(entry.Key) > math.MaxUint16 || len(entry.Value) > math.MaxUint16 {
		panic("block entry key or value size is larger than max(uint16)")
	}

	if len(entry.Key) == 0 {
		panic("block entry key cannot be empty")
	}

	lenbuf := make([]byte, 2)
	bytes := make([]byte, 0, 4)

	binary.LittleEndian.PutUint16(lenbuf, uint16(len(entry.Key)))
	bytes = append(bytes, lenbuf...)
	bytes = append(bytes, entry.Key...)

	binary.LittleEndian.PutUint16(lenbuf, uint16(len(entry.Value)))
	bytes = append(bytes, lenbuf...)
	bytes = append(bytes, entry.Value...)

	return bytes
}

func EntryFromBytes(entry []byte) Entry {
	var keyOffset uint16 = 0
	keyLen := binary.LittleEndian.Uint16(entry[keyOffset:2])
	start := 2 + keyOffset
	key := []byte(entry[start : start+keyLen])

	valOffset := 2 + keyLen
	valLen := binary.LittleEndian.Uint16(entry[valOffset : valOffset+2])
	start = 2 + valOffset
	val := []byte(entry[start : start+valLen])

	return Entry{Key: key, Value: val}
}

func bytesToUint16(b []byte) uint16 {
	if len(b) != 2 {
		panic("bytes to uint16: input size not equals 2")
	}

	return binary.LittleEndian.Uint16(b)
}

func keyFromBytes(b []byte) (key []byte, read int) {
	keylen := bytesToUint16(b[:2])
	b = b[2:]
	return b[:keylen], 2 + int(keylen)
}

func keyFromSect(r *io.SectionReader) []byte {
	lenbuf := [2]byte{}
	_, err := r.Read(lenbuf[:])
	if err != nil {
		panic(err)
	}

	keylen := bytesToUint16(lenbuf[:])
	outbuf := make([]byte, keylen)
	_, err = r.Read(outbuf)
	if err != nil {
		panic(err)
	}

	return outbuf
}

type Iter[T any] interface {
	Value() T
	Next() bool
	Err() error
}

type BlockIter struct {
	block *Block
	idx   int
	err   error
}

func (it *BlockIter) Next() bool {
	return len(it.block.index) > it.idx && it.err == nil
}

func (it *BlockIter) Err() error {
	return it.err
}

func (it *BlockIter) Value() Entry {
	r := it.block.index[it.idx].r

	buf := make([]byte, 0, r.Size())
	_, err := r.Read(buf)
	if err != nil {
		it.err = err
		return Entry{}
	}

	it.idx++

	return EntryFromBytes(buf)
}
