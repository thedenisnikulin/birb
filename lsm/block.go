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
	index EntryIndex // TODO make it sparse? (not every entry in index)
}

func BlockFromBytes(r *io.SectionReader) (Block, error) {
	metaBuf := make([]byte, MetaSize)
	_, err := r.ReadAt(metaBuf, r.Size()-MetaSize)
	if err != nil {
		return Block{}, err
	}

	meta, err := MetaFromBytes(metaBuf)
	if err != nil {
		return Block{}, err
	}

	index := make(EntryIndex, 0, meta.indexLen/4) // each entry is 4 bytes long
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

type EntryIndex []*EntryIndexValue

func EntryIndexValueFromBytes(buf []byte) (idxval EntryIndexValue, err error) {
	if len(buf) < 4 {
		return EntryIndexValue{}, fmt.Errorf("invalid entry index value length")
	}

	off := binary.LittleEndian.Uint16(buf)
	len := binary.LittleEndian.Uint16(buf[2:])
	return EntryIndexValue{offset: off, len: len}, nil
}

type EntryIndexValue struct {
	offset uint16
	len    uint16
	r      *io.SectionReader
}

// TODO deprecate in favor of BlockLazy
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

func NewBlock(maxSize int) DeprecatedBlock {
	return DeprecatedBlock{
		entries: make([]Entry, 0),
	}
}

// TODO should push sorted somehow
// func (block *Block) Push(k, v []byte) {
// 	entry := NewBlockEntry(k, v)
// 	block.data = append(block.data, entry...)
// 	block.offests = append(block.offests, uint16(len(block.data)-1))
// 	block.len++
// }

// ???
func (DeprecatedBlock) Checksum() []byte {
	panic("not implemented")
}

func (DeprecatedBlock) Bytes() []byte {
	panic("not implemented")
}

func DeprecatedBlockFromBytes(b []byte) (DeprecatedBlock, error) {
	panic("not implemented")
}

// on disk Entry representation:
// -----------------------------------------------------------------------
// |                           Entry #1                            | ... |
// -----------------------------------------------------------------------
// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
// -----------------------------------------------------------------------
type Entry struct {
	key   []byte
	value []byte
}

func (entry Entry) Bytes() []byte {
	if len(entry.key) > math.MaxUint16 || len(entry.value) > math.MaxUint16 {
		panic("block entry key or value size is larger than max(uint16)")
	}

	if len(entry.key) == 0 {
		panic("block entry key cannot be empty")
	}

	lenbuf := make([]byte, 2)
	bytes := make([]byte, 0, 4)

	binary.LittleEndian.PutUint16(lenbuf, uint16(len(entry.key)))
	bytes = append(bytes, lenbuf...)
	bytes = append(bytes, entry.key...)

	binary.LittleEndian.PutUint16(lenbuf, uint16(len(entry.value)))
	bytes = append(bytes, lenbuf...)
	bytes = append(bytes, entry.value...)

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

	return Entry{key: key, value: val}
}

type BlockIter struct {
	block  DeprecatedBlock
	offset int
}

func (it *BlockIter) SeekToFirst() {
	if len(it.block.entries) == 0 {
		return
	}

	it.offset = 0
}

func (it *BlockIter) SeekToKey(key []byte) bool {
	offset, found := slices.BinarySearchFunc(it.block.entries, key,
		func(entry Entry, target []byte) int {
			return bytes.Compare(entry.key, target)
		})

	if found {
		it.offset = offset
	}

	return found
}

func (it *BlockIter) Next() bool {
	if it.offset+1 >= len(it.block.entries) {
		return false
	}

	it.offset++
	return true
}

func (it BlockIter) Entry() Entry {
	if it.offset >= len(it.block.entries) {
		panic("out of bounds")
	}

	return it.block.entries[it.offset]
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
