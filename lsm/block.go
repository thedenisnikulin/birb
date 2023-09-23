package lsm

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"slices"
)

type BlockLazy struct {
	file  *os.File
	index map[string]BlockEntryIndexValue
}

type BlockEntryIndexValue struct {
	offset uint16
	len    uint16
}

// TODO deprecate in favor of BlockLazy
// on disk Block representation:
// ---------------------------------------------------------------------
// |          data         |          offsets          |      meta     |
// |-----------------------|---------------------------|---------------|
// |entry|entry|entry|entry|offset|offset|offset|offset|num_of_elements|
// ---------------------------------------------------------------------
// TODO add bloom filter
type Block struct {
	entries []BlockEntry
}

func NewBlock(maxSize int) Block {
	return Block{
		entries: make([]BlockEntry, 0),
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
func (Block) Checksum() []byte {
	panic("not implemented")
}

func (Block) Bytes() []byte {
	panic("not implemented")
}

func BlockFromBytes(b []byte) (Block, error) {
	panic("not implemented")
}

type BlockMeta struct {
}

// on disk BlockEntry representation:
// -----------------------------------------------------------------------
// |                           Entry #1                            | ... |
// -----------------------------------------------------------------------
// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
// -----------------------------------------------------------------------
type BlockEntry struct {
	key   []byte
	value []byte
}

func (entry BlockEntry) Bytes() []byte {
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

func BlockEntryFromBytes(entry []byte) BlockEntry {
	var keyOffset uint16 = 0
	keyLen := binary.LittleEndian.Uint16(entry[keyOffset:2])
	start := 2 + keyOffset
	key := []byte(entry[start : start+keyLen])

	valOffset := 2 + keyLen
	valLen := binary.LittleEndian.Uint16(entry[valOffset : valOffset+2])
	start = 2 + valOffset
	val := []byte(entry[start : start+valLen])

	return BlockEntry{key: key, value: val}
}

type BlockIter struct {
	block  Block
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
		func(entry BlockEntry, target []byte) int {
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

func (it BlockIter) Entry() BlockEntry {
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

func keyFromBytes(b []byte) []byte {
	keylen := bytesToUint16(b[:2])
	b = b[2:]
	return b[:keylen]
}
