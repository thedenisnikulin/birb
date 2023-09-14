package lsm

import (
	"encoding/binary"
	"math"
)

// ---------------------------------------------------------------------
// |          data         |          offsets          |      meta     |
// |-----------------------|---------------------------|---------------|
// |entry|entry|entry|entry|offset|offset|offset|offset|num_of_elements|
// ---------------------------------------------------------------------
type Block struct {
	data    []byte
	offests []uint16
	len     int
	maxSize int // TODO a
}

func NewBlock(maxSize int) Block {
	return Block{
		data:    make([]byte, 0),
		offests: make([]uint16, 0),
		maxSize: maxSize,
	}
}

func (block *Block) Push(k, v []byte) {
	entry := NewBlockEntry(k, v)
	block.data = append(block.data, entry...)
	block.offests = append(block.offests, uint16(len(block.data)-1))
	block.len++
}

func (Block) Bytes() []byte {
	panic("not implemented")
}

// ???
func (Block) Checksum() []byte {
	panic("not implemented")
}

func BlockFromBytes(b []byte) (Block, error) {
	panic("not implemented")
}

// -----------------------------------------------------------------------
// |                           Entry #1                            | ... |
// -----------------------------------------------------------------------
// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
// -----------------------------------------------------------------------
type BlockEntry []byte

func NewBlockEntry(k, v []byte) BlockEntry {
	if len(k) > math.MaxUint16 || len(v) > math.MaxUint16 {
		panic("block entry key or value size is larger than max(uint16)")
	}

	if len(k) == 0 {
		panic("block entry key cannot be empty")
	}

	lenbuf := make([]byte, 2)
	entry := make([]byte, 0, 4)

	binary.LittleEndian.PutUint16(lenbuf, uint16(len(k)))
	entry = append(entry, lenbuf...)
	entry = append(entry, k...)

	binary.LittleEndian.PutUint16(lenbuf, uint16(len(v)))
	entry = append(entry, lenbuf...)
	entry = append(entry, v...)

	return BlockEntry(entry)
}

func (entry BlockEntry) KeyValue() ([]byte, []byte) {
	var keyOffset uint16 = 0
	keyLen := binary.LittleEndian.Uint16(entry[keyOffset:2])
	start := 2 + keyOffset
	key := []byte(entry[start : start+keyLen])

	valOffset := 2 + keyLen
	valLen := binary.LittleEndian.Uint16(entry[valOffset : valOffset+2])
	start = 2 + valOffset
	val := []byte(entry[start : start+valLen])

	return key, val
}

type BlockIter struct {
	block  Block
	offset uint16
}

func (BlockIter) SeekToFirst() {
	panic("not implemented")
}

func (BlockIter) SeekToOffset(offset uint16) bool {
	panic("not implemented")
}

func (BlockIter) SeekToKey(key []byte) bool {
	panic("not implemented")
}

func (BlockIter) Next() bool {
	panic("not implemented")
}

func (BlockIter) Entry() BlockEntry {
	panic("not implemented")
}
