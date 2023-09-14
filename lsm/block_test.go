package lsm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockEntry(t *testing.T) {
	// arrange
	k := []byte("hello")
	v := []byte("world")

	// act
	entry := NewBlockEntry(k, v)
	parsedK, parsedV := entry.KeyValue()

	// assert
	assert.Equal(t, k, parsedK)
	assert.Equal(t, v, parsedV)
}

func TestBlockEntryEmptyValue(t *testing.T) {
	// arrange
	k := []byte("hello")
	v := []byte("")

	// act
	entry := NewBlockEntry(k, v)
	parsedK, parsedV := entry.KeyValue()

	// assert
	assert.Equal(t, k, parsedK)
	assert.Equal(t, v, parsedV)
}
