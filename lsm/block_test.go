package lsm

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockEntry(t *testing.T) {
	// arrange
	entry := Entry{key: []byte("hello"), value: []byte("world")}

	// act
	bytes := entry.Bytes()
	parsedEntry := EntryFromBytes(bytes)

	// assert
	assert.Equal(t, entry.key, parsedEntry.key)
	assert.Equal(t, entry.value, parsedEntry.value)
}

func TestBlockEntryEmptyValue(t *testing.T) {
	// arrange
	entry := Entry{key: []byte("hello"), value: []byte("")}

	// act
	bytes := entry.Bytes()
	parsedEntry := EntryFromBytes(bytes)

	// assert
	assert.Equal(t, entry.key, parsedEntry.key)
	assert.Equal(t, entry.value, parsedEntry.value)
}
