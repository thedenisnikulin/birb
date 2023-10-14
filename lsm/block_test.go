package lsm

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockEntry(t *testing.T) {
	// arrange
	entry := Entry{Key: []byte("hello"), Value: []byte("world")}

	// act
	bytes := entry.Bytes()
	parsedEntry := EntryFromBytes(bytes)

	// assert
	assert.Equal(t, entry.Key, parsedEntry.Key)
	assert.Equal(t, entry.Value, parsedEntry.Value)
}

func TestBlockEntryEmptyValue(t *testing.T) {
	// arrange
	entry := Entry{Key: []byte("hello"), Value: []byte("")}

	// act
	bytes := entry.Bytes()
	parsedEntry := EntryFromBytes(bytes)

	// assert
	assert.Equal(t, entry.Key, parsedEntry.Key)
	assert.Equal(t, entry.Value, parsedEntry.Value)
}
