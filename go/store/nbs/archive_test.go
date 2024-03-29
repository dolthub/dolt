// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nbs

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
)

func TestArchiveSingleChunk(t *testing.T) {
	writer := NewFixedBufferTableSink(make([]byte, 1024))

	aw := newArchiveWriter(writer)
	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	bsId, err := aw.writeByteSpan(testBlob)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), bsId)
	assert.Equal(t, uint64(14), aw.bytesWritten) // 14 ==  10 data bytes + CRC

	oneHash := hashWithPrefix(t, 23)

	err = aw.stageChunk(oneHash, 0, 1)
	assert.NoError(t, err)

	n, err := aw.writeIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint32(24), n) // Verified manually. A single chunk allows for single byte varints, so
	// ByteSpan -> 2 bytes, Prefix -> 8 bytes, ChunkRef -> 2 bytes, Suffix -> 12 bytes. Total 24 bytes.

	err = aw.writeFooter(n)
	assert.NoError(t, err)

	assert.Equal(t, uint64(58), aw.bytesWritten) // 14 + 24 + 20 (footer is 20 bytes)

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	aIdx, err := newArchiveIndex(readerAt, fileSize)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{23}, aIdx.prefixes)
	assert.True(t, aIdx.has(oneHash))

	dict, data, err := aIdx.getRaw(oneHash)
	assert.NoError(t, err)
	assert.Nil(t, dict)
	assert.Equal(t, testBlob, data)
}

func TestArchiveSingleChunkWithDictionary(t *testing.T) {
	writer := NewFixedBufferTableSink(make([]byte, 1024))

	aw := newArchiveWriter(writer)
	testDict := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	testData := []byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	_, _ = aw.writeByteSpan(testDict)
	_, _ = aw.writeByteSpan(testData)

	h := hashWithPrefix(t, 42)
	err := aw.stageChunk(h, 1, 2)
	assert.NoError(t, err)

	n, _ := aw.writeIndex()
	err = aw.writeFooter(n)
	assert.NoError(t, err)

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	aIdx, err := newArchiveIndex(readerAt, fileSize)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{42}, aIdx.prefixes)

	assert.True(t, aIdx.has(h))

	dict, data, err := aIdx.getRaw(h)
	assert.NoError(t, err)
	assert.Equal(t, testDict, dict)
	assert.Equal(t, testData, data)
}

func TestArchiverMultipleChunksMultipleDictionaries(t *testing.T) {
	writer := NewFixedBufferTableSink(make([]byte, 1024))

	aw := newArchiveWriter(writer)
	dict1 := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}           // span 1
	dict2 := []byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}           // span 2
	dict3 := []byte{3, 3, 3, 3, 3, 3, 3, 3, 3, 3}           // span 3
	dict4 := []byte{4, 4, 4, 4, 4, 4, 4, 4, 4, 4}           // span 4
	data1 := []byte{11, 11, 11, 11, 11, 11, 11, 11, 11, 11} // span 5
	data2 := []byte{22, 22, 22, 22, 22, 22, 22, 22, 22, 22} // span 6
	data3 := []byte{33, 33, 33, 33, 33, 33, 33, 33, 33, 33} // span 7
	data4 := []byte{44, 44, 44, 44, 44, 44, 44, 44, 44, 44} // span 8

	_, _ = aw.writeByteSpan(dict1)
	_, _ = aw.writeByteSpan(dict2)
	_, _ = aw.writeByteSpan(dict3)
	_, _ = aw.writeByteSpan(dict4)
	_, _ = aw.writeByteSpan(data1)
	_, _ = aw.writeByteSpan(data2)
	_, _ = aw.writeByteSpan(data3)
	_, _ = aw.writeByteSpan(data4)

	h1 := hashWithPrefix(t, 42)
	h2 := hashWithPrefix(t, 42)
	h3 := hashWithPrefix(t, 42)
	h4 := hashWithPrefix(t, 81)
	h5 := hashWithPrefix(t, 21)
	h6 := hashWithPrefix(t, 88)
	h7 := hashWithPrefix(t, 42)

	_ = aw.stageChunk(h1, 0, 5)
	_ = aw.stageChunk(h2, 1, 6)
	_ = aw.stageChunk(h3, 2, 7)
	_ = aw.stageChunk(h4, 3, 8)
	_ = aw.stageChunk(h5, 1, 5)
	_ = aw.stageChunk(h6, 0, 6)
	_ = aw.stageChunk(h7, 1, 7)

	n, _ := aw.writeIndex()
	_ = aw.writeFooter(n)

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	aIdx, err := newArchiveIndex(readerAt, fileSize)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{21, 42, 42, 42, 42, 81, 88}, aIdx.prefixes)

	assert.True(t, aIdx.has(h1))
	assert.True(t, aIdx.has(h2))
	assert.True(t, aIdx.has(h3))
	assert.True(t, aIdx.has(h4))
	assert.True(t, aIdx.has(h5))
	assert.True(t, aIdx.has(h6))
	assert.True(t, aIdx.has(h7))
	assert.False(t, aIdx.has(hash.Hash{}))
	assert.False(t, aIdx.has(hashWithPrefix(t, 42)))
	assert.False(t, aIdx.has(hashWithPrefix(t, 55)))

	dict, data, _ := aIdx.getRaw(h1)
	assert.Nil(t, dict)
	assert.Equal(t, data1, data)

	dict, data, _ = aIdx.getRaw(h2)
	assert.Equal(t, dict1, dict)
	assert.Equal(t, data2, data)

	dict, data, _ = aIdx.getRaw(h3)
	assert.Equal(t, dict2, dict)
	assert.Equal(t, data3, data)

	dict, data, _ = aIdx.getRaw(h4)
	assert.Equal(t, dict3, dict)
	assert.Equal(t, data4, data)

	dict, data, _ = aIdx.getRaw(h5)
	assert.Equal(t, dict1, dict)
	assert.Equal(t, data1, data)

	dict, data, _ = aIdx.getRaw(h6)
	assert.Nil(t, dict)
	assert.Equal(t, data2, data)

	dict, data, _ = aIdx.getRaw(h7)
	assert.Equal(t, dict1, dict)
	assert.Equal(t, data3, data)
}

func TestArchiveBlockCorruption(t *testing.T) {
	writer := NewFixedBufferTableSink(make([]byte, 1024))
	aw := newArchiveWriter(writer)
	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, _ = aw.writeByteSpan(testBlob)

	h := hashWithPrefix(t, 23)
	_ = aw.stageChunk(h, 0, 1)

	n, _ := aw.writeIndex()
	_ = aw.writeFooter(n)

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	idx, err := newArchiveIndex(readerAt, fileSize)
	assert.NoError(t, err)

	// Corrupt the data
	theBytes[3] = 51

	dict, data, err := idx.getRaw(h)
	assert.Equal(t, ErrCRCMismatch, err)
	assert.Nil(t, dict)
	assert.Nil(t, data)
}

func TestPrefixSearch(t *testing.T) {
	pf := []uint64{2, 3, 4, 4, 4, 5, 6, 7, 10, 10, 11, 12, 13}

	assert.Equal(t, []int{}, findMatchingPrefixes(pf, 1))
	assert.Equal(t, []int{0}, findMatchingPrefixes(pf, 2))
	assert.Equal(t, []int{2, 3, 4}, findMatchingPrefixes(pf, 4))
	assert.Equal(t, []int{}, findMatchingPrefixes(pf, 8))
	assert.Equal(t, []int{8, 9}, findMatchingPrefixes(pf, 10))
	assert.Equal(t, []int{12}, findMatchingPrefixes(pf, 13))
	assert.Equal(t, []int{}, findMatchingPrefixes(pf, 14))

	pf = []uint64{}
	assert.Equal(t, []int{}, findMatchingPrefixes(pf, 42))

	pf = []uint64{23, 23, 23}
	assert.Equal(t, []int{0, 1, 2}, findMatchingPrefixes(pf, 23))
	assert.Equal(t, []int{}, findMatchingPrefixes(pf, 24)) // Don't run off the end is busted ways.
	assert.Equal(t, []int{}, findMatchingPrefixes(pf, 22))
}

func hashWithPrefix(t *testing.T, prefix uint64) hash.Hash {
	randomBytes := make([]byte, 20)
	n, err := rand.Read(randomBytes)
	assert.Equal(t, 20, n)
	assert.NoError(t, err)

	binary.BigEndian.PutUint64(randomBytes, prefix)
	return hash.Hash(randomBytes)
}
