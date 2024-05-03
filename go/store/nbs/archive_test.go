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
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/dolthub/gozstd"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestArchiveSingleChunk(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 1024))
	aw := newArchiveWriterWithSink(writer)
	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	bsId, err := aw.writeByteSpan(testBlob)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), bsId)
	assert.Equal(t, uint64(10), aw.bytesWritten) // 10 data bytes. No CRC or anything.

	oneHash := hashWithPrefix(t, 23)

	err = aw.stageChunk(oneHash, 0, 1)
	assert.NoError(t, err)

	err = aw.finalizeByteSpans()
	assert.NoError(t, err)

	err = aw.writeIndex()
	assert.NoError(t, err)
	// The 'uncompressed' size of the index is 23 bytes. Compressing such small data is not worth it, but we do verify
	// that the index is 35 bytes in this situation.
	assert.Equal(t, uint32(35), aw.indexLen)

	err = aw.writeMetadata([]byte(""))
	assert.NoError(t, err)

	err = aw.writeFooter()
	assert.NoError(t, err)

	assert.Equal(t, 10+35+archiveFooterSize, aw.bytesWritten) // 10 data bytes, 35 index bytes + footer

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	aIdx, err := newArchiveReader(readerAt, fileSize)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{23}, aIdx.prefixes)
	assert.True(t, aIdx.has(oneHash))

	dict, data, err := aIdx.getRaw(oneHash)
	assert.NoError(t, err)
	assert.Nil(t, dict)
	assert.Equal(t, testBlob, data)
}

func TestArchiveSingleChunkWithDictionary(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 1024))
	aw := newArchiveWriterWithSink(writer)
	testDict := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	testData := []byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	_, _ = aw.writeByteSpan(testDict)
	_, _ = aw.writeByteSpan(testData)

	h := hashWithPrefix(t, 42)
	err := aw.stageChunk(h, 1, 2)
	assert.NoError(t, err)

	aw.finalizeByteSpans()
	_ = aw.writeIndex()
	_ = aw.writeMetadata([]byte(""))
	err = aw.writeFooter()
	assert.NoError(t, err)

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	aIdx, err := newArchiveReader(readerAt, fileSize)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{42}, aIdx.prefixes)

	assert.True(t, aIdx.has(h))

	dict, data, err := aIdx.getRaw(h)
	assert.NoError(t, err)
	assert.Equal(t, testDict, dict)
	assert.Equal(t, testData, data)
}

func TestArchiverMultipleChunksMultipleDictionaries(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 1024))
	aw := newArchiveWriterWithSink(writer)
	data1 := []byte{11, 11, 11, 11, 11, 11, 11, 11, 11, 11} // span 1
	dict1 := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}           // span 2
	data2 := []byte{22, 22, 22, 22, 22, 22, 22, 22, 22, 22} // span 3
	data3 := []byte{33, 33, 33, 33, 33, 33, 33, 33, 33, 33} // span 4
	data4 := []byte{44, 44, 44, 44, 44, 44, 44, 44, 44, 44} // span 5
	dict2 := []byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}           // span 6

	h1 := hashWithPrefix(t, 42)
	id, _ := aw.writeByteSpan(data1)
	assert.Equal(t, uint32(1), id)
	_ = aw.stageChunk(h1, 0, 1)

	h2 := hashWithPrefix(t, 42)
	_, _ = aw.writeByteSpan(dict1)
	_, _ = aw.writeByteSpan(data2)
	_ = aw.stageChunk(h2, 2, 3)

	h3 := hashWithPrefix(t, 42)
	_, _ = aw.writeByteSpan(data3)
	_ = aw.stageChunk(h3, 2, 4)

	h4 := hashWithPrefix(t, 81)
	_, _ = aw.writeByteSpan(data4)
	_ = aw.stageChunk(h4, 0, 5)

	h5 := hashWithPrefix(t, 21)
	id, _ = aw.writeByteSpan(dict2)
	assert.Equal(t, uint32(6), id)
	_ = aw.stageChunk(h5, 6, 1)

	h6 := hashWithPrefix(t, 88)
	_ = aw.stageChunk(h6, 6, 1)

	h7 := hashWithPrefix(t, 42)
	_ = aw.stageChunk(h7, 2, 4)

	_ = aw.finalizeByteSpans()
	_ = aw.writeIndex()
	_ = aw.writeMetadata([]byte(""))
	_ = aw.writeFooter()

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	aIdx, err := newArchiveReader(readerAt, fileSize)
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
	assert.Equal(t, dict1, dict)
	assert.Equal(t, data3, data)

	dict, data, _ = aIdx.getRaw(h4)
	assert.Nil(t, dict)
	assert.Equal(t, data, data)

	dict, data, _ = aIdx.getRaw(h5)
	assert.Equal(t, dict2, dict)
	assert.Equal(t, data1, data)

	dict, data, _ = aIdx.getRaw(h6)
	assert.Equal(t, dict2, dict)
	assert.Equal(t, data1, data)

	dict, data, _ = aIdx.getRaw(h7)
	assert.Equal(t, dict1, dict)
	assert.Equal(t, data3, data)
}

func TestArchiveDictDecompression(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 4096))

	// This is 32K worth of data, but it's all very similar. Only fits in 4K if compressed with a dictionary.
	chks := generateSimilarChunks(42, 32)
	samples := make([][]byte, len(chks))
	for i, c := range chks {
		samples[i] = c.Data()
	}

	dict := gozstd.BuildDict(samples, 2048)
	cDict, err := gozstd.NewCDict(dict)
	assert.NoError(t, err)

	aw := newArchiveWriterWithSink(writer)

	dictId, err := aw.writeByteSpan(dict)
	for _, chk := range chks {
		cmp := gozstd.CompressDict(nil, chk.Data(), cDict)

		chId, err := aw.writeByteSpan(cmp)
		assert.NoError(t, err)

		err = aw.stageChunk(chk.Hash(), dictId, chId)
		assert.NoError(t, err)
	}
	err = aw.finalizeByteSpans()
	assert.NoError(t, err)

	err = aw.writeIndex()
	assert.NoError(t, err)

	err = aw.writeMetadata([]byte("hello world"))
	err = aw.writeFooter()
	assert.NoError(t, err)

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	aIdx, err := newArchiveReader(readerAt, fileSize)
	assert.NoError(t, err)

	// Now verify that we can look up the chunks by their original addresses, and the data is the same.
	for _, chk := range chks {
		roundTripData, err := aIdx.get(chk.Hash())
		assert.NoError(t, err)
		assert.Equal(t, chk.Data(), roundTripData)
	}
}

func TestMetadata(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 1024))
	aw := newArchiveWriterWithSink(writer)
	err := aw.finalizeByteSpans()
	assert.NoError(t, err)
	err = aw.writeIndex()
	assert.NoError(t, err)
	err = aw.writeMetadata([]byte("All work and no play"))
	assert.NoError(t, err)
	err = aw.writeFooter()
	assert.NoError(t, err)

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	rdr, err := newArchiveReader(readerAt, fileSize)
	assert.NoError(t, err)

	md, err := rdr.getMetadata()
	assert.NoError(t, err)
	assert.Equal(t, []byte("All work and no play"), md)
}

// zStd has a CRC check built into it, and it will get triggered when we
// attempt to decompress a corrupted chunk.
func TestArchiveChunkCorruption(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 1024))
	aw := newArchiveWriterWithSink(writer)
	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, _ = aw.writeByteSpan(testBlob)

	h := hashWithPrefix(t, 23)
	_ = aw.stageChunk(h, 0, 1)
	_ = aw.finalizeByteSpans()
	_ = aw.writeIndex()
	_ = aw.writeMetadata(nil)
	_ = aw.writeFooter()

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	idx, err := newArchiveReader(readerAt, fileSize)
	assert.NoError(t, err)

	// Corrupt the data
	writer.buff[3] = writer.buff[3] + 1

	data, err := idx.get(h)
	assert.ErrorContains(t, err, "cannot decompress invalid src")
	assert.Nil(t, data)
}

// Varlidate that the SHA512 checksums in the footer checkout, and fail when they are corrupted.
func TestArchiveCheckSumValidations(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 1024))
	aw := newArchiveWriterWithSink(writer)

	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, _ = aw.writeByteSpan(testBlob)

	h := hashWithPrefix(t, 23)
	_ = aw.stageChunk(h, 0, 1)
	err := aw.finalizeByteSpans()
	assert.NoError(t, err)
	err = aw.writeIndex()
	assert.NoError(t, err)
	err = aw.writeMetadata([]byte("All work and no play"))
	assert.NoError(t, err)
	err = aw.writeFooter()
	assert.NoError(t, err)

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	rdr, err := newArchiveReader(readerAt, fileSize)
	assert.NoError(t, err)

	err = rdr.verifyDataCheckSum()
	assert.NoError(t, err)
	err = rdr.verifyDataCheckSum()
	assert.NoError(t, err)
	err = rdr.verifyDataCheckSum()
	assert.NoError(t, err)

	theBytes[5] = theBytes[5] + 1
	err = rdr.verifyDataCheckSum()
	assert.ErrorContains(t, err, "checksum mismatch")

	offset := rdr.footer.indexSpan().offset + 2
	theBytes[offset] = theBytes[offset] + 1
	err = rdr.verifyIndexCheckSum()
	assert.ErrorContains(t, err, "checksum mismatch")

	offset = rdr.footer.metadataSpan().offset + 2
	theBytes[offset] = theBytes[offset] + 1
	err = rdr.verifyMetaCheckSum()
	assert.ErrorContains(t, err, "checksum mismatch")
}

func TestProllyBinSearchUneven(t *testing.T) {
	// We construct a prefix list which is not well distributed to ensure that the search still works, even if not
	// optimal.
	pf := make([]uint64, 1000)
	for i := 0; i < 900; i++ {
		pf[i] = uint64(i)
	}
	target := uint64(12345)
	pf[900] = target
	for i := 901; i < 1000; i++ {
		pf[i] = uint64(10000000 + i)
	}
	// In normal circumstances, a value of 12345 would be far to the left side of the list
	found := prollyBinSearch(pf, target)
	assert.Equal(t, 900, found)

	// Same test, but from something on the right side of the list.
	for i := 999; i > 100; i-- {
		pf[i] = uint64(math.MaxUint64 - uint64(i))
	}
	target = uint64(math.MaxUint64 - 12345)
	pf[100] = target
	for i := 99; i >= 0; i-- {
		pf[i] = uint64(10000000 - i)
	}
	found = prollyBinSearch(pf, target)
	assert.Equal(t, 100, found)
}

func TestProllyBinSearch(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	curVal := uint64(r.Int())
	pf := make([]uint64, 10000)
	for i := 0; i < 10000; i++ {
		pf[i] = curVal
		curVal += uint64(r.Intn(10))
	}

	for i := 0; i < 10000; i++ {
		idx := prollyBinSearch(pf, pf[i])
		// There are dupes in the list, so we don't always end up with the same index.
		assert.Equal(t, pf[i], pf[idx])
	}

	idx := prollyBinSearch(pf, pf[0]-1)
	assert.Equal(t, -1, idx)
	idx = prollyBinSearch(pf, pf[9999]+1)
	assert.Equal(t, -1, idx)

	// 23 is not a dupe, and neighbors don't match. stable due to seed.
	idx = prollyBinSearch(pf, pf[23]+1)
	assert.Equal(t, -1, idx)
	idx = prollyBinSearch(pf, pf[23]-1)
	assert.Equal(t, -1, idx)

}

func TestDuplicateInsertion(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 1024))
	aw := newArchiveWriterWithSink(writer)
	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, _ = aw.writeByteSpan(testBlob)

	h := hashWithPrefix(t, 23)
	_ = aw.stageChunk(h, 0, 1)
	err := aw.stageChunk(h, 0, 1)
	assert.Equal(t, ErrDuplicateChunkWritten, err)
}

func TestInsertRanges(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 1024))
	aw := newArchiveWriterWithSink(writer)
	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, _ = aw.writeByteSpan(testBlob)

	h := hashWithPrefix(t, 23)
	err := aw.stageChunk(h, 0, 2)
	assert.Equal(t, ErrInvalidChunkRange, err)

	err = aw.stageChunk(h, 2, 1)
	assert.Equal(t, ErrInvalidDictionaryRange, err)
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

// Helper functions to create test data below....
func hashWithPrefix(t *testing.T, prefix uint64) hash.Hash {
	randomBytes := make([]byte, 20)
	n, err := rand.Read(randomBytes)
	assert.Equal(t, 20, n)
	assert.NoError(t, err)

	binary.BigEndian.PutUint64(randomBytes, prefix)
	return hash.Hash(randomBytes)
}

func generateSimilarChunks(seed int64, count int) []*chunks.Chunk {
	chks := make([]*chunks.Chunk, count)
	for i := 0; i < count; i++ {
		chks[i] = generateRandomChunk(seed, 1000+i)
	}

	return chks
}

func generateRandomChunk(seed int64, len int) *chunks.Chunk {
	r := rand.NewSource(seed)

	data := make([]byte, len)
	for i := range data {
		data[i] = byte(r.Int63())
	}
	c := chunks.NewChunk(data)
	return &c
}
