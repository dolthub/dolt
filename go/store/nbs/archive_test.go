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
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand"
	"testing"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/gozstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// There are many tests which don't actually use the dictionary to compress. But some dictionary is required, so
// we'll use this one.
var defaultDict []byte
var defaultCDict *gozstd.CDict

func init() {
	defaultDict, defaultCDict = generateTerribleDefaultDictionary()
}

func TestArchiveSingleChunk(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 4096))
	aw := newArchiveWriterWithSink(writer)

	dId, err := aw.writeByteSpan(defaultDict)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), dId)

	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	bsId, err := aw.writeByteSpan(testBlob)
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), bsId)

	dataSz := uint64(len(defaultDict)) + uint64(len(testBlob))
	assert.Equal(t, dataSz, aw.bytesWritten)

	oneHash := hashWithPrefix(t, 23)

	err = aw.stageZStdChunk(oneHash, dId, bsId)
	assert.NoError(t, err)

	err = aw.finalizeByteSpans()
	assert.NoError(t, err)

	err = aw.writeIndex()
	assert.NoError(t, err)
	// Index size is not deterministic from the number of chunks, but when 1 dictionary and one chunk are in play, 44 bytes is correct:
	// [SpanIndex - two ByteSpans]   [Prefix Map]   [chunk ref ]    [hash.Suffix --]
	//        16 (2 uint64s)       + 8 (1 uint64) + 8 (2 uint32s) + 12                = ___44___
	assert.Equal(t, uint32(44), aw.indexLen)

	err = aw.writeMetadata([]byte(""))
	assert.NoError(t, err)

	err = aw.writeFooter()
	assert.NoError(t, err)

	assert.Equal(t, dataSz+44+archiveFooterSize, aw.bytesWritten)

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	tra := tableReaderAtAdapter{readerAt}

	aIdx, err := newArchiveReader(context.Background(), tra, fileSize, &Stats{})
	assert.NoError(t, err)

	assert.Equal(t, []uint64{23}, aIdx.prefixes)
	assert.True(t, aIdx.has(oneHash))

	dict, data, err := aIdx.getRaw(context.Background(), oneHash, &Stats{})
	assert.NoError(t, err)
	assert.NotNil(t, dict)
	assert.Equal(t, testBlob, data)
}

func TestArchiveSingleChunkWithDictionary(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 4096))
	aw := newArchiveWriterWithSink(writer)
	testData := []byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	_, _ = aw.writeByteSpan(defaultDict)
	_, _ = aw.writeByteSpan(testData)

	h := hashWithPrefix(t, 42)
	err := aw.stageZStdChunk(h, 1, 2)
	assert.NoError(t, err)

	_ = aw.finalizeByteSpans()
	_ = aw.writeIndex()
	_ = aw.writeMetadata([]byte(""))
	err = aw.writeFooter()
	assert.NoError(t, err)

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	tra := tableReaderAtAdapter{readerAt}
	aIdx, err := newArchiveReader(context.Background(), tra, fileSize, &Stats{})
	assert.NoError(t, err)
	assert.Equal(t, []uint64{42}, aIdx.prefixes)

	assert.True(t, aIdx.has(h))

	dict, data, err := aIdx.getRaw(context.Background(), h, &Stats{})
	assert.NoError(t, err)
	assert.NotNil(t, dict)
	assert.Equal(t, testData, data)
}

func TestArchiverMultipleChunksMultipleDictionaries(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 4096))
	aw := newArchiveWriterWithSink(writer)

	data1 := []byte{11, 11, 11, 11, 11, 11, 11, 11, 11, 11} // span 2
	dict1, _ := generateDictionary(1)                       // span 3
	data2 := []byte{22, 22, 22, 22, 22, 22, 22, 22, 22, 22} // span 4
	data3 := []byte{33, 33, 33, 33, 33, 33, 33, 33, 33, 33} // span 5
	data4 := []byte{44, 44, 44, 44, 44, 44, 44, 44, 44, 44} // span 6
	dict2, _ := generateDictionary(2)                       // span 7

	id, _ := aw.writeByteSpan(defaultDict)
	assert.Equal(t, uint32(1), id)

	h1 := hashWithPrefix(t, 42)
	id, _ = aw.writeByteSpan(data1)
	assert.Equal(t, uint32(2), id)
	_ = aw.stageZStdChunk(h1, 1, 2)

	h2 := hashWithPrefix(t, 42)
	id, _ = aw.writeByteSpan(dict1)
	assert.Equal(t, uint32(3), id)
	id, _ = aw.writeByteSpan(data2)
	assert.Equal(t, uint32(4), id)
	_ = aw.stageZStdChunk(h2, 3, 4)

	h3 := hashWithPrefix(t, 42)
	id, _ = aw.writeByteSpan(data3)
	assert.Equal(t, uint32(5), id)
	_ = aw.stageZStdChunk(h3, 3, 5)

	h4 := hashWithPrefix(t, 81)
	id, _ = aw.writeByteSpan(data4)
	assert.Equal(t, uint32(6), id)
	_ = aw.stageZStdChunk(h4, 1, 6)

	h5 := hashWithPrefix(t, 21)
	id, _ = aw.writeByteSpan(dict2)
	assert.Equal(t, uint32(7), id)
	_ = aw.stageZStdChunk(h5, 7, 2)

	h6 := hashWithPrefix(t, 88)
	_ = aw.stageZStdChunk(h6, 7, 2)

	h7 := hashWithPrefix(t, 42)
	_ = aw.stageZStdChunk(h7, 3, 5)

	_ = aw.finalizeByteSpans()
	_ = aw.writeIndex()
	_ = aw.writeMetadata([]byte(""))
	_ = aw.writeFooter()

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	tra := tableReaderAtAdapter{readerAt}
	aIdx, err := newArchiveReader(context.Background(), tra, fileSize, &Stats{})
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

	c := context.Background()
	s := &Stats{}

	dict, data, _ := aIdx.getRaw(c, h1, s)
	assert.NotNil(t, dict)
	assert.Equal(t, data1, data)

	dict, data, _ = aIdx.getRaw(c, h2, s)
	assert.NotNil(t, dict)
	assert.Equal(t, data2, data)

	dict, data, _ = aIdx.getRaw(c, h3, s)
	assert.NotNil(t, dict)
	assert.Equal(t, data3, data)

	dict, data, _ = aIdx.getRaw(c, h4, s)
	assert.NotNil(t, dict)
	assert.Equal(t, data, data)

	dict, data, _ = aIdx.getRaw(c, h5, s)
	assert.NotNil(t, dict)
	assert.Equal(t, data1, data)

	dict, data, _ = aIdx.getRaw(c, h6, s)
	assert.NotNil(t, dict)
	assert.Equal(t, data1, data)

	dict, data, _ = aIdx.getRaw(c, h7, s)
	assert.NotNil(t, dict)
	assert.Equal(t, data3, data)
}

func TestArchiveDictDecompression(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 4096))

	// This is 32K worth of data, but it's all very similar. Only fits in 4K if compressed with a dictionary.
	chks, _, _ := generateSimilarChunks(42, 32)
	samples := make([][]byte, len(chks))
	for i, c := range chks {
		samples[i] = c.Data()
	}

	dict := gozstd.BuildDict(samples, 2048)
	cDict, err := gozstd.NewCDict(dict)
	assert.NoError(t, err)

	aw := newArchiveWriterWithSink(writer)

	cmpDict := gozstd.Compress(nil, dict)
	dictId, err := aw.writeByteSpan(cmpDict)
	for _, chk := range chks {
		cmp := gozstd.CompressDict(nil, chk.Data(), cDict)

		chId, err := aw.writeByteSpan(cmp)
		assert.NoError(t, err)

		err = aw.stageZStdChunk(chk.Hash(), dictId, chId)
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
	tra := tableReaderAtAdapter{readerAt}
	aIdx, err := newArchiveReader(context.Background(), tra, fileSize, &Stats{})
	assert.NoError(t, err)

	c := context.Background()
	s := &Stats{}

	// Now verify that we can look up the chunks by their original addresses, and the data is the same.
	for _, chk := range chks {
		roundTripData, err := aIdx.get(c, chk.Hash(), s)
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
	tra := tableReaderAtAdapter{readerAt}
	rdr, err := newArchiveReader(context.Background(), tra, fileSize, &Stats{})
	assert.NoError(t, err)

	md, err := rdr.getMetadata(context.Background(), &Stats{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("All work and no play"), md)
}

// zStd has a CRC check built into it, and it will get triggered when we
// attempt to decompress a corrupted chunk.
func TestArchiveChunkCorruption(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 4096))
	aw := newArchiveWriterWithSink(writer)

	_, _ = aw.writeByteSpan(defaultDict)

	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, _ = aw.writeByteSpan(testBlob)

	h := hashWithPrefix(t, 23)
	_ = aw.stageZStdChunk(h, 1, 2)
	_ = aw.finalizeByteSpans()
	_ = aw.writeIndex()
	_ = aw.writeMetadata(nil)
	_ = aw.writeFooter()

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	tra := tableReaderAtAdapter{readerAt}
	idx, err := newArchiveReader(context.Background(), tra, fileSize, &Stats{})
	assert.NoError(t, err)

	// Corrupt the data
	writer.buff[len(defaultDict)+3] = writer.buff[len(defaultDict)+3] + 1

	data, err := idx.get(context.Background(), h, &Stats{})
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
	_ = aw.stageZStdChunk(h, 0, 1)
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
	tra := tableReaderAtAdapter{readerAt}
	rdr, err := newArchiveReader(context.Background(), tra, fileSize, &Stats{})
	assert.NoError(t, err)

	err = rdr.verifyDataCheckSum(context.Background(), &Stats{})
	assert.NoError(t, err)
	err = rdr.verifyIndexCheckSum(context.Background(), &Stats{})
	assert.NoError(t, err)
	err = rdr.verifyMetaCheckSum(context.Background(), &Stats{})
	assert.NoError(t, err)

	theBytes[5] = theBytes[5] + 1
	err = rdr.verifyDataCheckSum(context.Background(), &Stats{})
	assert.ErrorContains(t, err, "checksum mismatch")

	offset := rdr.footer.totalIndexSpan().offset + 2
	theBytes[offset] = theBytes[offset] + 1
	err = rdr.verifyIndexCheckSum(context.Background(), &Stats{})
	assert.ErrorContains(t, err, "checksum mismatch")

	offset = rdr.footer.metadataSpan().offset + 2
	theBytes[offset] = theBytes[offset] + 1
	err = rdr.verifyMetaCheckSum(context.Background(), &Stats{})
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
	assert.Equal(t, 0, idx)
	idx = prollyBinSearch(pf, pf[9999]+1)
	assert.Equal(t, 10000, idx)

	// 23 is not a dupe, and neighbors don't match. stable due to seed.
	idx = prollyBinSearch(pf, pf[23]+1)
	assert.Equal(t, 24, idx)
	idx = prollyBinSearch(pf, pf[23]-1)
	assert.Equal(t, 23, idx)

}

func TestDictionaryRangeError(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 1024))
	aw := newArchiveWriterWithSink(writer)
	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, _ = aw.writeByteSpan(testBlob)
	h := hashWithPrefix(t, 23)
	err := aw.stageZStdChunk(h, 0, 1)
	assert.Equal(t, ErrInvalidDictionaryRange, err)
}

func TestDuplicateInsertion(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 4096))
	aw := newArchiveWriterWithSink(writer)

	_, _ = aw.writeByteSpan(defaultDict)

	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, _ = aw.writeByteSpan(testBlob)

	h := hashWithPrefix(t, 23)
	_ = aw.stageZStdChunk(h, 1, 2)
	err := aw.stageZStdChunk(h, 1, 2)
	assert.Equal(t, ErrDuplicateChunkWritten, err)
}

func TestInsertRanges(t *testing.T) {
	writer := NewFixedBufferByteSink(make([]byte, 1024))
	aw := newArchiveWriterWithSink(writer)

	_, _ = aw.writeByteSpan(defaultDict)

	testBlob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, _ = aw.writeByteSpan(testBlob)

	h := hashWithPrefix(t, 23)
	err := aw.stageZStdChunk(h, 1, 3)
	assert.Equal(t, ErrInvalidChunkRange, err)

	err = aw.stageZStdChunk(h, 0, 1)
	assert.Equal(t, ErrInvalidDictionaryRange, err)

	err = aw.stageZStdChunk(h, 1, 0)
	assert.Equal(t, ErrInvalidChunkRange, err)

	err = aw.stageZStdChunk(h, 4, 1)
	assert.Equal(t, ErrInvalidDictionaryRange, err)
}

func TestFooterVersionAndSignature(t *testing.T) {
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
	tra := tableReaderAtAdapter{readerAt}
	rdr, err := newArchiveReader(context.Background(), tra, fileSize, &Stats{})
	assert.NoError(t, err)

	assert.Equal(t, archiveFormatVersionMax, rdr.footer.formatVersion)
	assert.Equal(t, archiveFileSignature, rdr.footer.fileSignature)

	// Corrupt the version
	theBytes[fileSize-archiveFooterSize+afrVersionOffset] = 23
	readerAt = bytes.NewReader(theBytes)
	tra = tableReaderAtAdapter{readerAt}
	_, err = newArchiveReader(context.Background(), tra, fileSize, &Stats{})
	assert.ErrorContains(t, err, "invalid format version")

	// Corrupt the signature, but first restore the version.
	theBytes[fileSize-archiveFooterSize+afrVersionOffset] = archiveFormatVersionMax
	theBytes[fileSize-archiveFooterSize+afrSigOffset+2] = 'X'
	readerAt = bytes.NewReader(theBytes)
	tra = tableReaderAtAdapter{readerAt}
	_, err = newArchiveReader(context.Background(), tra, fileSize, &Stats{})
	assert.ErrorContains(t, err, "invalid file signature")
}

func TestChunkRelations(t *testing.T) {
	cr := NewChunkRelations()
	assert.Equal(t, 0, cr.Count())

	h1 := hashWithPrefix(t, 1)
	h2 := hashWithPrefix(t, 2)
	h3 := hashWithPrefix(t, 3)
	h4 := hashWithPrefix(t, 4)
	h5 := hashWithPrefix(t, 5)
	h6 := hashWithPrefix(t, 6)
	h7 := hashWithPrefix(t, 7)

	cr.Add(h1, h2)
	assert.Equal(t, 2, cr.Count())
	assert.Equal(t, 1, len(cr.groups()))

	cr.Add(h3, h4)
	assert.Equal(t, 4, cr.Count())
	assert.Equal(t, 2, len(cr.groups()))

	cr.Add(h5, h6)
	assert.Equal(t, 6, cr.Count())
	assert.Equal(t, 3, len(cr.groups()))

	// restart.
	cr = NewChunkRelations()

	cr.Add(h1, h2)
	assert.Equal(t, 2, cr.Count())
	assert.Equal(t, 1, len(cr.groups()))

	cr.Add(h2, h3)
	assert.Equal(t, 3, cr.Count())
	assert.Equal(t, 1, len(cr.groups()))

	cr.Add(h2, h3) // Adding again should have no effect.
	assert.Equal(t, 3, cr.Count())
	assert.Equal(t, 1, len(cr.groups()))

	cr.Add(h4, h5) // New group.
	assert.Equal(t, 5, cr.Count())
	assert.Equal(t, 2, len(cr.groups()))

	cr.Add(h6, h7) // New group.
	assert.Equal(t, 7, cr.Count())
	assert.Equal(t, 3, len(cr.groups()))

	cr.Add(h1, h7) // Merging groups.
	assert.Equal(t, 7, cr.Count())
	assert.Equal(t, 2, len(cr.groups()))

	cr.Add(h2, h5) // Another merge into one mega group.
	assert.Equal(t, 7, cr.Count())
	assert.Equal(t, 1, len(cr.groups()))
}

func TestArchiveChunkGroup(t *testing.T) {
	// This test has a lot of magic numbers. They have been verified at the time of writing, and heavily
	// depend on the random data generated. If the random data generation changes, these numbers will
	//
	// There is also some non-determinism in the compression library itself, so the compression ratios are compared against
	// ranges we've seen over several runs of the tests.
	var stats Stats
	_, cache, hs := generateSimilarChunks(42, 10)
	cg, err := newChunkGroup(context.TODO(), cache, hs, defaultCDict, &stats)
	require.NoError(t, err)
	assertFloatBetween(t, cg.totalRatioWDict, 0.86, 0.87)
	assertIntBetween(t, cg.totalBytesSavedWDict, 8690, 8720)
	assertIntBetween(t, cg.avgRawChunkSize, 1004, 1005)

	unsimilar := generateRandomChunk(23, 980) // 20 bytes shorter to effect the average size.
	v, err := cg.testChunk(unsimilar)
	assert.NoError(t, err)
	assert.False(t, v)
	// Adding unsimilar chunk should change the ratio significantly downward. Doing this mainly to ensure the next
	// chunk tests positive because of it's high similarity.
	addChunkToCache(cache, unsimilar)
	err = cg.addChunk(context.TODO(), cache, unsimilar, defaultCDict, &stats)
	assert.NoError(t, err)
	assertFloatBetween(t, cg.totalRatioWDict, 0.78, 0.81)
	assertIntBetween(t, cg.totalBytesSavedWDict, 8650, 8700)
	assertIntBetween(t, cg.avgRawChunkSize, 990, 1010)

	similar := generateRandomChunk(42, 980)
	v, err = cg.testChunk(similar)
	assert.NoError(t, err)
	assert.True(t, v)

	addChunkToCache(cache, similar)
	err = cg.addChunk(context.TODO(), cache, similar, defaultCDict, &stats)
	assert.NoError(t, err)
	assertFloatBetween(t, cg.totalRatioWDict, 0.80, 0.81)
	assertIntBetween(t, cg.totalBytesSavedWDict, 9650, 9700)
	assertIntBetween(t, cg.avgRawChunkSize, 990, 1010)
}

func assertFloatBetween(t *testing.T, actual, min, max float64) {
	if actual < min || actual > max {
		t.Errorf("Expected %f to be between %f and %f", actual, min, max)
	}
}

func assertIntBetween(t *testing.T, actual, min, max int) {
	if actual < min || actual > max {
		t.Errorf("Expected %d to be between %d and %d", actual, min, max)
	}
}

// Helper functions to create test data below...
func hashWithPrefix(t *testing.T, prefix uint64) hash.Hash {
	randomBytes := make([]byte, 20)
	n, err := rand.Read(randomBytes)
	assert.Equal(t, 20, n)
	assert.NoError(t, err)

	binary.BigEndian.PutUint64(randomBytes, prefix)
	return hash.Hash(randomBytes)
}

// Most tests need a test dictionary. We generate a terrible one because we don't care about the actual compression.
// We return both the raw form and the CDict form.
func generateTerribleDefaultDictionary() ([]byte, *gozstd.CDict) {
	return generateDictionary(1977)
}

func generateDictionary(seed int64) ([]byte, *gozstd.CDict) {
	chks, _, _ := generateSimilarChunks(seed, 10)
	rawDict := buildDictionary(chks)
	cDict, _ := gozstd.NewCDict(rawDict)
	rawDict = gozstd.Compress(nil, rawDict)
	return rawDict, cDict
}

func addChunkToCache(cache *simpleChunkSourceCache, chk *chunks.Chunk) {
	internal, _ := cache.cs.(*testChunkSource)
	internal.chunks = append(internal.chunks, chk)
}

func generateSimilarChunks(seed int64, count int) ([]*chunks.Chunk, *simpleChunkSourceCache, hash.HashSet) {
	chks := make([]*chunks.Chunk, count)
	for i := 0; i < count; i++ {
		chks[i] = generateRandomChunk(seed, 1000+i)
	}

	c, hs := buildTestChunkSource(chks)
	return chks, c, hs
}

func generateRandomChunk(seed int64, len int) *chunks.Chunk {
	c := chunks.NewChunk(generateRandomBytes(seed, len))
	return &c
}

func generateRandomBytes(seed int64, len int) []byte {
	r := rand.NewSource(seed)

	data := make([]byte, len)
	for i := range data {
		data[i] = byte(r.Int63())
	}

	return data
}

func buildTestChunkSource(chks []*chunks.Chunk) (*simpleChunkSourceCache, hash.HashSet) {
	cpy := make([]*chunks.Chunk, len(chks))
	copy(cpy, chks)
	tcs := &testChunkSource{chunks: cpy}
	hs := hash.NewHashSet()
	for _, chk := range cpy {
		hs.Insert(chk.Hash())
	}
	cache, _ := newSimpleChunkSourceCache(tcs)
	return cache, hs
}

type testChunkSource struct {
	chunks []*chunks.Chunk
}

var _ chunkSource = (*testChunkSource)(nil)

func (tcs *testChunkSource) get(_ context.Context, h hash.Hash, _ keeperF, _ *Stats) ([]byte, gcBehavior, error) {
	for _, chk := range tcs.chunks {
		if chk.Hash() == h {
			return chk.Data(), gcBehavior_Continue, nil
		}
	}
	return nil, gcBehavior_Continue, errors.New("not found")
}

func (tcs *testChunkSource) has(h hash.Hash, keeper keeperF) (bool, gcBehavior, error) {
	panic("never used")
}

func (tcs *testChunkSource) hasMany(addrs []hasRecord, keeper keeperF) (bool, gcBehavior, error) {
	panic("never used")
}

func (tcs *testChunkSource) getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	panic("never used")
}

func (tcs *testChunkSource) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, ToChunker), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	panic("never used")
}

func (tcs *testChunkSource) count() (uint32, error) {
	panic("never used")
}

func (tcs *testChunkSource) uncompressedLen() (uint64, error) {
	panic("never used")
}

func (tcs *testChunkSource) close() error {
	panic("never used")
}

func (tcs *testChunkSource) hash() hash.Hash {
	panic("never used")
}

func (tcs *testChunkSource) suffix() string {
	panic("never used")
}

func (tcs *testChunkSource) reader(ctx context.Context) (io.ReadCloser, uint64, error) {
	panic("never used")
}

func (tcs *testChunkSource) getRecordRanges(ctx context.Context, requests []getRecord, keeper keeperF) (map[hash.Hash]Range, gcBehavior, error) {
	panic("never used")
}

func (tcs *testChunkSource) index() (tableIndex, error) {
	panic("never used")
}

func (tcs *testChunkSource) clone() (chunkSource, error) {
	panic("never used")
}

func (tcs *testChunkSource) currentSize() uint64 {
	panic("never used")
}

func (tcs *testChunkSource) iterateAllChunks(_ context.Context, _ func(chunks.Chunk), _ *Stats) error {
	panic("never used")
}
