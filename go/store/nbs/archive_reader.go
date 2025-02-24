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
	"context"
	"crypto/sha512"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/bits"

	"github.com/dolthub/gozstd"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// archiveReader is a reader for the archive format. We use primitive type slices where possible. These are read directly
// from disk into memory for speed. The downside is complexity on the read path, but it's all constant time.
type archiveReader struct {
	reader    io.ReaderAt
	prefixes  []uint64
	spanIndex []uint64
	chunkRefs []uint32 // Pairs of uint32s. First is the dict id, second is the data id.
	suffixes  []byte
	footer    archiveFooter
	dictCache *lru.TwoQueueCache[uint32, *gozstd.DDict]
}

type suffix [hash.SuffixLen]byte

type archiveFooter struct {
	indexSize     uint32
	byteSpanCount uint32
	chunkCount    uint32
	metadataSize  uint32
	dataCheckSum  sha512Sum
	indexCheckSum sha512Sum
	metaCheckSum  sha512Sum
	formatVersion byte
	fileSignature string
	fileSize      uint64 // Not actually part of the footer, but necessary for calculating offsets.
	hash          hash.Hash
}

// dataSpan returns the span of the data section of the archive. This is not generally used directly since we usually
// read individual spans for each chunk.
func (f archiveFooter) dataSpan() byteSpan {
	return byteSpan{offset: 0, length: f.fileSize - archiveFooterSize - uint64(f.metadataSize) - uint64(f.indexSize)}
}

// totalIndexSpan returns the span of the entire index section of the archive. This span is not directly useful as
// the index is broken into a compressed section and an uncompressed section. Use indexCompressedSpan and indexSuffixSpan
func (f archiveFooter) totalIndexSpan() byteSpan {
	return byteSpan{offset: f.fileSize - archiveFooterSize - uint64(f.metadataSize) - uint64(f.indexSize), length: uint64(f.indexSize)}
}

// indexByteOffsetSpan returns the span of the byte offsets section of the index. This is the first part of the index
func (f archiveFooter) indexByteOffsetSpan() byteSpan {
	totalIdx := f.totalIndexSpan()
	return byteSpan{offset: totalIdx.offset, length: uint64(f.byteSpanCount * uint64Size)}
}

// indexPrefixSpan returns the span of the prefix section of the index. This is the second part of the index.
func (f archiveFooter) indexPrefixSpan() byteSpan {
	// Prefix starts after the byte spans. Length is uint64 * chunk count.
	offs := f.indexByteOffsetSpan()
	return byteSpan{offs.offset + offs.length, uint64(f.chunkCount) * uint64Size}
}

// indexChunkRefSpan returns the span of the chunk reference section of the index. This is the third part of the index.
func (f archiveFooter) indexChunkRefSpan() byteSpan {
	// chunk refs starts after the prefix. Length is (uint32 + uint32) * chunk count.
	prefixes := f.indexPrefixSpan()
	chLen := uint64(f.chunkCount) * (uint32Size + uint32Size)
	return byteSpan{prefixes.offset + prefixes.length, chLen}
}

// indexSuffixSpan returns the span of the suffix section of the index. This is the fourth part of the index.
func (f archiveFooter) indexSuffixSpan() byteSpan {
	suffixLen := uint64(f.chunkCount * hash.SuffixLen)
	chunkRefs := f.indexChunkRefSpan()
	return byteSpan{chunkRefs.offset + chunkRefs.length, suffixLen}
}

// metadataSpan returns the span of the metadata section of the archive.
func (f archiveFooter) metadataSpan() byteSpan {
	return byteSpan{offset: f.fileSize - archiveFooterSize - uint64(f.metadataSize), length: uint64(f.metadataSize)}
}

func newArchiveMetadata(reader io.ReaderAt, fileSize uint64) (*ArchiveMetadata, error) {
	footer, err := loadFooter(reader, fileSize)
	if err != nil {
		return nil, err
	}

	if footer.formatVersion != archiveFormatVersion {
		return nil, ErrInvalidFormatVersion
	}

	metaSpan := footer.metadataSpan()
	metaRdr := io.NewSectionReader(reader, int64(metaSpan.offset), int64(metaSpan.length))

	// Read the data into a byte slice
	metaData := make([]byte, metaSpan.length)
	_, err = metaRdr.Read(metaData)
	if err != nil {
		return nil, err
	}
	var result map[string]string

	// Unmarshal the JSON data into the map. TODO - use json tags.
	err = json.Unmarshal(metaData, &result)
	if err != nil {
		return nil, err
	}

	return &ArchiveMetadata{
		originalTableFileId: result[amdkOriginTableFile],
	}, nil
}

func newArchiveReader(reader io.ReaderAt, fileSize uint64) (archiveReader, error) {
	footer, err := loadFooter(reader, fileSize)
	if err != nil {
		return archiveReader{}, err
	}

	byteOffSpan := footer.indexByteOffsetSpan()
	secRdr := io.NewSectionReader(reader, int64(byteOffSpan.offset), int64(byteOffSpan.length))
	byteSpans := make([]uint64, footer.byteSpanCount+1)
	byteSpans[0] = 0 // Null byteSpan to simplify logic.
	err = binary.Read(secRdr, binary.BigEndian, byteSpans[1:])
	if err != nil {
		return archiveReader{}, err
	}

	prefixSpan := footer.indexPrefixSpan()
	prefixRdr := io.NewSectionReader(reader, int64(prefixSpan.offset), int64(prefixSpan.length))
	prefixes := make([]uint64, footer.chunkCount)
	err = binary.Read(prefixRdr, binary.BigEndian, prefixes[:])
	if err != nil {
		return archiveReader{}, err
	}

	chunkRefSpan := footer.indexChunkRefSpan()
	chunkRdr := io.NewSectionReader(reader, int64(chunkRefSpan.offset), int64(chunkRefSpan.length))
	chunks := make([]uint32, footer.chunkCount*2)
	err = binary.Read(chunkRdr, binary.BigEndian, chunks[:])
	if err != nil {
		return archiveReader{}, err
	}

	suffixSpan := footer.indexSuffixSpan()
	sufRdr := io.NewSectionReader(reader, int64(suffixSpan.offset), int64(suffixSpan.length))
	suffixes := make([]byte, footer.chunkCount*hash.SuffixLen)
	_, err = io.ReadFull(sufRdr, suffixes)
	if err != nil {
		return archiveReader{}, err
	}

	dictCache, err := lru.New2Q[uint32, *gozstd.DDict](256)
	if err != nil {
		return archiveReader{}, err
	}

	return archiveReader{
		reader:    reader,
		prefixes:  prefixes,
		spanIndex: byteSpans,
		chunkRefs: chunks,
		suffixes:  suffixes,
		footer:    footer,
		dictCache: dictCache,
	}, nil
}

// clone returns a new archiveReader with a new (provided) reader. All other fields are immutable or thread safe,
// so they are copied.
func (ar archiveReader) clone(newReader io.ReaderAt) archiveReader {
	return archiveReader{
		reader:    newReader,
		prefixes:  ar.prefixes,
		spanIndex: ar.spanIndex,
		chunkRefs: ar.chunkRefs,
		suffixes:  ar.suffixes,
		footer:    ar.footer,
		dictCache: ar.dictCache, // cache is thread safe.
	}
}

func loadFooter(reader io.ReaderAt, fileSize uint64) (f archiveFooter, err error) {
	section := io.NewSectionReader(reader, int64(fileSize-archiveFooterSize), int64(archiveFooterSize))

	buf := make([]byte, archiveFooterSize)
	_, err = io.ReadFull(section, buf)
	if err != nil {
		return
	}

	f.formatVersion = buf[afrVersionOffset]
	f.fileSignature = string(buf[afrSigOffset:])
	// Verify File Signature
	if f.fileSignature != string(archiveFileSignature) {
		err = ErrInvalidFileSignature
		return
	}
	// Verify Format Version. Currently only one version is supported, but we'll need to be more flexible in the future.
	if f.formatVersion != archiveFormatVersion {
		err = ErrInvalidFormatVersion
		return
	}

	f.indexSize = binary.BigEndian.Uint32(buf[afrIndexLenOffset : afrIndexChkSumOffset+uint32Size])
	f.byteSpanCount = binary.BigEndian.Uint32(buf[afrByteSpanOffset : afrByteSpanOffset+uint32Size])
	f.chunkCount = binary.BigEndian.Uint32(buf[afrChunkCountOffset : afrChunkCountOffset+uint32Size])
	f.metadataSize = binary.BigEndian.Uint32(buf[afrMetaLenOffset : afrMetaLenOffset+uint32Size])
	f.dataCheckSum = sha512Sum(buf[afrDataChkSumOffset : afrDataChkSumOffset+sha512.Size])
	f.indexCheckSum = sha512Sum(buf[afrIndexChkSumOffset : afrIndexChkSumOffset+sha512.Size])
	f.metaCheckSum = sha512Sum(buf[afrMetaChkSumOffset : afrMetaChkSumOffset+sha512.Size])
	f.fileSize = fileSize

	// calculate the hash of the footer. We don't currently verify that this is what was used to load the content.
	sha := sha512.New()
	sha.Write(buf)
	f.hash = hash.New(sha.Sum(nil)[:hash.ByteLen])

	return
}

// search returns the index of the hash in the archive. If the hash is not found, -1 is returned.
func (ar archiveReader) search(hash hash.Hash) int {
	prefix := hash.Prefix()
	possibleMatch := prollyBinSearch(ar.prefixes, prefix)
	targetSfx := hash.Suffix()

	if possibleMatch < 0 || possibleMatch >= len(ar.prefixes) {
		return -1
	}

	for idx := possibleMatch; idx < len(ar.prefixes) && ar.prefixes[idx] == prefix; idx++ {
		if ar.getSuffixByID(uint32(idx)) == suffix(targetSfx) {
			return idx
		}
	}
	return -1
}

func (ar archiveReader) has(hash hash.Hash) bool {
	return ar.search(hash) >= 0
}

// get returns the decompressed data for the given hash. If the hash is not found, nil is returned (not an error)
func (ar archiveReader) get(hash hash.Hash) ([]byte, error) {
	dict, data, err := ar.getRaw(hash)
	if err != nil || data == nil {
		return nil, err
	}
	if dict == nil {
		return nil, errors.New("runtime error: unable to get archived chunk. dictionary is nil")
	}

	var result []byte
	result, err = gozstd.DecompressDict(nil, data, dict)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// getAsToChunker returns the chunk which is has not been decompressed. Similar to get, but with a different return type.
// If the hash is not found, a ToChunker instance with IsEmpty() == true is returned (no error)
func (ar archiveReader) getAsToChunker(h hash.Hash) (ToChunker, error) {
	dict, data, err := ar.getRaw(h)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return ArchiveToChunker{h, nil, []byte{}}, nil
	}

	return ArchiveToChunker{h, dict, data}, nil
}

func (ar archiveReader) count() uint32 {
	return ar.footer.chunkCount
}

func (ar archiveReader) close() error {
	if closer, ok := ar.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// readByteSpan reads the byte span from the archive. This allocates a new byte slice and returns it to the caller.
func (ar archiveReader) readByteSpan(bs byteSpan) ([]byte, error) {
	buff := make([]byte, bs.length)
	_, err := ar.reader.ReadAt(buff[:], int64(bs.offset))
	if err != nil {
		return nil, err
	}
	return buff, nil
}

// getRaw returns the raw data for the given hash. If the hash is not found, nil is returned for both slices. Also,
// no error is returned in this case. Errors will only be returned if there is an io error.
//
// The data returned is still compressed, and the DDict is required to decompress it.
func (ar archiveReader) getRaw(hash hash.Hash) (dict *gozstd.DDict, data []byte, err error) {
	idx := ar.search(hash)
	if idx < 0 {
		return nil, nil, nil
	}

	dictId, dataId := ar.getChunkRef(idx)
	if dictId != 0 {
		if cached, cacheHit := ar.dictCache.Get(dictId); cacheHit {
			dict = cached
		} else {
			byteSpan := ar.getByteSpanByID(dictId)
			dictBytes, err := ar.readByteSpan(byteSpan)
			if err != nil {
				return nil, nil, err
			}
			// Dictionaries are compressed with no dictionary.
			dcmpDict, e2 := gozstd.Decompress(nil, dictBytes)
			if e2 != nil {
				return nil, nil, e2
			}

			dict, e2 = gozstd.NewDDict(dcmpDict)
			if e2 != nil {
				return nil, nil, e2
			}

			ar.dictCache.Add(dictId, dict)
		}
	}

	byteSpan := ar.getByteSpanByID(dataId)
	data, err = ar.readByteSpan(byteSpan)
	if err != nil {
		return nil, nil, err
	}
	return
}

// getChunkRef returns the dictionary and data references for the chunk at the given index. Assumes good input!
func (ar archiveReader) getChunkRef(idx int) (dict, data uint32) {
	// Chunk refs are stored as pairs of uint32s, so we need to double the index.
	idx *= 2
	return ar.chunkRefs[idx], ar.chunkRefs[idx+1]
}

// getByteSpanByID returns the byte span for the chunk at the given index. Assumes good input!
func (ar archiveReader) getByteSpanByID(id uint32) byteSpan {
	if id == 0 {
		return byteSpan{}
	}
	// This works because byteOffSpan[0] == 0. See initialization.
	offset := ar.spanIndex[id-1]
	length := ar.spanIndex[id] - offset
	return byteSpan{offset: offset, length: length}
}

// getSuffixByID returns the suffix for the chunk at the given index. Assumes good input!
func (ar archiveReader) getSuffixByID(id uint32) suffix {
	start := id * hash.SuffixLen
	return suffix(ar.suffixes[start : start+hash.SuffixLen])
}

func (ar archiveReader) getMetadata() ([]byte, error) {
	return ar.readByteSpan(ar.footer.metadataSpan())
}

// verifyDataCheckSum verifies the checksum of the data section of the archive. Note - this requires a fully read of
// the data section, which could be sizable.
func (ar archiveReader) verifyDataCheckSum() error {
	return verifyCheckSum(ar.reader, ar.footer.dataSpan(), ar.footer.dataCheckSum)
}

// verifyIndexCheckSum verifies the checksum of the index section of the archive.
func (ar archiveReader) verifyIndexCheckSum() error {
	return verifyCheckSum(ar.reader, ar.footer.totalIndexSpan(), ar.footer.indexCheckSum)
}

// verifyMetaCheckSum verifies the checksum of the metadata section of the archive.
func (ar archiveReader) verifyMetaCheckSum() error {
	return verifyCheckSum(ar.reader, ar.footer.metadataSpan(), ar.footer.metaCheckSum)
}

func (ar archiveReader) iterate(ctx context.Context, cb func(chunks.Chunk) error) error {
	for i := uint32(0); i < ar.footer.chunkCount; i++ {
		var hasBytes [hash.ByteLen]byte

		binary.BigEndian.PutUint64(hasBytes[:uint64Size], ar.prefixes[i])
		suf := ar.getSuffixByID(i)
		copy(hasBytes[hash.ByteLen-hash.SuffixLen:], suf[:])
		h := hash.New(hasBytes[:])

		data, err := ar.get(h)
		if err != nil {
			return err
		}

		chk := chunks.NewChunkWithHash(h, data)
		err = cb(chk)
		if err != nil {
			return err
		}
	}
	return nil
}

func verifyCheckSum(reader io.ReaderAt, span byteSpan, checkSum sha512Sum) error {
	hshr := sha512.New()
	_, err := io.Copy(hshr, io.NewSectionReader(reader, int64(span.offset), int64(span.length)))
	if err != nil {
		return err
	}

	if sha512Sum(hshr.Sum(nil)) != checkSum {
		return fmt.Errorf("checksum mismatch.")
	}
	return nil
}

// prollyBinSearch is a search that returns the _best_ index of the target in the input slice. If the target exists,
// one or more times, the index of the first instance is returned. If the target does not exist, the index which it
// would be inserted at is returned.
//
// A strong requirement for the proper behavior of this function is to have a sorted and well distributed slice where the
// values are not dense. Crypto hashes are a good example of this.
//
// For our purposes where we are just trying to get the index, we must compare the resulting index to our target to
// determine if it is a match.
func prollyBinSearch(slice []uint64, target uint64) int {
	items := len(slice)
	if items == 0 {
		return 0
	}
	lft, rht := 0, items
	lo, hi := slice[lft], slice[rht-1]
	if target > hi {
		return rht
	}
	if lo >= target {
		return lft
	}
	for lft < rht {
		valRangeSz := hi - lo
		idxRangeSz := uint64(rht - lft - 1)
		shiftedTgt := target - lo
		mhi, mlo := bits.Mul64(shiftedTgt, idxRangeSz)
		dU64, _ := bits.Div64(mhi, mlo, valRangeSz)
		idx := int(dU64) + lft
		if slice[idx] < target {
			lft = idx + 1
			// No need to update lo if i == items, since this loop will be ending.
			if lft < items {
				lo = slice[lft]
				// Interpolation doesn't like lo >= target, so if we're already there, just return |i|.
				if lo >= target {
					return lft
				}
			}
		} else {
			rht = idx
			hi = slice[rht]
		}
	}
	return lft
}
