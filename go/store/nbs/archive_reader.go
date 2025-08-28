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
	"os"

	"github.com/dolthub/gozstd"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// reconstructHashFromPrefixAndSuffix creates a hash from a prefix and suffix
func reconstructHashFromPrefixAndSuffix(prefix uint64, suffix [hash.SuffixLen]byte) hash.Hash {
	var h hash.Hash
	binary.BigEndian.PutUint64(h[:hash.PrefixLen], prefix)
	copy(h[hash.PrefixLen:], suffix[:])
	return h
}

// archiveReader is a reader for the archive format. We use primitive type slices where possible. These are read directly
// from disk into memory for speed. The downside is complexity on the read path, but it's all constant time.
type archiveReader struct {
	reader      tableReaderAt
	indexReader archiveIndexReader // Memory-mapped or fallback index reader
	dictCache   *lru.TwoQueueCache[uint32, *DecompBundle]
	footer      archiveFooter
}

type suffix [hash.SuffixLen]byte

type archiveFooter struct {
	fileSignature string
	indexSize     uint64
	fileSize      uint64 // Not actually part of the footer, but necessary for calculating offsets.
	byteSpanCount uint32
	chunkCount    uint32
	metadataSize  uint32
	dataCheckSum  sha512Sum
	indexCheckSum sha512Sum
	metaCheckSum  sha512Sum
	hash          hash.Hash
	formatVersion byte
}

// actualFooterSize returns the footer size, in bytes for a specific archive. Due to the evolution of the archive format,
// the footer size expanded in format version 3, so we need to calculate the footer size when calculating offsets
// for this instance.
func (f archiveFooter) actualFooterSize() uint64 {
	if f.formatVersion < archiveVersionGiantIndexSupport {
		// Version 1 and 2 archives have a smaller footer.
		return archiveFooterSize - 4
	}
	return archiveFooterSize
}

// dataSpan returns the span of the data section of the archive. This is not generally used directly since we usually
// read individual spans for each chunk.
func (f archiveFooter) dataSpan() byteSpan {
	return byteSpan{offset: 0, length: f.fileSize - f.actualFooterSize() - uint64(f.metadataSize) - uint64(f.indexSize)}
}

// totalIndexSpan returns the span of the entire index section of the archive.
func (f archiveFooter) totalIndexSpan() byteSpan {
	return byteSpan{offset: f.fileSize - f.actualFooterSize() - uint64(f.metadataSize) - uint64(f.indexSize), length: uint64(f.indexSize)}
}

// indexByteOffsetSpan returns the span of the byte offsets section of the index. This is the first part of the index
func (f archiveFooter) indexByteOffsetSpan() byteSpan {
	totalIdx := f.totalIndexSpan()
	return byteSpan{offset: totalIdx.offset, length: uint64(f.byteSpanCount) * uint64Size}
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
	suffixLen := uint64(f.chunkCount) * hash.SuffixLen
	chunkRefs := f.indexChunkRefSpan()
	return byteSpan{chunkRefs.offset + chunkRefs.length, suffixLen}
}

// metadataSpan returns the span of the metadata section of the archive.
func (f archiveFooter) metadataSpan() byteSpan {
	return byteSpan{offset: f.fileSize - f.actualFooterSize() - uint64(f.metadataSize), length: uint64(f.metadataSize)}
}

func newArchiveMetadata(ctx context.Context, reader tableReaderAt, name hash.Hash, fileSize uint64, stats *Stats) (*ArchiveMetadata, error) {
	aRdr, err := newArchiveReader(ctx, reader, name, fileSize, stats)
	if err != nil {
		return nil, err
	}

	if aRdr.footer.formatVersion > archiveFormatVersionMax {
		return nil, ErrInvalidFormatVersion
	}

	metaSpan := aRdr.footer.metadataSpan()
	metaRdr := newSectionReader(ctx, reader, int64(metaSpan.offset), int64(metaSpan.length), stats)

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

	snappyChunks := 0
	snappyBytes := uint64(0)
	zStdChunks := 0
	zStdBytes := uint64(0)
	seenDictIds := map[uint32]bool{}
	dictionaryBytes := uint64(0)
	idx := 0
	for idx < int(aRdr.footer.chunkCount) {
		dictId, dataId := aRdr.getChunkRef(idx)

		bs := aRdr.getByteSpanByID(dataId)
		if dictId != 0 {
			zStdBytes += bs.length
			zStdChunks += 1

			if !seenDictIds[dictId] {
				seenDictIds[dictId] = true

				bs := aRdr.getByteSpanByID(dictId)
				dictionaryBytes += bs.length
			}
		} else {
			snappyBytes += bs.length
			snappyChunks += 1
		}
		idx += 1
	}

	return &ArchiveMetadata{
		formatVersion:       int(aRdr.footer.formatVersion),
		snappyChunkCount:    snappyChunks,
		snappyBytes:         snappyBytes,
		zStdChunkCount:      zStdChunks,
		zStdBytes:           zStdBytes,
		dictionaryCount:     len(seenDictIds),
		dictionaryBytes:     dictionaryBytes,
		originalTableFileId: result[amdkOriginTableFile],
	}, nil
}

func newArchiveReaderFromFooter(ctx context.Context, reader tableReaderAt, name hash.Hash, fileSz uint64, footer []byte, stats *Stats) (archiveReader, error) {
	if uint64(len(footer)) != archiveFooterSize {
		return archiveReader{}, errors.New("runtime error: invalid footer.")
	}

	ftr, err := buildFooter(name, fileSz, footer)
	if err != nil {
		return archiveReader{}, err
	}

	return buildArchiveReader(ctx, reader, ftr, stats)
}

func newArchiveReader(ctx context.Context, reader tableReaderAt, name hash.Hash, fileSize uint64, stats *Stats) (archiveReader, error) {
	footer, err := loadFooter(ctx, reader, name, fileSize, stats)
	if err != nil {
		return archiveReader{}, fmt.Errorf("Failed to loadFooter: %w", err)
	}

	return buildArchiveReader(ctx, reader, footer, stats)
}

func buildArchiveReader(ctx context.Context, reader tableReaderAt, footer archiveFooter, stats *Stats) (archiveReader, error) {
	dictCache, err := lru.New2Q[uint32, *DecompBundle](256)
	if err != nil {
		return archiveReader{}, err
	}

	var indexRdr archiveIndexReader

	// Try to use memory mapping if the reader is a file
	if fileReader, ok := reader.(*fileReaderAt); ok && fileReader.mmapIndexes {
		indexRdr, err = newMmapIndexReader(fileReader.f, footer)
		if err != nil {
			return archiveReader{}, err
		}
	} else {
		if _, isSet := os.LookupEnv(dconfig.EnvAssertNoInMemoryArchiveIndex); isSet {
			return archiveReader{}, fmt.Errorf("attempted to load archive index into memory but %s was set", dconfig.EnvAssertNoInMemoryArchiveIndex)
		}
		indexRdr, err = newInMemoryArchiveIndexReader(ctx, reader, footer, stats)
		if err != nil {
			return archiveReader{}, err
		}
	}

	return archiveReader{
		reader:      reader,
		indexReader: indexRdr,
		footer:      footer,
		dictCache:   dictCache,
	}, nil
}

// newInMemoryArchiveIndexReader implements the original index loading logic for non-file readers
func newInMemoryArchiveIndexReader(ctx context.Context, reader tableReaderAt, footer archiveFooter, stats *Stats) (archiveIndexReader, error) {
	byteOffSpan := footer.indexByteOffsetSpan()
	secRdr := newSectionReader(ctx, reader, int64(byteOffSpan.offset), int64(byteOffSpan.length), stats)
	byteSpans := make([]uint64, footer.byteSpanCount+1)
	byteSpans[0] = 0 // Null byteSpan to simplify logic.
	err := binary.Read(secRdr, binary.BigEndian, byteSpans[1:])
	if err != nil {
		return nil, fmt.Errorf("Failed to read byte spans: %w", err)
	}

	prefixSpan := footer.indexPrefixSpan()
	prefixRdr := newSectionReader(ctx, reader, int64(prefixSpan.offset), int64(prefixSpan.length), stats)
	prefixes := make([]uint64, footer.chunkCount)
	err = binary.Read(prefixRdr, binary.BigEndian, prefixes[:])
	if err != nil {
		return nil, fmt.Errorf("Failed to read prefixes: %w", err)
	}

	chunkRefSpan := footer.indexChunkRefSpan()
	chunkRdr := newSectionReader(ctx, reader, int64(chunkRefSpan.offset), int64(chunkRefSpan.length), stats)
	chnks := make([]uint32, uint64(footer.chunkCount)*2)
	err = binary.Read(chunkRdr, binary.BigEndian, chnks[:])
	if err != nil {
		return nil, fmt.Errorf("Failed to read chunk references: %w", err)
	}

	suffixSpan := footer.indexSuffixSpan()
	sufRdr := newSectionReader(ctx, reader, int64(suffixSpan.offset), int64(suffixSpan.length), stats)
	suffixes := make([]byte, suffixSpan.length)
	_, err = io.ReadFull(sufRdr, suffixes)
	if err != nil {
		return nil, err
	}

	return &inMemoryArchiveIndexReader{
		prefixes:  prefixes,
		spanIndex: byteSpans,
		chunkRefs: chnks,
		suffixes:  suffixes,
	}, nil
}

// inMemoryArchiveIndexReader provides the original in-memory index implementation as a fallback
type inMemoryArchiveIndexReader struct {
	prefixes  []uint64
	spanIndex []uint64
	chunkRefs []uint32
	suffixes  []byte
}

func (f *inMemoryArchiveIndexReader) getNumChunks() uint32 {
	return uint32(len(f.prefixes))
}

func (f *inMemoryArchiveIndexReader) getSpanIndex(idx uint32) uint64 {
	if idx >= uint32(len(f.spanIndex)) {
		return 0
	}
	return f.spanIndex[idx]
}

func (f *inMemoryArchiveIndexReader) getPrefix(idx uint32) uint64 {
	if idx >= uint32(len(f.prefixes)) {
		return 0
	}
	return f.prefixes[idx]
}

func (f *inMemoryArchiveIndexReader) searchPrefix(prefix uint64) int32 {
	return int32(prollyBinSearch(f.prefixes, prefix))
}

func (f *inMemoryArchiveIndexReader) getChunkRef(idx uint32) (dict, data uint32) {
	if idx < 0 || idx*2+1 >= uint32(len(f.chunkRefs)) {
		return 0, 0
	}
	return f.chunkRefs[idx*2], f.chunkRefs[idx*2+1]
}

func (f *inMemoryArchiveIndexReader) getSuffix(idx uint32) suffix {
	if idx >= uint32(len(f.suffixes)/hash.SuffixLen) {
		return suffix{}
	}
	start := idx * hash.SuffixLen
	return suffix(f.suffixes[start : start+hash.SuffixLen])
}

func (f *inMemoryArchiveIndexReader) Close() error {
	return nil
}

// clone returns a new archiveReader with a new (provided) reader. All other fields are immutable or thread safe,
// so they are copied.
func (ar archiveReader) clone() (archiveReader, error) {
	reader, err := ar.reader.clone()
	if err != nil {
		return archiveReader{}, err
	}
	return archiveReader{
		reader:      reader,
		indexReader: ar.indexReader,
		footer:      ar.footer,
		dictCache:   ar.dictCache, // cache is thread safe.
	}, nil
}

type readerAtWithStatsBridge struct {
	reader ReaderAtWithStats
	ctx    context.Context
	stats  *Stats
}

func (r readerAtWithStatsBridge) ReadAt(p []byte, off int64) (int, error) {
	return r.reader.ReadAtWithStats(r.ctx, p, off, r.stats)
}

func newSectionReader(ctx context.Context, rd ReaderAtWithStats, off, len int64, stats *Stats) *io.SectionReader {
	return io.NewSectionReader(readerAtWithStatsBridge{rd, ctx, stats}, off, len)
}

func loadFooter(ctx context.Context, reader ReaderAtWithStats, name hash.Hash, fileSize uint64, stats *Stats) (f archiveFooter, err error) {
	section := newSectionReader(ctx, reader, int64(fileSize-archiveFooterSize), int64(archiveFooterSize), stats)
	buf := make([]byte, archiveFooterSize)
	_, err = io.ReadFull(section, buf)
	if err != nil {
		return
	}
	return buildFooter(name, fileSize, buf)
}

func buildFooter(name hash.Hash, fileSize uint64, buf []byte) (f archiveFooter, err error) {
	f.formatVersion = buf[afrVersionOffset]
	f.fileSignature = string(buf[afrSigOffset:])
	// Verify File Signature
	if f.fileSignature != archiveFileSignature {
		err = ErrInvalidFileSignature
		return
	}
	// Verify Format Version. 1,2,3 supported.
	if f.formatVersion > archiveFormatVersionMax {
		err = ErrInvalidFormatVersion
		return
	}

	smallFooter := false
	if f.formatVersion < archiveVersionGiantIndexSupport {
		smallFooter = true
	}

	if smallFooter {
		// Version 1 and 2 archives have a smaller footer. Ignore the first 4 bytes.
		if afrIndexLenOffset != 0 {
			// Future proofing for the event where we need to extend the footer with additional fields. This is intended
			// to blow up in development if we try and change it.
			panic("runtime error: afrIndexChkSumOffset must be 0.")
		}
		f.indexSize = uint64(binary.BigEndian.Uint32(buf[4 : 4+uint32Size]))
	} else {
		f.indexSize = binary.BigEndian.Uint64(buf[afrIndexLenOffset : afrIndexLenOffset+uint64Size])
	}

	f.byteSpanCount = binary.BigEndian.Uint32(buf[afrByteSpanOffset : afrByteSpanOffset+uint32Size])
	f.chunkCount = binary.BigEndian.Uint32(buf[afrChunkCountOffset : afrChunkCountOffset+uint32Size])
	f.metadataSize = binary.BigEndian.Uint32(buf[afrMetaLenOffset : afrMetaLenOffset+uint32Size])
	f.dataCheckSum = sha512Sum(buf[afrDataChkSumOffset : afrDataChkSumOffset+sha512.Size])
	f.indexCheckSum = sha512Sum(buf[afrIndexChkSumOffset : afrIndexChkSumOffset+sha512.Size])
	f.metaCheckSum = sha512Sum(buf[afrMetaChkSumOffset : afrMetaChkSumOffset+sha512.Size])
	f.fileSize = fileSize

	f.hash = name

	return
}

// search returns the index of the hash in the archive. If the hash is not found, -1 is returned.
func (ar archiveReader) search(hash hash.Hash) int {
	prefix := hash.Prefix()
	possibleMatch := ar.indexReader.searchPrefix(prefix)
	targetSfx := hash.Suffix()

	if possibleMatch < 0 || uint32(possibleMatch) >= ar.footer.chunkCount {
		return -1
	}

	for idx := uint32(possibleMatch); idx < ar.footer.chunkCount && ar.indexReader.getPrefix(idx) == prefix; idx++ {
		if ar.indexReader.getSuffix(idx) == suffix(targetSfx) {
			return int(idx)
		}
	}
	return -1
}

func (ar archiveReader) has(hash hash.Hash) bool {
	return ar.search(hash) >= 0
}

// get returns the decompressed data for the given hash. If the hash is not found, nil is returned (not an error)
func (ar archiveReader) get(ctx context.Context, hash hash.Hash, stats *Stats) ([]byte, error) {
	dict, data, err := ar.getRaw(ctx, hash, stats)
	if err != nil || data == nil {
		return nil, err
	}

	if dict == nil {
		if ar.footer.formatVersion >= archiveVersionSnappySupport {
			// Snappy compression format. The data is compressed with a checksum at the end.
			cc, err := NewCompressedChunk(hash, data)
			if err != nil {
				return nil, err
			}
			chk, err := cc.ToChunk()
			if err != nil {
				return nil, err
			}
			return chk.Data(), nil
		}
		return nil, errors.New("runtime error: unable to get archived chunk. dictionary is nil")
	}

	var result []byte
	result, err = gozstd.DecompressDict(nil, data, dict.dDict)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// getAsToChunker returns the chunk which is has not been decompressed. Similar to get, but with a different return type.
// If the hash is not found, a ToChunker instance with IsEmpty() == true is returned (no error)
func (ar archiveReader) getAsToChunker(ctx context.Context, h hash.Hash, stats *Stats) (ToChunker, error) {
	dict, data, err := ar.getRaw(ctx, h, stats)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return CompressedChunk{}, nil
	}

	if dict == nil {
		if ar.footer.formatVersion >= archiveVersionSnappySupport {
			cc, err := NewCompressedChunk(h, data)
			if err != nil {
				return nil, err
			}
			return cc, nil
		}
		return nil, errors.New("runtime error: unable to get archived chunk. dictionary is nil")
	}

	return ArchiveToChunker{dict, data, h}, nil
}

func (ar archiveReader) count() uint32 {
	return ar.footer.chunkCount
}

func (ar archiveReader) close() error {
	err := ar.indexReader.Close()
	if err != nil {
		return err
	}
	return ar.reader.Close()
}

// readByteSpan reads the byte span from the archive. This allocates a new byte slice and returns it to the caller.
func (ar archiveReader) readByteSpan(ctx context.Context, bs byteSpan, stats *Stats) ([]byte, error) {
	buff := make([]byte, bs.length)
	_, err := ar.reader.ReadAtWithStats(ctx, buff[:], int64(bs.offset), stats)
	if err != nil {
		return nil, err
	}
	return buff, nil
}

// getRaw returns the raw data for the given hash. If the hash is not found, nil is returned for both output, and no error.
//
// The data is returned still compressed:
// Format Version 1: Only zStd compression is supported. The data returned requires the dictionary to be decompressed.
// Format Version 2: The compression format of the data is:
//   - zStd when a dictionary is returned. The data is decompressed with the dictionary.
//   - Snappy compression when no dictionary is returned. The data has a checksum 32 bit checksum at the end. This
//     format matches the noms format.
func (ar archiveReader) getRaw(ctx context.Context, hash hash.Hash, stats *Stats) (dict *DecompBundle, data []byte, err error) {
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
			dictBytes, err := ar.readByteSpan(ctx, byteSpan, stats)
			if err != nil {
				return nil, nil, err
			}
			dict, err = NewDecompBundle(dictBytes)
			if err != nil {
				return nil, nil, err
			}

			ar.dictCache.Add(dictId, dict)
		}
	}

	byteSpan := ar.getByteSpanByID(dataId)
	data, err = ar.readByteSpan(ctx, byteSpan, stats)
	if err != nil {
		return nil, nil, err
	}
	return
}

// getChunkRef returns the dictionary and data references for the chunk at the given index. Assumes good input!
func (ar archiveReader) getChunkRef(idx int) (dict, data uint32) {
	return ar.indexReader.getChunkRef(uint32(idx))
}

// getByteSpanByID returns the byte span for the chunk at the given index. Assumes good input!
func (ar archiveReader) getByteSpanByID(id uint32) byteSpan {
	if id == 0 {
		return byteSpan{}
	}
	// This works because spanIndex[0] == 0. See initialization.
	offset := ar.indexReader.getSpanIndex(id - 1)
	length := ar.indexReader.getSpanIndex(id) - offset
	return byteSpan{offset: offset, length: length}
}

// getSuffixByID returns the suffix for the chunk at the given index. Assumes good input!
func (ar archiveReader) getSuffixByID(id uint64) suffix {
	return ar.indexReader.getSuffix(uint32(id))
}

func (ar archiveReader) getMetadata(ctx context.Context, stats *Stats) ([]byte, error) {
	return ar.readByteSpan(ctx, ar.footer.metadataSpan(), stats)
}

func (ar archiveReader) iterate(ctx context.Context, cb func(chunks.Chunk) error, stats *Stats) error {
	for i := uint32(0); i < ar.footer.chunkCount; i++ {
		prefix := ar.indexReader.getPrefix(i)
		suffix := ar.indexReader.getSuffix(i)
		h := reconstructHashFromPrefixAndSuffix(prefix, suffix)

		data, err := ar.get(ctx, h, stats)
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

func verifyCheckSum(ctx context.Context, reader tableReaderAt, span byteSpan, checkSum sha512Sum, stats *Stats) error {
	hshr := sha512.New()
	_, err := io.Copy(hshr, newSectionReader(ctx, reader, int64(span.offset), int64(span.length), stats))
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
