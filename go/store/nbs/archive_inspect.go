// Copyright 2025 Dolthub, Inc.
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
	"encoding/binary"
	"fmt"

	"github.com/dolthub/dolt/go/store/hash"
)

// ByteSpanInfo provides information about a byte span in the archive
type ByteSpanInfo struct {
	Offset uint64
	Length uint64
}

// ChunkInfo contains information about a chunk within the archive
type ChunkInfo struct {
	CompressionType    string
	DictionaryID       uint32
	DataID             uint32
	DictionaryByteSpan ByteSpanInfo
	DataByteSpan       ByteSpanInfo
}

// SearchDebugInfo contains detailed information about a chunk search operation
type SearchDebugInfo struct {
	Hash            string
	Prefix          uint64
	Suffix          []byte
	PossibleMatch   int32
	ChunkCount      uint32
	IndexReaderType string
	ValidRange      bool
	FinalResult     int
	Matches         []PrefixMatch
}

// PrefixMatch contains information about a chunk that matches the search prefix
type PrefixMatch struct {
	Index       uint32
	SuffixAtIdx []byte
	SuffixMatch bool
}

// IndexReaderDetails contains detailed information about index reader state
type IndexReaderDetails struct {
	IndexReaderType string
	RequestedIndex  uint32
	ChunkCount      uint32
	ByteSpanCount   uint32
	Error           string
	Hash            string
	Prefix          uint64
	Suffix          []byte
	DictionaryID    uint32
	DataID          uint32

	// In-memory reader specific fields
	PrefixArrayLength    int
	SuffixArrayLength    int
	ChunkRefArrayLength  int
	SpanIndexArrayLength int
	ExpectedSuffixStart  uint32
	ExpectedSuffixEnd    uint32
	SuffixArrayBounds    bool
	RawSuffixBytes       []byte

	// Memory-mapped reader specific fields
	MmapIndexSize       uint64
	MmapByteSpanCount   uint32
	MmapChunkCount      uint32
	SpanIndexOffset     uint64
	PrefixesOffset      uint64
	ChunkRefsOffset     uint64
	SuffixesOffset      uint64
	ActualSuffixOffset  uint64
	RawSuffixBytesError string
}

// ArchiveInspector provides a way to inspect archive files from outside the nbs package. Intended for debugging and inspection,
// currently only used by the `dolt admin archive-inspect` command.
type ArchiveInspector struct {
	reader archiveReader
}

// NewArchiveInspectorFromFileWithMmap creates an ArchiveInspector from a file path with configurable mmap
func NewArchiveInspectorFromFileWithMmap(ctx context.Context, archivePath string, q MemoryQuotaProvider, enableMmap bool) (*ArchiveInspector, error) {
	fra, err := newFileReaderAt(archivePath, enableMmap)
	if err != nil {
		return nil, err
	}

	// Use a dummy hash since it's not needed when we have the file reader already.
	dummyHash := hash.Hash{}
	stats := &Stats{}

	archiveReader, err := newArchiveReader(ctx, fra, dummyHash, uint64(fra.sz), q, stats)
	if err != nil {
		fra.Close()
		return nil, err
	}

	return &ArchiveInspector{reader: archiveReader}, nil
}

// Close releases resources associated with the archive inspector
func (ai *ArchiveInspector) Close() error {
	return ai.reader.close()
}

// ChunkCount returns the number of chunks in the archive
func (ai *ArchiveInspector) ChunkCount() uint32 {
	return ai.reader.count()
}

// FormatVersion returns the format version of the archive
func (ai *ArchiveInspector) FormatVersion() uint8 {
	return ai.reader.footer.formatVersion
}

// FileSignature returns the file signature of the archive
func (ai *ArchiveInspector) FileSignature() string {
	return ai.reader.footer.fileSignature
}

// IndexSize returns the size of the index section in bytes
func (ai *ArchiveInspector) IndexSize() uint64 {
	return ai.reader.footer.indexSize
}

// MetadataSize returns the size of the metadata section in bytes
func (ai *ArchiveInspector) MetadataSize() uint32 {
	return ai.reader.footer.metadataSize
}

// FileSize returns the total size of the archive file
func (ai *ArchiveInspector) FileSize() uint64 {
	return ai.reader.footer.fileSize
}

func (ai *ArchiveInspector) SplitOffset() uint64 {
	return ai.reader.footer.dataSpan().length
}

// ByteSpanCount returns the number of byte spans in the archive
func (ai *ArchiveInspector) ByteSpanCount() uint32 {
	return ai.reader.footer.byteSpanCount
}

// GetMetadata retrieves the metadata from the archive as raw bytes
func (ai *ArchiveInspector) GetMetadata(ctx context.Context) ([]byte, error) {
	stats := &Stats{}
	return ai.reader.getMetadata(ctx, stats)
}

// SearchChunk exposes the underlying search method for debugging
func (ai *ArchiveInspector) SearchChunk(h hash.Hash) int {
	return ai.reader.search(h)
}

// SearchChunkDebug exposes detailed search information for debugging
func (ai *ArchiveInspector) SearchChunkDebug(h hash.Hash) *SearchDebugInfo {
	prefix := h.Prefix()
	possibleMatch := ai.reader.indexReader.searchPrefix(prefix)
	targetSfx := h.Suffix()

	debug := &SearchDebugInfo{
		Hash:            h.String(),
		Prefix:          prefix,
		Suffix:          targetSfx,
		PossibleMatch:   possibleMatch,
		ChunkCount:      ai.reader.footer.chunkCount,
		IndexReaderType: fmt.Sprintf("%T", ai.reader.indexReader),
	}

	// Check if possibleMatch is in valid range
	if possibleMatch < 0 || uint32(possibleMatch) >= ai.reader.footer.chunkCount {
		debug.ValidRange = false
		debug.FinalResult = -1
		return debug
	}

	debug.ValidRange = true

	// Check prefix matches in the range
	matches := []PrefixMatch{}
	for idx := uint32(possibleMatch); idx < ai.reader.footer.chunkCount && ai.reader.indexReader.getPrefix(idx) == prefix; idx++ {
		suffixAtIdx := ai.reader.indexReader.getSuffix(idx)
		match := PrefixMatch{
			Index:       idx,
			SuffixAtIdx: suffixAtIdx[:],
			SuffixMatch: suffixAtIdx == suffix(targetSfx),
		}
		matches = append(matches, match)

		if suffixAtIdx == suffix(targetSfx) {
			debug.FinalResult = int(idx)
			debug.Matches = matches
			return debug
		}
	}

	debug.Matches = matches
	debug.FinalResult = -1
	return debug
}

// GetIndexReaderDetails exposes internal index reader state for debugging
func (ai *ArchiveInspector) GetIndexReaderDetails(idx uint32) *IndexReaderDetails {
	details := &IndexReaderDetails{
		IndexReaderType: fmt.Sprintf("%T", ai.reader.indexReader),
		RequestedIndex:  idx,
		ChunkCount:      ai.reader.footer.chunkCount,
		ByteSpanCount:   ai.reader.footer.byteSpanCount,
	}

	if idx >= ai.reader.footer.chunkCount {
		details.Error = "index out of range"
		return details
	}

	// Get prefix and suffix
	prefix := ai.reader.indexReader.getPrefix(idx)
	suffix := ai.reader.indexReader.getSuffix(idx)

	details.Prefix = prefix
	details.Suffix = suffix[:]

	// Construct the full hash from prefix and suffix
	hashBytes := make([]byte, hash.ByteLen)
	binary.BigEndian.PutUint64(hashBytes[:hash.PrefixLen], prefix)
	copy(hashBytes[hash.PrefixLen:], suffix[:])
	reconstructedHash := hash.New(hashBytes)
	details.Hash = reconstructedHash.String()

	// Get chunk references
	dictID, dataID := ai.reader.indexReader.getChunkRef(idx)
	details.DictionaryID = dictID
	details.DataID = dataID

	// For in-memory reader, expose the raw array details
	if inMem, ok := ai.reader.indexReader.(*inMemoryArchiveIndexReader); ok {
		details.PrefixArrayLength = len(inMem.prefixes)
		details.SuffixArrayLength = len(inMem.suffixes)
		details.ChunkRefArrayLength = len(inMem.chunkRefs)
		details.SpanIndexArrayLength = len(inMem.spanIndex)

		// Calculate expected suffix position
		expectedSuffixStart := idx * hash.SuffixLen
		details.ExpectedSuffixStart = expectedSuffixStart
		details.ExpectedSuffixEnd = expectedSuffixStart + hash.SuffixLen
		details.SuffixArrayBounds = expectedSuffixStart+hash.SuffixLen <= uint32(len(inMem.suffixes))

		// Show raw bytes around the suffix position for debugging
		if expectedSuffixStart < uint32(len(inMem.suffixes)) {
			end := expectedSuffixStart + hash.SuffixLen
			if end > uint32(len(inMem.suffixes)) {
				end = uint32(len(inMem.suffixes))
			}
			details.RawSuffixBytes = inMem.suffixes[expectedSuffixStart:end]
		}
	}

	// For mmap reader, expose similar details
	if mmapReader, isMmap := ai.reader.indexReader.(*mmapIndexReader); isMmap {
		details.MmapIndexSize = mmapReader.indexSize
		details.MmapByteSpanCount = mmapReader.byteSpanCount
		details.MmapChunkCount = mmapReader.chunkCount
		details.SpanIndexOffset = mmapReader.spanIndexOffset
		details.PrefixesOffset = mmapReader.prefixesOffset
		details.ChunkRefsOffset = mmapReader.chunkRefsOffset
		details.SuffixesOffset = mmapReader.suffixesOffset

		// Calculate expected suffix position in mmap
		expectedSuffixStart := uint64(idx) * hash.SuffixLen
		actualSuffixOffset := mmapReader.suffixesOffset + expectedSuffixStart
		details.ExpectedSuffixStart = uint32(expectedSuffixStart)
		details.ExpectedSuffixEnd = uint32(expectedSuffixStart + hash.SuffixLen)
		details.ActualSuffixOffset = actualSuffixOffset

		// Try to read raw bytes around the suffix position
		if mmapReader.data != nil {
			rawBytes := make([]byte, hash.SuffixLen)
			_, err := mmapReader.data.ReadAt(rawBytes, int64(actualSuffixOffset))
			if err == nil {
				details.RawSuffixBytes = rawBytes
			} else {
				details.RawSuffixBytesError = err.Error()
			}
		}
	}

	return details
}

// GetChunkInfo looks up information about a specific chunk in the archive
func (ai *ArchiveInspector) GetChunkInfo(ctx context.Context, h hash.Hash) (*ChunkInfo, error) {
	idx := ai.reader.search(h)
	if idx < 0 {
		return nil, fmt.Errorf("chunk %s not found", h.String())
	}

	// Get the chunk reference (dictionary ID and data ID)
	dictID, dataID := ai.reader.getChunkRef(idx)

	dictByteSpan := ai.reader.getByteSpanByID(dictID)
	dataByteSpan := ai.reader.getByteSpanByID(dataID)

	compressionType := "unknown"
	formatVersion := ai.reader.footer.formatVersion

	if dictID == 0 {
		// Dictionary ID 0 means no dictionary
		if formatVersion == 1 {
			compressionType = "zstd (no dictionary)"
		} else if formatVersion >= 2 {
			compressionType = "snappy"
		}
	} else {
		// Dictionary ID > 0 means zstd with dictionary
		compressionType = "zstd (with dictionary)"
	}

	return &ChunkInfo{
		CompressionType: compressionType,
		DictionaryID:    dictID,
		DataID:          dataID,
		DictionaryByteSpan: ByteSpanInfo{
			Offset: dictByteSpan.offset,
			Length: dictByteSpan.length,
		},
		DataByteSpan: ByteSpanInfo{
			Offset: dataByteSpan.offset,
			Length: dataByteSpan.length,
		},
	}, nil
}
