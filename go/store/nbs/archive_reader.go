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
	"encoding/binary"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
)

type archiveIndex struct {
	reader    io.ReaderAt
	prefixes  []uint64
	byteSpans []byteSpan
	chunkRefs []chunkRef
	suffixes  []suffix
}

type byteSpan struct {
	offset uint64
	length uint64
}

type chunkRef struct {
	dict uint32
	data uint32
}

type suffix [hash.SuffixLen]byte

// Our mix of using binary.ReadUvarint and binary.Read paints us in a bit of a corner here. To work around this
// we wrap the section reader with the ByteReader interface. There may be a better way to do this.
type sectionReaderByteReader struct {
	sectionReader *io.SectionReader
}

// ReadByte - see op.SectionReader.ReadByte.
func (r sectionReaderByteReader) ReadByte() (byte, error) {
	buf := []byte{0}
	_, err := r.sectionReader.Read(buf)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

func newArchiveIndex(reader io.ReaderAt, fileSize uint64) (archiveIndex, error) {
	idx, bs, cc, err := loadFooter(reader, fileSize)
	if err != nil {
		return archiveIndex{}, err
	}

	indexStart := fileSize - archiveFooterSize - uint64(idx)
	section := io.NewSectionReader(reader, int64(indexStart), int64(idx))

	byteSpans := make([]byteSpan, bs+1)
	byteSpans = append(byteSpans, byteSpan{offset: 0, length: 0}) // Null byteSpan to simplify logic.

	byteReader := sectionReaderByteReader{sectionReader: section}
	for i := uint32(0); i < bs; i++ {
		offset, err := binary.ReadUvarint(byteReader)
		if err != nil {
			return archiveIndex{}, err
		}
		length, err := binary.ReadUvarint(byteReader)
		if err != nil {
			return archiveIndex{}, err
		}

		byteSpans[i+1] = byteSpan{offset: offset, length: length}
	}

	prefixes := make([]uint64, cc)
	for i := uint32(0); i < cc; i++ {
		val := uint64(0)
		err := binary.Read(section, binary.BigEndian, &val)
		if err != nil {
			return archiveIndex{}, err
		}
		prefixes[i] = val
	}

	chunks := make([]chunkRef, cc)
	for i := uint32(0); i < cc; i++ {
		dict64, err := binary.ReadUvarint(byteReader)
		if err != nil {
			return archiveIndex{}, err
		}
		data64, err := binary.ReadUvarint(byteReader)
		if err != nil {
			return archiveIndex{}, err
		}
		chunks[i] = chunkRef{dict: uint32(dict64), data: uint32(data64)}
	}

	suffixes := make([]suffix, cc)
	for i := uint32(0); i < cc; i++ {
		n, err := section.Read(suffixes[i][:])
		if err != nil {
			return archiveIndex{}, err
		}
		if n != hash.SuffixLen {
			return archiveIndex{}, io.ErrUnexpectedEOF
		}
	}

	return archiveIndex{
		reader:    reader,
		prefixes:  prefixes,
		byteSpans: byteSpans,
		chunkRefs: chunks,
		suffixes:  suffixes,
	}, nil
}

func loadFooter(reader io.ReaderAt, fileSize uint64) (indexSize, byteSpanCount, chunkCount uint32, err error) {
	section := io.NewSectionReader(reader, int64(fileSize-archiveFooterSize), archiveFooterSize)

	bytesRead := 0
	buf := make([]byte, archiveFooterSize)
	bytesRead, err = section.Read(buf)
	if err != nil {
		return
	}
	if bytesRead != archiveFooterSize {
		err = io.ErrUnexpectedEOF
		return
	}

	// Verify File Signature
	if string(buf[13:]) != archiveFileSignature {
		err = ErrInvalidFileSignature
		return
	}
	// Verify Format Version. Currently only one version is supported, but we'll need to be more flexible in the future.
	if buf[12] != archiveFormatVersion {
		err = ErrInvalidFormatVersion
		return
	}

	indexSize = binary.BigEndian.Uint32(buf[:uint32Size])
	byteSpanCount = binary.BigEndian.Uint32(buf[uint32Size : uint32Size*2])
	chunkCount = binary.BigEndian.Uint32(buf[uint32Size*2 : uint32Size*3])

	return
}

func (ai archiveIndex) has(hash hash.Hash) bool {
	prefix := hash.Prefix()
	matchingIndexes := findMatchingPrefixes(ai.prefixes, prefix)
	if len(matchingIndexes) == 0 {
		return false
	}

	targetSfx := hash.Suffix()

	for _, idx := range matchingIndexes {
		if ai.suffixes[idx] == suffix(targetSfx) {
			return true
		}
	}

	return false
}

// findMatchingPrefixes returns all indexes of the input slice that have a prefix that matches the target prefix.
func findMatchingPrefixes(slice []uint64, target uint64) []int {
	matchingIndexes := []int{}
	anIdx := binarySearch(slice, target)
	if anIdx == -1 {
		return matchingIndexes
	}

	if anIdx > 0 {
		// Ensure we are on the first matching index.
		for anIdx > 0 && slice[anIdx-1] == target {
			anIdx--
		}
	}

	for anIdx < len(slice) && slice[anIdx] == target {
		matchingIndexes = append(matchingIndexes, anIdx)
		anIdx++
	}

	return matchingIndexes
}

func binarySearch(prefixes []uint64, target uint64) int {
	low := 0
	high := len(prefixes) - 1
	for low <= high {
		mid := low + (high-low)/2
		if prefixes[mid] == target {
			return mid // Found
		} else if prefixes[mid] < target {
			low = mid + 1 // Search right half
		} else {
			high = mid - 1 // Search left half
		}
	}
	return -1 // Not found
}
