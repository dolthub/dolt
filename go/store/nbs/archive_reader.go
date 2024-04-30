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
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/gozstd"
)

type archiveReader struct {
	reader    io.ReaderAt
	prefixes  []uint64
	byteSpans []byteSpan
	chunkRefs []chunkRef
	suffixes  []suffix
	footer    footer
}

type chunkRef struct {
	dict uint32
	data uint32
}

type suffix [hash.SuffixLen]byte

type footer struct {
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
}

// dataSpan returns the span of the data section of the archive. This is not generally used directly since we usually
// read individual spans for each chunk.
func (f footer) dataSpan() byteSpan {
	return byteSpan{offset: 0, length: f.fileSize - archiveFooterSize - uint64(f.metadataSize) - uint64(f.indexSize)}
}

// indexSpan returns the span of the index section of the archive.
func (f footer) indexSpan() byteSpan {
	return byteSpan{offset: f.fileSize - archiveFooterSize - uint64(f.metadataSize) - uint64(f.indexSize), length: uint64(f.indexSize)}
}

// matadataSpan returns the span of the metadata section of the archive.
func (f footer) metadataSpan() byteSpan {
	return byteSpan{offset: f.fileSize - archiveFooterSize - uint64(f.metadataSize), length: uint64(f.metadataSize)}
}

// Our mix of using binary.ReadUvarint and binary.Read paints us in a bit of a corner here. To work around this
// we wrap the section reader with the ByteReader interface. There may be a better way to do this.
type sectionReaderByteReader struct {
	sectionReader *io.SectionReader
}

// ReadByte - see op.SectionReader.ReadByte. This may prove to be a bottleneck. We use this to read varints, which
// by definition we don't know the length of in advance.
func (r sectionReaderByteReader) ReadByte() (byte, error) {
	buf := []byte{0}
	_, err := r.sectionReader.Read(buf)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

func newArchiveReader(reader io.ReaderAt, fileSize uint64) (archiveReader, error) {
	footer, err := loadFooter(reader, fileSize)
	if err != nil {
		return archiveReader{}, err
	}

	indexSpan := footer.indexSpan()
	section := io.NewSectionReader(reader, int64(indexSpan.offset), int64(indexSpan.length))

	byteSpans := make([]byteSpan, footer.byteSpanCount+1)
	byteSpans = append(byteSpans, byteSpan{offset: 0, length: 0}) // Null byteSpan to simplify logic.

	offset := uint64(0)
	byteReader := sectionReaderByteReader{sectionReader: section}
	for i := uint32(0); i < footer.byteSpanCount; i++ {
		length, err := binary.ReadUvarint(byteReader)
		if err != nil {
			return archiveReader{}, err
		}
		byteSpans[i+1] = byteSpan{offset: offset, length: length}
		offset += length
	}

	prefixes := make([]uint64, footer.chunkCount)
	for i := uint32(0); i < footer.chunkCount; i++ {
		val := uint64(0)
		err := binary.Read(section, binary.BigEndian, &val)
		if err != nil {
			return archiveReader{}, err
		}
		prefixes[i] = val
	}

	chunks := make([]chunkRef, footer.chunkCount)
	for i := uint32(0); i < footer.chunkCount; i++ {
		dict64, err := binary.ReadUvarint(byteReader)
		if err != nil {
			return archiveReader{}, err
		}
		data64, err := binary.ReadUvarint(byteReader)
		if err != nil {
			return archiveReader{}, err
		}
		chunks[i] = chunkRef{dict: uint32(dict64), data: uint32(data64)}
	}

	suffixes := make([]suffix, footer.chunkCount)
	for i := uint32(0); i < footer.chunkCount; i++ {
		n, err := section.Read(suffixes[i][:])
		if err != nil {
			return archiveReader{}, err
		}
		if n != hash.SuffixLen {
			return archiveReader{}, io.ErrUnexpectedEOF
		}
	}

	return archiveReader{
		reader:    reader,
		prefixes:  prefixes,
		byteSpans: byteSpans,
		chunkRefs: chunks,
		suffixes:  suffixes,
		footer:    footer,
	}, nil
}

func loadFooter(reader io.ReaderAt, fileSize uint64) (f footer, err error) {
	section := io.NewSectionReader(reader, int64(fileSize-archiveFooterSize), int64(archiveFooterSize))

	bytesRead := 0
	buf := make([]byte, archiveFooterSize)
	bytesRead, err = section.Read(buf)
	if err != nil {
		return
	}
	if bytesRead != int(archiveFooterSize) {
		err = io.ErrUnexpectedEOF
		return
	}

	f.indexSize = binary.BigEndian.Uint32(buf[afrIndexLenOffset : afrIndexChkSumOffset+uint32Size])
	f.byteSpanCount = binary.BigEndian.Uint32(buf[afrByteSpanOffset : afrByteSpanOffset+uint32Size])
	f.chunkCount = binary.BigEndian.Uint32(buf[afrChunkCountOffset : afrChunkCountOffset+uint32Size])
	f.metadataSize = binary.BigEndian.Uint32(buf[afrMetaLenOffset : afrMetaLenOffset+uint32Size])
	f.dataCheckSum = sha512Sum(buf[afrDataChkSumOffset : afrDataChkSumOffset+sha512.Size])
	f.indexCheckSum = sha512Sum(buf[afrIndexChkSumOffset : afrIndexChkSumOffset+sha512.Size])
	f.metaCheckSum = sha512Sum(buf[afrMetaChkSumOffset : afrMetaChkSumOffset+sha512.Size])
	f.formatVersion = buf[afrVersionOffset]
	f.fileSignature = string(buf[afrSigOffset:])
	f.fileSize = fileSize

	// Verify File Signature
	if f.fileSignature != archiveFileSignature {
		err = ErrInvalidFileSignature
		return
	}
	// Verify Format Version. Currently only one version is supported, but we'll need to be more flexible in the future.
	if f.formatVersion != archiveFormatVersion {
		err = ErrInvalidFormatVersion
		return
	}

	return
}

func (ai archiveReader) search(hash hash.Hash) (bool, int) {
	prefix := hash.Prefix()
	matchingIndexes := findMatchingPrefixes(ai.prefixes, prefix)
	if len(matchingIndexes) == 0 {
		return false, 0
	}

	targetSfx := hash.Suffix()

	for _, idx := range matchingIndexes {
		if ai.suffixes[idx] == suffix(targetSfx) {
			return true, idx
		}
	}

	return false, 0
}

func (ai archiveReader) has(hash hash.Hash) bool {
	found, _ := ai.search(hash)
	return found
}

// get returns the decompressed data for the given hash. If the hash is not found, nil is returned (not an error)
func (ai archiveReader) get(hash hash.Hash) ([]byte, error) {
	dict, data, err := ai.getRaw(hash)
	if err != nil || data == nil {
		return nil, err
	}

	var result []byte
	if dict == nil {
		result, err = gozstd.Decompress(nil, data)
	} else {
		dDict, e2 := gozstd.NewDDict(dict)
		if e2 != nil {
			return nil, e2
		}
		result, err = gozstd.DecompressDict(nil, data, dDict)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (ai archiveReader) readByteSpan(buff []byte, bs byteSpan) ([]byte, error) {
	if len(buff) < int(bs.length) {
		return nil, io.ErrShortBuffer
	}

	read, err := ai.reader.ReadAt(buff[:bs.length], int64(bs.offset))
	if err != nil {
		return nil, err
	}
	if uint64(read) != bs.length {
		return nil, io.ErrUnexpectedEOF
	}
	return buff[:bs.length], nil
}

// getRaw returns the raw data for the given hash. If the hash is not found, nil is returned for both slices. Also,
// no error is returned in this case. Errors will only be returned if there is an io error.
//
// The data returned is still compressed, regardless of the dictionary being present or not.
func (ai archiveReader) getRaw(hash hash.Hash) (dict, data []byte, err error) {
	found, idx := ai.search(hash)
	if !found {
		return nil, nil, nil
	}

	chunkRef := ai.chunkRefs[idx]
	if chunkRef.dict != 0 {
		byteSpan := ai.byteSpans[chunkRef.dict]
		dict = make([]byte, byteSpan.length)
		dict, err = ai.readByteSpan(dict, byteSpan)
		if err != nil {
			return nil, nil, err
		}
	}

	byteSpan := ai.byteSpans[chunkRef.data]
	data = make([]byte, byteSpan.length)
	data, err = ai.readByteSpan(data, byteSpan)
	if err != nil {
		return nil, nil, err
	}
	return
}

func (ai archiveReader) getMetadata() ([]byte, error) {
	span := ai.footer.metadataSpan()
	data := make([]byte, span.length)
	return ai.readByteSpan(data, span)
}

// verifyDataCheckSum verifies the checksum of the data section of the archive. Note - this requires a fully read of
// the data section, which could be sizable.
func (ai archiveReader) verifyDataCheckSum() error {
	return verifyCheckSum(ai.reader, ai.footer.dataSpan(), ai.footer.dataCheckSum)
}

// verifyIndexCheckSum verifies the checksum of the index section of the archive.
func (ai archiveReader) verifyIndexCheckSum() error {
	return verifyCheckSum(ai.reader, ai.footer.indexSpan(), ai.footer.indexCheckSum)
}

// verifyMetaCheckSum verifies the checksum of the metadata section of the archive.
func (ai archiveReader) verifyMetaCheckSum() error {
	return verifyCheckSum(ai.reader, ai.footer.metadataSpan(), ai.footer.metaCheckSum)
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

// findMatchingPrefixes returns all indexes of the input slice that have a prefix that matches the target prefix.
func findMatchingPrefixes(slice []uint64, target uint64) []int {
	matchingIndexes := []int{}
	anIdx := prollyBinSearch(slice, target)

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

// prollyBinSearch is a search that returns the index of the target in the input slice. It starts by assuming
// that the slice has evenly distributed values, and picks a starting point which is close to where the target should
// be based on this assumption.
//
// Nailing it on the first shot is unlikely, so we follow by doing a binary search in the same area.
//
// If the value isn't found, -1 is returned.
func prollyBinSearch(slice []uint64, target uint64) int {
	if len(slice) == 0 {
		return -1
	}

	low := 0
	high := len(slice) - 1
	for low <= high {
		if slice[low] > target {
			return -1
		} else if slice[low] == target {
			return low
		}
		if slice[high] < target {
			return -1
		} else if slice[high] == target {
			return high
		}

		if high-low > 256 {
			// Determine the estimated position of the target in the slice, as a float from 0 to 1.
			minVal := slice[low]
			maxVal := slice[high]
			shiftedTarget := target - minVal
			shiftedMax := maxVal - minVal

			est := float64(shiftedTarget) / float64(shiftedMax)
			estIdx := int(float64(high-low) * est)
			estIdx += low

			if estIdx >= len(slice) {
				estIdx = len(slice) - 1
			}

			if slice[estIdx] == target {
				return estIdx // bulls-eye!
			}

			// When we miss the target, we know that we are pretty close based on the assumption of distribution.
			// Therefore, unlike a binary search where we consider everything on the left or right, we instead do
			// a scan in the appropriate direction using a widening scope. When all is said and done, low and high
			// will be set to values which are pretty close to the guess.
			widenScope := 16
			if slice[estIdx] > target {
				// We overshot, so search left
				high = estIdx - 1
				newLow := high - widenScope
				for newLow > low && slice[newLow] > target {
					high = newLow    // just verified that newLow is higher than target
					widenScope <<= 2 // Quadruple the scope each loop.
					newLow = high - widenScope
				}
				if newLow > low {
					low = newLow
				}
			} else {
				// We undershot, so search right
				low = estIdx + 1
				newHigh := low + widenScope
				for newHigh < high && slice[newHigh] < target {
					low = newHigh
					widenScope <<= 2
					newHigh = low + widenScope
				}
				if newHigh < high {
					high = newHigh
				}
			}
		} else {
			// Fall back to binary search
			for low <= high {
				mid := low + (high-low)/2
				if slice[mid] == target {
					return mid // Found
				} else if slice[mid] < target {
					low = mid + 1 // Search right half
				} else {
					high = mid - 1 // Search left half
				}
			}
			return -1 // Not found
		}
	}
	return -1
}
