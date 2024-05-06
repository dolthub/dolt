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
	"bufio"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/dolthub/gozstd"
	"github.com/pkg/errors"

	"github.com/dolthub/dolt/go/store/hash"
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

// totalIndexSpan returns the span of the entire index section of the archive. This span is not directly useful as
// the index is broken into a compressed section and an uncompressed section. Use indexCompressedSpan and indexSuffixSpan
func (f footer) totalIndexSpan() byteSpan {
	return byteSpan{offset: f.fileSize - archiveFooterSize - uint64(f.metadataSize) - uint64(f.indexSize), length: uint64(f.indexSize)}
}

// indexCompressedSpan returns the span of the index section of the archive.
func (f footer) indexCompressedSpan() byteSpan {
	suffixLen := uint64(f.chunkCount * hash.SuffixLen)
	totalIdx := f.totalIndexSpan()
	return byteSpan{offset: totalIdx.offset, length: totalIdx.length - suffixLen}
}

func (f footer) indexSuffixSpan() byteSpan {
	suffixLen := uint64(f.chunkCount * hash.SuffixLen)
	totalIdx := f.totalIndexSpan()
	compressedLen := totalIdx.length - suffixLen

	return byteSpan{totalIdx.offset + compressedLen, suffixLen}
}

func (f footer) metadataSpan() byteSpan {
	return byteSpan{offset: f.fileSize - archiveFooterSize - uint64(f.metadataSize), length: uint64(f.metadataSize)}
}

func newArchiveReader(reader io.ReaderAt, fileSize uint64) (archiveReader, error) {
	footer, err := loadFooter(reader, fileSize)
	if err != nil {
		return archiveReader{}, err
	}

	indexSpan := footer.indexCompressedSpan()
	secRdr := io.NewSectionReader(reader, int64(indexSpan.offset), int64(indexSpan.length))
	rawReader := bufio.NewReader(secRdr)

	errChan := make(chan error, 1)

	redr, wrtr := io.Pipe()
	go func() {
		defer wrtr.Close()
		err := gozstd.StreamDecompress(wrtr, rawReader)
		if err != nil {
			errChan <- errors.Wrap(err, "Failed to decompress archive index")
		}

		close(errChan)
	}()
	byteReader := bufio.NewReader(redr)

	byteSpans := make([]byteSpan, footer.byteSpanCount+1)
	byteSpans = append(byteSpans, byteSpan{offset: 0, length: 0}) // Null byteSpan to simplify logic.

	offset := uint64(0)
	for i := uint32(0); i < footer.byteSpanCount; i++ {
		length := uint64(0)
		err := binary.Read(byteReader, binary.BigEndian, &length)
		if err != nil {
			return archiveReader{}, err
		}
		byteSpans[i+1] = byteSpan{offset: offset, length: length}
		offset += length
	}

	lastPrefix := uint64(0)
	prefixes := make([]uint64, footer.chunkCount)
	for i := uint32(0); i < footer.chunkCount; i++ {
		delta := uint64(0)
		err := binary.Read(byteReader, binary.BigEndian, &delta)
		if err != nil {
			return archiveReader{}, err
		}
		prefixes[i] = lastPrefix + delta
		lastPrefix = prefixes[i]
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

		if dict64 > math.MaxUint32 || data64 > math.MaxUint32 {
			return archiveReader{}, errors.New("invalid chunk reference. Chunk references must be 32-bit unsigned integers.")
		}

		chunks[i] = chunkRef{dict: uint32(dict64), data: uint32(data64)}
	}

	// Wait for all compressed data to finish.
	err, _ = <-errChan
	if err != nil {
		return archiveReader{}, err
	}

	suffixSpan := footer.indexSuffixSpan()
	sufRdr := io.NewSectionReader(reader, int64(suffixSpan.offset), int64(suffixSpan.length))
	sufReader := bufio.NewReader(sufRdr)
	suffixes := make([]suffix, footer.chunkCount)
	for i := uint32(0); i < footer.chunkCount; i++ {
		_, err := io.ReadFull(sufReader, suffixes[i][:])
		if err != nil {
			return archiveReader{}, err
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

	return
}

func (ai archiveReader) search(hash hash.Hash) (bool, int) {
	prefix := hash.Prefix()
	possibleMatch := prollyBinSearch(ai.prefixes, prefix)
	targetSfx := hash.Suffix()

	for i := 0; i < len(ai.suffixes); i++ {
		idx := possibleMatch + i

		if ai.prefixes[idx] != prefix {
			return false, -1
		}

		if ai.suffixes[idx] == suffix(targetSfx) {
			return true, idx
		}
	}
	return false, -1
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
	return verifyCheckSum(ai.reader, ai.footer.totalIndexSpan(), ai.footer.indexCheckSum)
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
