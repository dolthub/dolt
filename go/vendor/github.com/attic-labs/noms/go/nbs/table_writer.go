// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"hash"
	"sort"

	"github.com/attic-labs/noms/go/d"
	"github.com/golang/snappy"
)

// tableWriter encodes a collection of byte stream chunks into a nbs table. NOT goroutine safe.
type tableWriter struct {
	buff                  []byte
	pos                   uint64
	totalCompressedData   uint64
	totalUncompressedData uint64
	prefixes              prefixIndexSlice // TODO: This is in danger of exploding memory
	blockHash             hash.Hash

	snapper snappyEncoder
}

type snappyEncoder interface {
	Encode(dst, src []byte) []byte
}

type realSnappyEncoder struct{}

func (r realSnappyEncoder) Encode(dst, src []byte) []byte {
	return snappy.Encode(dst, src)
}

func maxTableSize(numChunks, totalData uint64) uint64 {
	avgChunkSize := totalData / numChunks
	d.Chk.True(avgChunkSize < maxChunkSize)
	maxSnappySize := snappy.MaxEncodedLen(int(avgChunkSize))
	d.Chk.True(maxSnappySize > 0)
	return numChunks*(prefixTupleSize+lengthSize+addrSuffixSize+checksumSize+uint64(maxSnappySize)) + footerSize
}

func indexSize(numChunks uint32) uint64 {
	return uint64(numChunks) * (addrSuffixSize + lengthSize + prefixTupleSize)
}

func lengthsOffset(numChunks uint32) uint64 {
	return uint64(numChunks) * prefixTupleSize
}

func suffixesOffset(numChunks uint32) uint64 {
	return uint64(numChunks) * (prefixTupleSize + lengthSize)
}

// len(buff) must be >= maxTableSize(numChunks, totalData)
func newTableWriter(buff []byte, snapper snappyEncoder) *tableWriter {
	if snapper == nil {
		snapper = realSnappyEncoder{}
	}
	return &tableWriter{
		buff:      buff,
		blockHash: sha512.New(),
		snapper:   snapper,
	}
}

func (tw *tableWriter) addChunk(h addr, data []byte) bool {
	if len(data) == 0 {
		panic("NBS blocks cannont be zero length")
	}

	// Compress data straight into tw.buff
	compressed := tw.snapper.Encode(tw.buff[tw.pos:], data)
	dataLength := uint64(len(compressed))
	tw.totalCompressedData += dataLength

	// BUG 3156 indicated that, sometimes, snappy decided that there's not enough space in tw.buff[tw.pos:] to encode into.
	// This _should never happen anymore be_, because we iterate over all chunks to be added and sum the max amount of space that snappy says it might need.
	// Since we know that |data| can't be 0-length, we also know that the compressed version of |data| has length greater than zero. The first element in a snappy-encoded blob is a Uvarint indicating how much data is present. Therefore, if there's a Uvarint-encoded 0 at tw.buff[tw.pos:], we know that snappy did not write anything there and we have a problem.
	if v, n := binary.Uvarint(tw.buff[tw.pos:]); v == 0 {
		d.Chk.True(n != 0)
		panic(fmt.Errorf("BUG 3156: unbuffered chunk %s: uncompressed %d, compressed %d, snappy max %d, tw.buff %d\n", h.String(), len(data), dataLength, snappy.MaxEncodedLen(len(data)), len(tw.buff[tw.pos:])))
	}

	tw.pos += dataLength
	tw.totalUncompressedData += uint64(len(data))

	// checksum (4 LSBytes, big-endian)
	binary.BigEndian.PutUint32(tw.buff[tw.pos:], crc(compressed))
	tw.pos += checksumSize

	// Stored in insertion order
	tw.prefixes = append(tw.prefixes, prefixIndexRec{
		h.Prefix(),
		h[addrPrefixSize:],
		uint32(len(tw.prefixes)),
		uint32(checksumSize + dataLength),
	})

	return true
}

func (tw *tableWriter) finish() (uncompressedLength uint64, blockAddr addr) {
	tw.writeIndex()
	tw.writeFooter()
	uncompressedLength = tw.pos

	var h []byte
	h = tw.blockHash.Sum(h) // Appends hash to h
	copy(blockAddr[:], h)
	return
}

type prefixIndexRec struct {
	prefix      uint64
	suffix      []byte
	order, size uint32
}

type prefixIndexSlice []prefixIndexRec

func (hs prefixIndexSlice) Len() int           { return len(hs) }
func (hs prefixIndexSlice) Less(i, j int) bool { return hs[i].prefix < hs[j].prefix }
func (hs prefixIndexSlice) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

func (tw *tableWriter) writeIndex() {
	sort.Sort(tw.prefixes)

	pfxScratch := [addrPrefixSize]byte{}

	numRecords := uint32(len(tw.prefixes))
	lengthsOffset := tw.pos + lengthsOffset(numRecords)   // skip prefix and ordinal for each record
	suffixesOffset := tw.pos + suffixesOffset(numRecords) // skip size for each record
	for _, pi := range tw.prefixes {
		binary.BigEndian.PutUint64(pfxScratch[:], pi.prefix)

		// hash prefix
		n := uint64(copy(tw.buff[tw.pos:], pfxScratch[:]))
		d.Chk.True(n == addrPrefixSize)
		tw.pos += n

		// order
		binary.BigEndian.PutUint32(tw.buff[tw.pos:], pi.order)
		tw.pos += ordinalSize

		// length
		offset := lengthsOffset + uint64(pi.order)*lengthSize
		binary.BigEndian.PutUint32(tw.buff[offset:], pi.size)

		// hash suffix
		offset = suffixesOffset + uint64(pi.order)*addrSuffixSize
		n = uint64(copy(tw.buff[offset:], pi.suffix))
		d.Chk.True(n == addrSuffixSize)
	}
	suffixesLen := uint64(numRecords) * addrSuffixSize
	tw.blockHash.Write(tw.buff[suffixesOffset : suffixesOffset+suffixesLen])
	tw.pos = suffixesOffset + suffixesLen
}

func (tw *tableWriter) writeFooter() {
	tw.pos += writeFooter(tw.buff[tw.pos:], uint32(len(tw.prefixes)), tw.totalUncompressedData)
}

func writeFooter(dst []byte, chunkCount uint32, uncData uint64) (consumed uint64) {
	// chunk count
	binary.BigEndian.PutUint32(dst[consumed:], chunkCount)
	consumed += uint32Size

	// total uncompressed chunk data
	binary.BigEndian.PutUint64(dst[consumed:], uncData)
	consumed += uint64Size

	// magic number
	copy(dst[consumed:], magicNumber)
	consumed += magicNumberSize
	return
}
