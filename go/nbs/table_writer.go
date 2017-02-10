// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"hash"
	"os"
	"sort"

	"github.com/attic-labs/noms/go/d"
	"github.com/golang/snappy"
)

// tableWriter encodes a collection of byte stream chunks into a nbs table. NOT goroutine safe.
type tableWriter struct {
	buff              []byte
	pos               uint64
	totalPhysicalData uint64
	prefixes          prefixIndexSlice // TODO: This is in danger of exploding memory
	blockHash         hash.Hash

	snapper snappyEncoder
}

type snappyEncoder interface {
	Encode(dst, src []byte) []byte
}

type realSnappyEncoder struct{}

func (r realSnappyEncoder) Encode(dst, src []byte) []byte {
	return snappy.Encode(dst, src)
}

func maxTableSize(lengths []uint32) (max uint64) {
	max = footerSize
	for _, length := range lengths {
		max += prefixTupleSize + lengthSize + addrSuffixSize + checksumSize // Metadata
		d.Chk.True(int64(length) < maxInt)
		max += uint64(snappy.MaxEncodedLen(int(length)))
	}
	return
}

func indexSize(numChunks uint32) uint64 {
	return uint64(numChunks) * (addrSuffixSize + lengthSize + prefixTupleSize)
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

	// BUG 3156 indicated that, sometimes, snappy decided that there's not enough space in tw.buff[tw.pos:] to encode into.
	// This _should never happen anymore be_, because we iterate over all chunks to be added and sum the max amount of space that snappy says it might need.
	// Since we know that |data| can't be 0-length, we also know that the compressed version of |data| has length greater than zero. The first element in a snappy-encoded blob is a Uvarint indicating how much data is present. Therefore, if there's a Uvarint-encoded 0 at tw.buff[tw.pos:], we know that snappy did not write anything there and we have a problem.
	if v, n := binary.Uvarint(tw.buff[tw.pos:]); v == 0 {
		d.Chk.True(n != 0)
		d.Chk.True(uint64(len(tw.buff[tw.pos:])) >= dataLength)
		fmt.Fprintf(os.Stderr, "BUG 3156: unbuffered chunk %s: uncompressed %d, compressed %d, snappy max %d, tw.buff %d\n", h.String(), len(data), dataLength, snappy.MaxEncodedLen(len(data)), len(tw.buff[tw.pos:]))

		// Copy the compressed data over to tw.buff.
		copy(tw.buff[tw.pos:], compressed)
	}

	tw.pos += dataLength
	tw.totalPhysicalData += dataLength

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

func (tw *tableWriter) finish() (byteLength uint64, blockAddr addr) {
	tw.writeIndex()
	tw.writeFooter()
	byteLength = tw.pos

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

	numRecords := uint64(len(tw.prefixes))
	lengthsOffset := tw.pos + numRecords*prefixTupleSize    // skip prefix and ordinal for each record
	suffixesOffset := lengthsOffset + numRecords*lengthSize // skip size for each record
	for _, pi := range tw.prefixes {
		binary.BigEndian.PutUint64(pfxScratch[:], pi.prefix)
		tw.blockHash.Write(pfxScratch[:])
		tw.blockHash.Write(pi.suffix)

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
	tw.pos = suffixesOffset + numRecords*addrSuffixSize
}

func (tw *tableWriter) writeFooter() {
	// chunk count
	chunkCount := uint32(len(tw.prefixes))
	binary.BigEndian.PutUint32(tw.buff[tw.pos:], chunkCount)
	tw.pos += uint32Size

	// total chunk data
	binary.BigEndian.PutUint64(tw.buff[tw.pos:], tw.totalPhysicalData)
	tw.pos += uint64Size

	// magic number
	copy(tw.buff[tw.pos:], magicNumber)
	tw.pos += magicNumberSize
}
