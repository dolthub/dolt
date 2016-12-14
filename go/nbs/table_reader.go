// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/attic-labs/noms/go/d"
	"github.com/golang/snappy"
)

type tableIndex struct {
	chunkCount        uint32
	prefixes, offsets []uint64
	lengths, ordinals []uint32
	suffixes          []byte
}

// tableReader implements get & has queries against a single nbs table. goroutine safe.
type tableReader struct {
	tableIndex
	r             io.ReaderAt
	readAmpThresh uint64
}

// parses a valid nbs tableIndex from a byte stream. |buff| must end with an NBS index and footer, though it may contain an unspecified number of bytes before that data. |tableIndex| doesn't keep alive any references to |buff|.
func parseTableIndex(buff []byte) tableIndex {
	pos := uint64(len(buff))

	// footer
	pos -= magicNumberSize
	d.Chk.True(string(buff[pos:]) == magicNumber)

	// skip total chunk data
	pos -= uint64Size

	pos -= uint32Size
	chunkCount := binary.BigEndian.Uint32(buff[pos:])

	// index
	suffixesSize := uint64(chunkCount) * addrSuffixSize
	pos -= suffixesSize
	suffixes := make([]byte, suffixesSize)
	copy(suffixes, buff[pos:])

	lengthsSize := uint64(chunkCount) * lengthSize
	pos -= lengthsSize
	lengths, offsets := computeOffsets(chunkCount, buff[pos:pos+lengthsSize])

	tuplesSize := uint64(chunkCount) * prefixTupleSize
	pos -= tuplesSize
	prefixes, ordinals := computePrefixes(chunkCount, buff[pos:pos+tuplesSize])

	return tableIndex{
		chunkCount,
		prefixes, offsets,
		lengths, ordinals,
		suffixes,
	}
}

func computeOffsets(count uint32, buff []byte) (lengths []uint32, offsets []uint64) {
	lengths = make([]uint32, count)
	offsets = make([]uint64, count)

	lengths[0] = binary.BigEndian.Uint32(buff)

	for i := uint64(1); i < uint64(count); i++ {
		lengths[i] = binary.BigEndian.Uint32(buff[i*lengthSize:])
		offsets[i] = offsets[i-1] + uint64(lengths[i-1])
	}
	return
}

func computePrefixes(count uint32, buff []byte) (prefixes []uint64, ordinals []uint32) {
	prefixes = make([]uint64, count)
	ordinals = make([]uint32, count)

	for i := uint64(0); i < uint64(count); i++ {
		idx := i * prefixTupleSize
		prefixes[i] = binary.BigEndian.Uint64(buff[idx:])
		ordinals[i] = binary.BigEndian.Uint32(buff[idx+addrPrefixSize:])
	}
	return
}

func (ti tableIndex) prefixIdxToOrdinal(idx uint32) uint32 {
	return ti.ordinals[idx]
}

// returns the first position in |tr.prefixes| whose value == |prefix|. Returns |tr.chunkCount|
// if absent
func (ti tableIndex) prefixIdx(prefix uint64) (idx uint32) {
	// NOTE: The golang impl of sort.Search is basically inlined here. This method can be called in
	// an extremely tight loop and inlining the code was a significant perf improvement.
	idx, j := 0, ti.chunkCount
	for idx < j {
		h := idx + (j-idx)/2 // avoid overflow when computing h
		// i ≤ h < j
		if ti.prefixes[h] < prefix {
			idx = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}

	return
}

// Return true IFF the suffix at insertion order |ordinal| matches the address |a|.
func (ti tableIndex) ordinalSuffixMatches(ordinal uint32, h addr) bool {
	li := uint64(ordinal) * addrSuffixSize
	return bytes.Compare(h[addrPrefixSize:], ti.suffixes[li:li+addrSuffixSize]) == 0
}

// returns the ordinal of |h| if present. returns |ti.chunkCount| if absent
func (ti tableIndex) lookupOrdinal(h addr) uint32 {
	prefix := h.Prefix()

	for idx := ti.prefixIdx(prefix); idx < ti.chunkCount && ti.prefixes[idx] == prefix; idx++ {
		ordinal := ti.prefixIdxToOrdinal(idx)
		if ti.ordinalSuffixMatches(ordinal, h) {
			return ordinal
		}
	}

	return ti.chunkCount
}

// newTableReader parses a valid nbs table byte stream and returns a reader. buff must end with an NBS index and footer, though it may contain an unspecified number of bytes before that data. r should allow retrieving any desired range of bytes from the table.
func newTableReader(index tableIndex, r io.ReaderAt, readAmpThresh uint64) tableReader {
	return tableReader{index, r, readAmpThresh}
}

// Scan across (logically) two ordered slices of address prefixes.
func (tr tableReader) hasMany(addrs []hasRecord) (remaining bool) {
	// TODO: Use findInIndex if (tr.chunkCount - len(addrs)*Log2(tr.chunkCount)) > (tr.chunkCount - len(addrs))

	filterIdx := uint32(0)
	filterLen := uint32(len(tr.prefixes))

	for i, addr := range addrs {
		if addr.has {
			continue
		}

		for filterIdx < filterLen && addr.prefix > tr.prefixes[filterIdx] {
			filterIdx++
		}

		if filterIdx >= filterLen {
			remaining = true
			return
		}

		if addr.prefix != tr.prefixes[filterIdx] {
			remaining = true
			continue
		}

		// prefixes are equal, so locate and compare against the corresponding suffix
		for j := filterIdx; j < filterLen && addr.prefix == tr.prefixes[j]; j++ {
			if tr.ordinalSuffixMatches(tr.prefixIdxToOrdinal(j), *addr.a) {
				addrs[i].has = true
				break
			}
		}

		if !addrs[i].has {
			remaining = true
		}
	}

	return
}

func (tr tableReader) count() uint32 {
	return tr.chunkCount
}

// returns true iff |h| can be found in this table.
func (tr tableReader) has(h addr) bool {
	ordinal := tr.lookupOrdinal(h)
	return ordinal < tr.count()
}

// returns the storage associated with |h|, iff present. Returns nil if absent. On success,
// the returned byte slice directly references the underlying storage.
func (tr tableReader) get(h addr) (data []byte) {
	ordinal := tr.lookupOrdinal(h)
	if ordinal == tr.count() {
		return
	}

	offset := tr.offsets[ordinal]
	length := uint64(tr.lengths[ordinal])
	buff := make([]byte, length) // TODO: Avoid this allocation for every get
	n, err := tr.r.ReadAt(buff, int64(offset))
	d.Chk.NoError(err)
	d.Chk.True(n == int(length))
	data = tr.parseChunk(h, buff)
	d.Chk.True(data != nil)

	return
}

type offsetRec struct {
	reqIdx, ordinal uint32
	offset          uint64
}

type offsetRecSlice []offsetRec

func (hs offsetRecSlice) Len() int           { return len(hs) }
func (hs offsetRecSlice) Less(i, j int) bool { return hs[i].offset < hs[j].offset }
func (hs offsetRecSlice) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

const blockSize = 1 << 12

// getMany retrieves multiple stored blocks and optimizes by attempting to read in larger physical
// blocks which contain multiple stored blocks. |reqs| must be sorted by address prefix.
func (tr tableReader) getMany(reqs []getRecord) (remaining bool) {
	filterIdx := uint32(0)
	filterLen := uint32(len(tr.prefixes))
	offsetRecords := make(offsetRecSlice, 0, len(reqs))

	// Pass #1: Iterate over |reqs| and |tr.prefixes| (both sorted by address) and build the set
	// of table locations which must be read in order to satisfy the getMany operation.
	for i, req := range reqs {
		// advance within the prefixes until we reach one which is >= req.prefix
		for filterIdx < filterLen && tr.prefixes[filterIdx] < req.prefix {
			filterIdx++
		}

		if filterIdx >= filterLen {
			remaining = true // last prefix visited.
			break
		}

		if req.prefix != tr.prefixes[filterIdx] {
			remaining = true
			continue
		}

		// record all offsets within the table which contain the data required.
		for j := filterIdx; j < filterLen && req.prefix == tr.prefixes[j]; j++ {
			if tr.ordinalSuffixMatches(tr.prefixIdxToOrdinal(j), *req.a) {
				offsetRecords = append(offsetRecords, offsetRec{uint32(i), tr.ordinals[j], tr.offsets[tr.ordinals[j]]})
			}
		}
	}

	// Now |offsets| contains all locations within the table which must be search (note that there
	// may be duplicates of a particular location). Sort by offset and scan forward, grouping
	// sequences of reads into large physical reads.
	sort.Sort(offsetRecords)

	scratch := []byte{} // raw byte area into which reads occur
	// slice within |scratch| which contains a contiguous sequence of bytes read from the table
	buff := scratch[:]
	baseOffset := uint64(0) // the offset within the table which corresponds to byte 0 of |buff|

	for i, rec := range offsetRecords {
		if reqs[rec.reqIdx].data != nil {
			continue // already satisfied
		}

		// offset within |buff| which corresponds to the logical location of the chunkRecord
		localOffset := rec.offset - baseOffset
		length := tr.lengths[rec.ordinal]

		if uint64(len(buff)) < localOffset+uint64(length) {
			// |buff| doesn't contain sufficient bytes to read the current chunk record. scan forward
			// and read in a new sequence of bytes

			readStart := rec.offset
			readEnd := rec.offset + uint64(length) // implicitly include the first chunk

			// As we scan forward, for each location/length, we'll include it in the current read if
			// the total number of bytes we'll read contains fewer than X bytes we don't care about.
			readAmp := uint64(0)

			// scan ahead in offsets
			for j := i + 1; j < len(offsetRecords); j++ {
				fRec := offsetRecords[j]

				if reqs[fRec.reqIdx].data != nil {
					continue // already satisfied
				}

				if fRec.offset < readEnd {
					// |offsetRecords| will contain an offsetRecord for *every* chunkRecord whose address
					// prefix matches the prefix of a requested address. If the set of requests contains
					// addresses which share a common prefix, then it's possible for multiple offsetRecords
					// to reference the same table offset position. In that case, we'll see sequential
					// offsetRecords with the same fRec.offset.
					continue
				}

				// only consider "wasted" bytes ABOVE block_size to be read amplification.
				fReadAmp := fRec.offset - readEnd
				if fReadAmp < blockSize {
					fReadAmp = 0
				} else {
					fReadAmp -= blockSize
				}

				fLength := tr.lengths[fRec.ordinal]
				if fRec.offset+uint64(fLength)-readStart < tr.readAmpThresh*(readAmp+fReadAmp) {
					break // including the next block will read too many unneeded bytes
				}

				readEnd = fRec.offset + uint64(fLength)
				readAmp += fReadAmp
			}

			// Ensure our memory buffer is large enough
			if readEnd-readStart > uint64(len(scratch)) {
				scratch = make([]byte, readEnd-readStart)
			}

			buff = scratch[:readEnd-readStart]
			n, err := tr.r.ReadAt(buff, int64(readStart))
			d.Chk.NoError(err)
			d.Chk.True(uint64(n) == readEnd-readStart)

			baseOffset = readStart
			localOffset = 0
		}

		chunkRecord := buff[localOffset : localOffset+uint64(length)]
		data := tr.parseChunk(*reqs[rec.reqIdx].a, chunkRecord)
		if data != nil {
			reqs[rec.reqIdx].data = data
		} else {
			remaining = true
		}
	}

	return
}

// Fetches the byte stream of data logically encoded within the table starting at |pos|.
func (tr tableReader) parseChunk(h addr, buff []byte) []byte {
	dataLen := uint64(len(buff)) - checksumSize
	data, err := snappy.Decode(nil, buff[:dataLen])
	d.Chk.NoError(err)
	buff = buff[dataLen:]

	chksum := binary.BigEndian.Uint32(buff)
	d.Chk.True(chksum == crc(data))

	return data
}
