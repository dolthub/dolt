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

// tableReader implements get & has queries against a single nbs table. goroutine safe.
type tableReader struct {
	r                 io.ReaderAt
	suffixes          []byte
	prefixes, offsets []uint64
	lengths, ordinals []uint32
	chunkCount        uint32
}

// newTableReader parses a valid nbs table byte stream and returns a reader. buff must end with an NBS index and footer, though it may contain an unspecified number of bytes before that data. r should allow retrieving any desired range of bytes from the table.
func newTableReader(buff []byte, r io.ReaderAt) tableReader {
	tr := tableReader{r: r}

	pos := uint64(len(buff))

	// footer
	pos -= magicNumberSize
	d.Chk.True(string(buff[pos:]) == magicNumber)

	// skip total chunk data
	pos -= uint64Size

	pos -= uint32Size
	tr.chunkCount = binary.BigEndian.Uint32(buff[pos:])

	// index
	suffixesSize := uint64(tr.chunkCount) * addrSuffixSize
	pos -= suffixesSize
	tr.suffixes = buff[pos : pos+suffixesSize]

	lengthsSize := uint64(tr.chunkCount) * lengthSize
	pos -= lengthsSize
	tr.lengths, tr.offsets = computeOffsets(tr.chunkCount, buff[pos:pos+lengthsSize])

	tuplesSize := uint64(tr.chunkCount) * prefixTupleSize
	pos -= tuplesSize
	tr.prefixes, tr.ordinals = computePrefixes(tr.chunkCount, buff[pos:pos+tuplesSize])
	return tr
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
			li := uint64(tr.prefixIdxToOrdinal(j)) * addrSuffixSize
			if bytes.Compare(addr.a[addrPrefixSize:], tr.suffixes[li:li+addrSuffixSize]) == 0 {
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

func (tr tableReader) prefixIdxToOrdinal(idx uint32) uint32 {
	return tr.ordinals[idx]
}

// returns the first position in |tr.prefixes| whose value == |prefix|. Returns |tr.chunkCount|
// if absent
func (tr tableReader) prefixIdx(prefix uint64) (idx uint32) {
	// NOTE: The golang impl of sort.Search is basically inlined here. This method can be called in
	// an extremely tight loop and inlining the code was a significant perf improvement.
	idx, j := 0, tr.chunkCount
	for idx < j {
		h := idx + (j-idx)/2 // avoid overflow when computing h
		// i ≤ h < j
		if tr.prefixes[h] < prefix {
			idx = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}

	return
}

func (tr tableReader) count() uint32 {
	return tr.chunkCount
}

// returns true iff |h| can be found in this table.
func (tr tableReader) has(h addr) bool {
	prefix := h.Prefix()
	idx := tr.prefixIdx(prefix)

	for ; idx < tr.chunkCount && tr.prefixes[idx] == prefix; idx++ {
		ordinal := tr.prefixIdxToOrdinal(idx)
		suffixOffset := uint64(ordinal) * addrSuffixSize

		if bytes.Compare(tr.suffixes[suffixOffset:suffixOffset+addrSuffixSize], h[addrPrefixSize:]) == 0 {
			return true
		}
	}

	return false
}

// returns the storage associated with |h|, iff present. Returns nil if absent. On success,
// the returned byte slice directly references the underlying storage.
func (tr tableReader) get(h addr) (data []byte) {
	prefix := h.Prefix()
	idx := tr.prefixIdx(prefix)

	for ; idx < tr.chunkCount && tr.prefixes[idx] == prefix; idx++ {
		ordinal := tr.prefixIdxToOrdinal(idx)
		offset := tr.offsets[ordinal]
		length := uint64(tr.lengths[ordinal])
		buff := make([]byte, length) // TODO: Avoid this allocation for every get
		n, err := tr.r.ReadAt(buff, int64(offset))
		d.Chk.NoError(err)
		d.Chk.True(n == int(length))
		data = tr.parseChunk(h, buff)
		if data != nil {
			break
		}
	}

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
const readAmpThresh = 1 << 1

// getMany retrieves multiple stored blocks and optimizes by attempting to read in larger physical
// blocks which contain multiple stored blocks. |reqs| must be sorted by address prefix.
func (tr tableReader) getMany(reqs []getRecord) (remaining bool) {
	filterIdx := uint64(0)
	filterLen := uint64(len(tr.prefixes))
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

		// record all offsets within the table which *may* contain the address we are searching for.
		for j := filterIdx; j < filterLen && req.prefix == tr.prefixes[j]; j++ {
			offsetRecords = append(offsetRecords, offsetRec{uint32(i), tr.ordinals[j], tr.offsets[tr.ordinals[j]]})
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
				if fRec.offset+uint64(fLength)-readStart < readAmpThresh*(readAmp+fReadAmp) {
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
	// chksum (4 LSBytes, big-endian)
	chksum := binary.BigEndian.Uint32(buff)
	if chksum != h.Checksum() {
		return nil // false positive
	}
	buff = buff[checksumSize:]

	// data
	data, err := snappy.Decode(nil, buff)
	d.Chk.NoError(err)

	computedAddr := computeAddr(data)
	d.Chk.True(chksum == computedAddr.Checksum()) // integrity check

	if computedAddr != h {
		return nil // false positive
	}

	return data
}
