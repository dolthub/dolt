// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/golang/snappy"
)

type tableIndex struct {
	chunkCount            uint32
	totalUncompressedData uint64
	prefixes, offsets     []uint64
	lengths, ordinals     []uint32
	suffixes              []byte
}

type tableReaderAt interface {
	ReadAtWithStats(p []byte, off int64, stats *Stats) (n int, err error)
}

// tableReader implements get & has queries against a single nbs table. goroutine safe.
// |blockSize| refers to the block-size of the underlying storage. We assume that, each time we read data, we actually have to read in blocks of this size. So, we're willing to tolerate up to |blockSize| overhead each time we read a chunk, if it helps us group more chunks together into a single read request to backing storage.
type tableReader struct {
	tableIndex
	r         tableReaderAt
	blockSize uint64
}

// parses a valid nbs tableIndex from a byte stream. |buff| must end with an NBS index and footer, though it may contain an unspecified number of bytes before that data. |tableIndex| doesn't keep alive any references to |buff|.
func parseTableIndex(buff []byte) tableIndex {
	pos := uint64(len(buff))

	// footer
	pos -= magicNumberSize
	d.Chk.True(string(buff[pos:]) == magicNumber)

	// total uncompressed chunk data
	pos -= uint64Size
	totalUncompressedData := binary.BigEndian.Uint64(buff[pos:])

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
		chunkCount, totalUncompressedData,
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
func newTableReader(index tableIndex, r tableReaderAt, blockSize uint64) tableReader {
	return tableReader{index, r, blockSize}
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

func (tr tableReader) uncompressedLen() uint64 {
	return tr.totalUncompressedData
}

func (tr tableReader) index() tableIndex {
	return tr.tableIndex
}

// returns true iff |h| can be found in this table.
func (tr tableReader) has(h addr) bool {
	ordinal := tr.lookupOrdinal(h)
	return ordinal < tr.count()
}

// returns the storage associated with |h|, iff present. Returns nil if absent. On success,
// the returned byte slice directly references the underlying storage.
func (tr tableReader) get(h addr, stats *Stats) (data []byte) {
	ordinal := tr.lookupOrdinal(h)
	if ordinal == tr.count() {
		return
	}

	offset := tr.offsets[ordinal]
	length := uint64(tr.lengths[ordinal])
	buff := make([]byte, length) // TODO: Avoid this allocation for every get

	n, err := tr.r.ReadAtWithStats(buff, int64(offset), stats)
	d.Chk.NoError(err)
	d.Chk.True(n == int(length))
	data = tr.parseChunk(buff)
	d.Chk.True(data != nil)

	return
}

type offsetRec struct {
	a       *addr
	ordinal uint32
	offset  uint64
}

type offsetRecSlice []offsetRec

func (hs offsetRecSlice) Len() int           { return len(hs) }
func (hs offsetRecSlice) Less(i, j int) bool { return hs[i].offset < hs[j].offset }
func (hs offsetRecSlice) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

func (tr tableReader) readAtOffsets(
	readStart, readEnd uint64,
	reqs []getRecord,
	offsets offsetRecSlice,
	foundChunks chan *chunks.Chunk,
	wg *sync.WaitGroup,
	stats *Stats,
) {

	readLength := readEnd - readStart
	buff := make([]byte, readLength)

	n, err := tr.r.ReadAtWithStats(buff, int64(readStart), stats)

	d.Chk.NoError(err)
	d.Chk.True(uint64(n) == readLength)

	for _, rec := range offsets {
		d.Chk.True(rec.offset >= readStart)
		localStart := rec.offset - readStart
		localEnd := localStart + uint64(tr.lengths[rec.ordinal])
		d.Chk.True(localEnd <= readLength)
		data := tr.parseChunk(buff[localStart:localEnd])
		c := chunks.NewChunkWithHash(hash.Hash(*rec.a), data)
		foundChunks <- &c
	}

	wg.Done()

}

// getMany retrieves multiple stored blocks and optimizes by attempting to read in larger physical
// blocks which contain multiple stored blocks. |reqs| must be sorted by address prefix.
func (tr tableReader) getMany(
	reqs []getRecord,
	foundChunks chan *chunks.Chunk,
	wg *sync.WaitGroup,
	stats *Stats,
) (remaining bool) {
	// Pass #1: Iterate over |reqs| and |tr.prefixes| (both sorted by address) and build the set
	// of table locations which must be read in order to satisfy the getMany operation.
	offsetRecords, remaining := tr.findOffsets(reqs)
	tr.getManyAtOffsets(reqs, offsetRecords, foundChunks, wg, stats)
	return remaining
}

func (tr tableReader) getManyAtOffsets(
	reqs []getRecord,
	offsetRecords offsetRecSlice,
	foundChunks chan *chunks.Chunk,
	wg *sync.WaitGroup,
	stats *Stats,
) {
	// Now |offsetRecords| contains all locations within the table which must be search (note
	// that there may be duplicates of a particular location). Sort by offset and scan forward,
	// grouping sequences of reads into large physical reads.

	var batch offsetRecSlice
	var readStart, readEnd uint64

	for i := 0; i < len(offsetRecords); {
		rec := offsetRecords[i]
		length := tr.lengths[rec.ordinal]

		if batch == nil {
			batch = make(offsetRecSlice, 1)
			batch[0] = offsetRecords[i]
			readStart = rec.offset
			readEnd = readStart + uint64(length)
			i++
			continue
		}

		if newReadEnd, canRead := canReadAhead(rec, tr.lengths[rec.ordinal], readStart, readEnd, tr.blockSize); canRead {
			batch = append(batch, rec)
			readEnd = newReadEnd
			i++
			continue
		}

		wg.Add(1)
		go tr.readAtOffsets(readStart, readEnd, reqs, batch, foundChunks, wg, stats)
		batch = nil
	}

	if batch != nil {
		wg.Add(1)
		go tr.readAtOffsets(readStart, readEnd, reqs, batch, foundChunks, wg, stats)
		batch = nil
	}

	return
}

// findOffsets iterates over |reqs| and |tr.prefixes| (both sorted by
// address) to build the set of table locations which must be read in order to
// find each chunk specified by |reqs|. If this table contains all requested
// chunks remaining will be set to false upon return. If some are not here,
// then remaining will be true. The result offsetRecSlice is sorted in offset
// order.
func (tr tableReader) findOffsets(reqs []getRecord) (ors offsetRecSlice, remaining bool) {
	filterIdx := uint32(0)
	filterLen := uint32(len(tr.prefixes))
	ors = make(offsetRecSlice, 0, len(reqs))

	// Iterate over |reqs| and |tr.prefixes| (both sorted by address) and build the set
	// of table locations which must be read in order to satisfy |reqs|.
	for i, req := range reqs {
		if req.found {
			continue
		}

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
				reqs[i].found = true
				ors = append(ors, offsetRec{req.a, tr.ordinals[j], tr.offsets[tr.ordinals[j]]})
			}
		}
	}

	sort.Sort(ors)
	return ors, remaining
}

func canReadAhead(fRec offsetRec, fLength uint32, readStart, readEnd, blockSize uint64) (newEnd uint64, canRead bool) {
	if fRec.offset < readEnd {
		// |offsetRecords| will contain an offsetRecord for *every* chunkRecord whose address
		// prefix matches the prefix of a requested address. If the set of requests contains
		// addresses which share a common prefix, then it's possible for multiple offsetRecords
		// to reference the same table offset position. In that case, we'll see sequential
		// offsetRecords with the same fRec.offset.
		return readEnd, true
	}

	if fRec.offset-readEnd > blockSize {
		return readEnd, false
	}

	return fRec.offset + uint64(fLength), true
}

// Fetches the byte stream of data logically encoded within the table starting at |pos|.
func (tr tableReader) parseChunk(buff []byte) []byte {
	dataLen := uint64(len(buff)) - checksumSize

	chksum := binary.BigEndian.Uint32(buff[dataLen:])
	d.Chk.True(chksum == crc(buff[:dataLen]))

	data, err := snappy.Decode(nil, buff[:dataLen])
	d.Chk.NoError(err)

	return data
}

func (tr tableReader) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool) {
	var offsetRecords offsetRecSlice
	// Pass #1: Build the set of table locations which must be read in order to find all the elements of |reqs| which are present in this table.
	offsetRecords, remaining = tr.findOffsets(reqs)

	// Now |offsetRecords| contains all locations within the table which must
	// be searched (note that there may be duplicates of a particular
	// location). Scan forward, grouping sequences of reads into large physical
	// reads.

	var readStart, readEnd uint64
	readStarted := false

	for i := 0; i < len(offsetRecords); {
		rec := offsetRecords[i]
		length := tr.lengths[rec.ordinal]

		if !readStarted {
			readStarted = true
			reads++
			readStart = rec.offset
			readEnd = readStart + uint64(length)
			i++
			continue
		}

		if newReadEnd, canRead := canReadAhead(rec, tr.lengths[rec.ordinal], readStart, readEnd, tr.blockSize); canRead {
			readEnd = newReadEnd
			i++
			continue
		}

		readStarted = false
	}

	return
}

func (tr tableReader) extract(chunks chan<- extractRecord) {
	// Build reverse lookup table from ordinal -> chunk hash
	hashes := make(addrSlice, len(tr.prefixes))
	for idx, prefix := range tr.prefixes {
		ordinal := tr.prefixIdxToOrdinal(uint32(idx))
		binary.BigEndian.PutUint64(hashes[ordinal][:], prefix)
		li := uint64(ordinal) * addrSuffixSize
		copy(hashes[ordinal][addrPrefixSize:], tr.suffixes[li:li+addrSuffixSize])
	}
	chunkLen := tr.offsets[tr.chunkCount-1] + uint64(tr.lengths[tr.chunkCount-1])
	buff := make([]byte, chunkLen)
	n, err := tr.r.ReadAtWithStats(buff, int64(tr.offsets[0]), &Stats{})
	d.Chk.NoError(err)
	d.Chk.True(uint64(n) == chunkLen)

	sendChunk := func(i uint32) {
		localOffset := tr.offsets[i] - tr.offsets[0]
		chunks <- extractRecord{a: hashes[i], data: tr.parseChunk(buff[localOffset : localOffset+uint64(tr.lengths[i])])}
	}

	for i := uint32(0); i < tr.chunkCount; i++ {
		sendChunk(i)
	}
}

func (tr tableReader) reader() io.Reader {
	return &readerAdapter{tr.r, 0}
}

type readerAdapter struct {
	rat tableReaderAt
	off int64
}

func (ra *readerAdapter) Read(p []byte) (n int, err error) {
	n, err = ra.rat.ReadAtWithStats(p, ra.off, &Stats{})
	ra.off += int64(n)
	return
}
