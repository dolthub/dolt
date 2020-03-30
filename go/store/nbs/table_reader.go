// Copyright 2019 Liquidata, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"sync"

	"github.com/golang/snappy"

	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

// CompressedChunk represents a chunk of data in a table file which is still compressed via snappy.
type CompressedChunk struct {
	// H is the hash of the chunk
	H hash.Hash

	// FullCompressedChunk is the entirety of the compressed chunk data including the crc
	FullCompressedChunk []byte

	// CompressedData is just the snappy encoded byte buffer that stores the chunk data
	CompressedData []byte
}

// NewCompressedChunk creates a CompressedChunk
func NewCompressedChunk(h hash.Hash, buff []byte) (CompressedChunk, error) {
	dataLen := uint64(len(buff)) - checksumSize

	chksum := binary.BigEndian.Uint32(buff[dataLen:])
	compressedData := buff[:dataLen]

	if chksum != crc(compressedData) {
		return CompressedChunk{}, errors.New("checksum error")
	}

	return CompressedChunk{H: h, FullCompressedChunk: buff, CompressedData: compressedData}, nil
}

// ToChunk snappy decodes the compressed data and returns a chunks.Chunk
func (cmp CompressedChunk) ToChunk() (chunks.Chunk, error) {
	data, err := snappy.Decode(nil, cmp.CompressedData)

	if err != nil {
		return chunks.Chunk{}, err
	}

	return chunks.NewChunkWithHash(cmp.H, data), nil
}

func ChunkToCompressedChunk(chunk chunks.Chunk) CompressedChunk {
	compressed := snappy.Encode(nil, chunk.Data())
	length := len(compressed)
	compressed = append(compressed, []byte{0, 0, 0, 0}...)
	binary.BigEndian.PutUint32(compressed[length:], crc(compressed[:length]))
	return CompressedChunk{H: chunk.Hash(), FullCompressedChunk: compressed, CompressedData: compressed[:length]}
}

// Hash returns the hash of the data
func (cmp CompressedChunk) Hash() hash.Hash {
	return cmp.H
}

// IsEmpty returns true if the chunk contains no data.
func (cmp CompressedChunk) IsEmpty() bool {
	return len(cmp.CompressedData) == 0 || (len(cmp.CompressedData) == 1 && cmp.CompressedData[0] == 0)
}

var EmptyCompressedChunk CompressedChunk

func init() {
	EmptyCompressedChunk = ChunkToCompressedChunk(chunks.EmptyChunk)
}

// ErrInvalidTableFile is an error returned when a table file is corrupt or invalid.
var ErrInvalidTableFile = errors.New("invalid or corrupt table file")

type tableIndex struct {
	chunkCount            uint32
	totalUncompressedData uint64
	prefixes, offsets     []uint64
	lengths, ordinals     []uint32
	suffixes              []byte
}

type tableReaderAt interface {
	ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error)
}

// tableReader implements get & has queries against a single nbs table. goroutine safe.
// |blockSize| refers to the block-size of the underlying storage. We assume that, each
// time we read data, we actually have to read in blocks of this size. So, we're willing
// to tolerate up to |blockSize| overhead each time we read a chunk, if it helps us group
// more chunks together into a single read request to backing storage.
type tableReader struct {
	tableIndex
	r         tableReaderAt
	blockSize uint64
}

// parses a valid nbs tableIndex from a byte stream. |buff| must end with an NBS index
// and footer, though it may contain an unspecified number of bytes before that data.
// |tableIndex| doesn't keep alive any references to |buff|.
func parseTableIndex(buff []byte) (tableIndex, error) {
	pos := int64(len(buff))

	// footer
	pos -= magicNumberSize

	if string(buff[pos:]) != magicNumber {
		return tableIndex{}, ErrInvalidTableFile
	}

	// total uncompressed chunk data
	pos -= uint64Size

	if pos < 0 {
		return tableIndex{}, ErrInvalidTableFile
	}

	totalUncompressedData := binary.BigEndian.Uint64(buff[pos:])

	pos -= uint32Size

	if pos < 0 {
		return tableIndex{}, ErrInvalidTableFile
	}

	chunkCount := binary.BigEndian.Uint32(buff[pos:])

	// index
	suffixesSize := int64(chunkCount) * addrSuffixSize
	pos -= suffixesSize

	if pos < 0 {
		return tableIndex{}, ErrInvalidTableFile
	}

	suffixes := make([]byte, suffixesSize)
	copy(suffixes, buff[pos:])

	lengthsSize := int64(chunkCount) * lengthSize
	pos -= lengthsSize

	if pos < 0 {
		return tableIndex{}, ErrInvalidTableFile
	}

	lengths, offsets := computeOffsets(chunkCount, buff[pos:pos+lengthsSize])

	tuplesSize := int64(chunkCount) * prefixTupleSize
	pos -= tuplesSize

	if pos < 0 {
		return tableIndex{}, ErrInvalidTableFile
	}

	prefixes, ordinals := computePrefixes(chunkCount, buff[pos:pos+tuplesSize])

	return tableIndex{
		chunkCount, totalUncompressedData,
		prefixes, offsets,
		lengths, ordinals,
		suffixes,
	}, nil
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
	return bytes.Equal(h[addrPrefixSize:], ti.suffixes[li:li+addrSuffixSize])
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

// newTableReader parses a valid nbs table byte stream and returns a reader. buff must end with an NBS index
// and footer, though it may contain an unspecified number of bytes before that data. r should allow
// retrieving any desired range of bytes from the table.
func newTableReader(index tableIndex, r tableReaderAt, blockSize uint64) tableReader {
	return tableReader{index, r, blockSize}
}

// Scan across (logically) two ordered slices of address prefixes.
func (tr tableReader) hasMany(addrs []hasRecord) (bool, error) {
	// TODO: Use findInIndex if (tr.chunkCount - len(addrs)*Log2(tr.chunkCount)) > (tr.chunkCount - len(addrs))

	filterIdx := uint32(0)
	filterLen := uint32(len(tr.prefixes))

	var remaining bool
	for i, addr := range addrs {
		if addr.has {
			continue
		}

		for filterIdx < filterLen && addr.prefix > tr.prefixes[filterIdx] {
			filterIdx++
		}

		if filterIdx >= filterLen {
			return true, nil
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

	return remaining, nil
}

func (tr tableReader) count() (uint32, error) {
	return tr.chunkCount, nil
}

func (tr tableReader) uncompressedLen() (uint64, error) {
	return tr.totalUncompressedData, nil
}

func (tr tableReader) index() (tableIndex, error) {
	return tr.tableIndex, nil
}

// returns true iff |h| can be found in this table.
func (tr tableReader) has(h addr) (bool, error) {
	ordinal := tr.lookupOrdinal(h)
	count, err := tr.count()

	if err != nil {
		return false, err
	}

	return ordinal < count, nil
}

// returns the storage associated with |h|, iff present. Returns nil if absent. On success,
// the returned byte slice directly references the underlying storage.
func (tr tableReader) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	ordinal := tr.lookupOrdinal(h)
	cnt, err := tr.count()

	if err != nil {
		return nil, err
	}

	if ordinal == cnt {
		return nil, nil
	}

	offset := tr.offsets[ordinal]
	length := uint64(tr.lengths[ordinal])
	buff := make([]byte, length) // TODO: Avoid this allocation for every get

	n, err := tr.r.ReadAtWithStats(ctx, buff, int64(offset), stats)

	if err != nil {
		return nil, err
	}

	if n != int(length) {
		return nil, errors.New("failed to read all data")
	}

	cmp, err := NewCompressedChunk(hash.Hash(h), buff)

	if err != nil {
		return nil, err
	}

	if len(cmp.CompressedData) == 0 {
		return nil, errors.New("failed to get data")
	}

	chnk, err := cmp.ToChunk()

	if err != nil {
		return nil, err
	}

	return chnk.Data(), nil
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

func (tr tableReader) readCompressedAtOffsets(
	ctx context.Context,
	readStart, readEnd uint64,
	reqs []getRecord,
	offsets offsetRecSlice,
	foundCmpChunks chan<- CompressedChunk,
	stats *Stats,
) error {
	return tr.readAtOffsetsWithCB(ctx, readStart, readEnd, reqs, offsets, stats, func(cmp CompressedChunk) error {
		foundCmpChunks <- cmp
		return nil
	})
}

func (tr tableReader) readAtOffsets(
	ctx context.Context,
	readStart, readEnd uint64,
	reqs []getRecord,
	offsets offsetRecSlice,
	foundChunks chan<- *chunks.Chunk,
	stats *Stats,
) error {
	return tr.readAtOffsetsWithCB(ctx, readStart, readEnd, reqs, offsets, stats, func(cmp CompressedChunk) error {
		chk, err := cmp.ToChunk()

		if err != nil {
			return err
		}

		foundChunks <- &chk
		return nil
	})
}

func (tr tableReader) readAtOffsetsWithCB(
	ctx context.Context,
	readStart, readEnd uint64,
	reqs []getRecord,
	offsets offsetRecSlice,
	stats *Stats,
	cb func(cmp CompressedChunk) error,
) error {
	readLength := readEnd - readStart
	buff := make([]byte, readLength)

	n, err := tr.r.ReadAtWithStats(ctx, buff, int64(readStart), stats)

	if err != nil {
		return err
	}

	if uint64(n) != readLength {
		return errors.New("failed to read all data")
	}

	for _, rec := range offsets {
		if rec.offset < readStart {
			return errors.New("offset before the start")
		}

		localStart := rec.offset - readStart
		localEnd := localStart + uint64(tr.lengths[rec.ordinal])

		if localEnd > readLength {
			return errors.New("length goes past the end")
		}

		cmp, err := NewCompressedChunk(hash.Hash(*rec.a), buff[localStart:localEnd])

		if err != nil {
			return err
		}

		err = cb(cmp)

		if err != nil {
			return err
		}
	}

	return nil
}

// getMany retrieves multiple stored blocks and optimizes by attempting to read in larger physical
// blocks which contain multiple stored blocks. |reqs| must be sorted by address prefix.
func (tr tableReader) getMany(
	ctx context.Context,
	reqs []getRecord,
	foundChunks chan<- *chunks.Chunk,
	wg *sync.WaitGroup,
	ae *atomicerr.AtomicError,
	stats *Stats) bool {

	// Pass #1: Iterate over |reqs| and |tr.prefixes| (both sorted by address) and build the set
	// of table locations which must be read in order to satisfy the getMany operation.
	offsetRecords, remaining := tr.findOffsets(reqs)
	tr.getManyAtOffsets(ctx, reqs, offsetRecords, foundChunks, wg, ae, stats)
	return remaining
}
func (tr tableReader) getManyCompressed(ctx context.Context, reqs []getRecord, foundCmpChunks chan<- CompressedChunk, wg *sync.WaitGroup, ae *atomicerr.AtomicError, stats *Stats) bool {
	// Pass #1: Iterate over |reqs| and |tr.prefixes| (both sorted by address) and build the set
	// of table locations which must be read in order to satisfy the getMany operation.
	offsetRecords, remaining := tr.findOffsets(reqs)
	tr.getManyCompressedAtOffsets(ctx, reqs, offsetRecords, foundCmpChunks, wg, ae, stats)
	return remaining
}

func (tr tableReader) getManyCompressedAtOffsets(ctx context.Context, reqs []getRecord, offsetRecords offsetRecSlice, foundCmpChunks chan<- CompressedChunk, wg *sync.WaitGroup, ae *atomicerr.AtomicError, stats *Stats) {
	tr.getManyAtOffsetsWithReadFunc(ctx, reqs, offsetRecords, wg, ae, stats, func(
		ctx context.Context,
		readStart, readEnd uint64,
		reqs []getRecord,
		offsets offsetRecSlice,
		stats *Stats) error {

		return tr.readCompressedAtOffsets(ctx, readStart, readEnd, reqs, offsets, foundCmpChunks, stats)
	})
}

func (tr tableReader) getManyAtOffsets(
	ctx context.Context,
	reqs []getRecord,
	offsetRecords offsetRecSlice,
	foundChunks chan<- *chunks.Chunk,
	wg *sync.WaitGroup,
	ae *atomicerr.AtomicError,
	stats *Stats,
) {
	tr.getManyAtOffsetsWithReadFunc(ctx, reqs, offsetRecords, wg, ae, stats, func(
		ctx context.Context,
		readStart, readEnd uint64,
		reqs []getRecord,
		offsets offsetRecSlice,
		stats *Stats) error {

		return tr.readAtOffsets(ctx, readStart, readEnd, reqs, offsets, foundChunks, stats)
	})
}

func (tr tableReader) getManyAtOffsetsWithReadFunc(
	ctx context.Context,
	reqs []getRecord,
	offsetRecords offsetRecSlice,
	wg *sync.WaitGroup,
	ae *atomicerr.AtomicError,
	stats *Stats,
	readAtOffsets func(
		ctx context.Context,
		readStart, readEnd uint64,
		reqs []getRecord,
		offsets offsetRecSlice,
		stats *Stats) error,
) {
	type readBatch struct {
		batch     offsetRecSlice
		readStart uint64
		readEnd   uint64
	}
	buildBatches := func(batchCh chan<- readBatch) {
		// |offsetRecords| contains all locations within the table
		// which must be search in sorted order and without
		// duplicates. Now scan forward, grouping sequences of reads
		// into larger physical reads.

		var batch offsetRecSlice
		var readStart, readEnd uint64

		for i := 0; i < len(offsetRecords); {
			if ae.IsSet() {
				break
			}

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

			batchCh <- readBatch{batch, readStart, readEnd}
			batch = nil
		}

		if !ae.IsSet() && batch != nil {
			batchCh <- readBatch{batch, readStart, readEnd}
		}
	}
	readBatches := func(batchCh <-chan readBatch) {
		for rb := range batchCh {
			if !ae.IsSet() {
				err := readAtOffsets(ctx, rb.readStart, rb.readEnd, reqs, rb.batch, stats)
				ae.SetIfError(err)
			}
		}
	}

	ioParallelism := 4

	batchCh := make(chan readBatch, 128)
	go func() {
		defer close(batchCh)
		buildBatches(batchCh)
	}()
	wg.Add(ioParallelism)
	for i := 0; i < ioParallelism; i++ {
		go func() {
			defer wg.Done()
			readBatches(batchCh)
		}()
	}
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
				break
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

func (tr tableReader) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool, err error) {
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

func (tr tableReader) extract(ctx context.Context, chunks chan<- extractRecord) error {
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
	n, err := tr.r.ReadAtWithStats(ctx, buff, int64(tr.offsets[0]), &Stats{})

	if err != nil {
		return err
	}

	if uint64(n) != chunkLen {
		return errors.New("did not read all data")
	}

	sendChunk := func(i uint32) error {
		localOffset := tr.offsets[i] - tr.offsets[0]

		cmp, err := NewCompressedChunk(hash.Hash(hashes[i]), buff[localOffset:localOffset+uint64(tr.lengths[i])])

		if err != nil {
			return err
		}

		chnk, err := cmp.ToChunk()

		if err != nil {
			return err
		}

		chunks <- extractRecord{a: hashes[i], data: chnk.Data()}
		return nil
	}

	for i := uint32(0); i < tr.chunkCount; i++ {
		err = sendChunk(i)

		if err != nil {
			return err
		}
	}

	return nil
}

func (tr tableReader) reader(ctx context.Context) (io.Reader, error) {
	return &readerAdapter{tr.r, 0, ctx}, nil
}

type readerAdapter struct {
	rat tableReaderAt
	off int64
	ctx context.Context
}

func (ra *readerAdapter) Read(p []byte) (n int, err error) {
	n, err = ra.rat.ReadAtWithStats(ra.ctx, p, ra.off, &Stats{})
	ra.off += int64(n)
	return
}
