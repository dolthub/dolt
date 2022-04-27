// Copyright 2019 Dolthub, Inc.
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
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"sync/atomic"

	"github.com/golang/snappy"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// Do not read more than 128MB at a time.
const maxReadSize = 128 * 1024 * 1024

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

type indexEntry interface {
	Offset() uint64
	Length() uint32
}

type indexResult struct {
	o uint64
	l uint32
}

func (ir indexResult) Offset() uint64 {
	return ir.o
}

func (ir indexResult) Length() uint32 {
	return ir.l
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
	prefixes              []uint64
	chunkCount            uint32
	totalUncompressedData uint64
	r                     tableReaderAt
	blockSize             uint64
}

// newTableReader parses a valid nbs table byte stream and returns a reader. buff must end with an NBS index
// and footer, though it may contain an unspecified number of bytes before that data. r should allow
// retrieving any desired range of bytes from the table.
func newTableReader(index tableIndex, r tableReaderAt, blockSize uint64) (tableReader, error) {
	p, err := index.Prefixes()
	if err != nil {
		return tableReader{}, err
	}
	return tableReader{
		index,
		p,
		index.ChunkCount(),
		index.TotalUncompressedData(),
		r,
		blockSize,
	}, nil
}

// Scan across (logically) two ordered slices of address prefixes.
func (tr tableReader) hasMany(addrs []hasRecord) (bool, error) {
	// TODO: Use findInIndex if (tr.chunkCount - len(addrs)*Log2(tr.chunkCount)) > (tr.chunkCount - len(addrs))

	filterIdx := uint32(0)
	filterLen := uint32(tr.chunkCount)

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
			m, err := tr.EntrySuffixMatches(j, addr.a)
			if err != nil {
				return false, err
			}
			if m {
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
	_, ok, err := tr.Lookup(&h)
	return ok, err
}

// returns the storage associated with |h|, iff present. Returns nil if absent. On success,
// the returned byte slice directly references the underlying storage.
func (tr tableReader) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	e, found, err := tr.Lookup(&h)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	offset := e.Offset()
	length := uint64(e.Length())
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
	a      *addr
	offset uint64
	length uint32
}

type offsetRecSlice []offsetRec

func (hs offsetRecSlice) Len() int           { return len(hs) }
func (hs offsetRecSlice) Less(i, j int) bool { return hs[i].offset < hs[j].offset }
func (hs offsetRecSlice) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

var _ chunkReadPlanner = tableReader{}
var _ chunkReader = tableReader{}

func (tr tableReader) readCompressedAtOffsets(
	ctx context.Context,
	rb readBatch,
	found func(context.Context, CompressedChunk),
	stats *Stats,
) error {
	return tr.readAtOffsetsWithCB(ctx, rb, stats, func(ctx context.Context, cmp CompressedChunk) error {
		found(ctx, cmp)
		return nil
	})
}

func (tr tableReader) readAtOffsets(
	ctx context.Context,
	rb readBatch,
	found func(context.Context, *chunks.Chunk),
	stats *Stats,
) error {
	return tr.readAtOffsetsWithCB(ctx, rb, stats, func(ctx context.Context, cmp CompressedChunk) error {
		chk, err := cmp.ToChunk()

		if err != nil {
			return err
		}

		found(ctx, &chk)
		return nil
	})
}

func (tr tableReader) readAtOffsetsWithCB(
	ctx context.Context,
	rb readBatch,
	stats *Stats,
	cb func(ctx context.Context, cmp CompressedChunk) error,
) error {
	readLength := rb.End() - rb.Start()
	buff := make([]byte, readLength)

	n, err := tr.r.ReadAtWithStats(ctx, buff, int64(rb.Start()), stats)
	if err != nil {
		return err
	}

	if uint64(n) != readLength {
		return errors.New("failed to read all data")
	}

	for i := range rb {
		cmp, err := rb.ExtractChunkFromRead(buff, i)
		if err != nil {
			return err
		}

		err = cb(ctx, cmp)
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
	eg *errgroup.Group,
	reqs []getRecord,
	found func(context.Context, *chunks.Chunk),
	stats *Stats) (bool, error) {

	// Pass #1: Iterate over |reqs| and |tr.prefixes| (both sorted by address) and build the set
	// of table locations which must be read in order to satisfy the getMany operation.
	offsetRecords, remaining, err := tr.findOffsets(reqs)
	if err != nil {
		return false, err
	}
	err = tr.getManyAtOffsets(ctx, eg, offsetRecords, found, stats)
	return remaining, err
}
func (tr tableReader) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (bool, error) {
	// Pass #1: Iterate over |reqs| and |tr.prefixes| (both sorted by address) and build the set
	// of table locations which must be read in order to satisfy the getMany operation.
	offsetRecords, remaining, err := tr.findOffsets(reqs)
	if err != nil {
		return false, err
	}
	err = tr.getManyCompressedAtOffsets(ctx, eg, offsetRecords, found, stats)
	return remaining, err
}

func (tr tableReader) getManyCompressedAtOffsets(ctx context.Context, eg *errgroup.Group, offsetRecords offsetRecSlice, found func(context.Context, CompressedChunk), stats *Stats) error {
	return tr.getManyAtOffsetsWithReadFunc(ctx, eg, offsetRecords, stats, func(
		ctx context.Context,
		rb readBatch,
		stats *Stats) error {
		return tr.readCompressedAtOffsets(ctx, rb, found, stats)
	})
}

func (tr tableReader) getManyAtOffsets(
	ctx context.Context,
	eg *errgroup.Group,
	offsetRecords offsetRecSlice,
	found func(context.Context, *chunks.Chunk),
	stats *Stats,
) error {
	return tr.getManyAtOffsetsWithReadFunc(ctx, eg, offsetRecords, stats, func(
		ctx context.Context,
		rb readBatch,
		stats *Stats) error {
		return tr.readAtOffsets(ctx, rb, found, stats)
	})
}

type readBatch offsetRecSlice

func (r readBatch) Start() uint64 {
	return r[0].offset
}

func (r readBatch) End() uint64 {
	last := r[len(r)-1]
	return last.offset + uint64(last.length)
}

func (s readBatch) ExtractChunkFromRead(buff []byte, idx int) (CompressedChunk, error) {
	rec := s[idx]
	chunkStart := rec.offset - s.Start()
	return NewCompressedChunk(hash.Hash(*rec.a), buff[chunkStart:chunkStart+uint64(rec.length)])
}

func toReadBatches(offsets offsetRecSlice, blockSize uint64) []readBatch {
	res := make([]readBatch, 0)
	var batch readBatch
	for i := 0; i < len(offsets); {
		rec := offsets[i]
		if batch == nil {
			batch = readBatch{rec}
			i++
			continue
		}

		if _, canRead := canReadAhead(rec, batch.Start(), batch.End(), blockSize); canRead {
			batch = append(batch, rec)
			i++
			continue
		}

		res = append(res, batch)
		batch = nil
	}
	if batch != nil {
		res = append(res, batch)
	}
	return res
}

func (tr tableReader) getManyAtOffsetsWithReadFunc(
	ctx context.Context,
	eg *errgroup.Group,
	offsetRecords offsetRecSlice,
	stats *Stats,
	readAtOffsets func(
		ctx context.Context,
		rb readBatch,
		stats *Stats) error,
) error {
	batches := toReadBatches(offsetRecords, tr.blockSize)
	var idx int32
	readBatches := func() error {
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			i := atomic.AddInt32(&idx, 1) - 1
			if int(i) >= len(batches) {
				return nil
			}
			rb := batches[i]
			err := readAtOffsets(ctx, rb, stats)
			if err != nil {
				return err
			}
		}
	}
	ioParallelism := 4
	for i := 0; i < ioParallelism; i++ {
		eg.Go(readBatches)
	}

	return nil
}

// findOffsets iterates over |reqs| and |tr.prefixes| (both sorted by
// address) to build the set of table locations which must be read in order to
// find each chunk specified by |reqs|. If this table contains all requested
// chunks remaining will be set to false upon return. If some are not here,
// then remaining will be true. The result offsetRecSlice is sorted in offset
// order.
func (tr tableReader) findOffsets(reqs []getRecord) (ors offsetRecSlice, remaining bool, err error) {
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
			m, err := tr.EntrySuffixMatches(j, req.a)
			if err != nil {
				return nil, false, err
			}
			if m {
				reqs[i].found = true
				entry, err := tr.IndexEntry(j, nil)
				if err != nil {
					return nil, false, err
				}
				ors = append(ors, offsetRec{req.a, entry.Offset(), entry.Length()})
				break
			}
		}

		if !reqs[i].found {
			remaining = true
		}
	}

	sort.Sort(ors)
	return ors, remaining, nil
}

func canReadAhead(fRec offsetRec, curStart, curEnd, blockSize uint64) (newEnd uint64, canRead bool) {
	if fRec.offset < curEnd {
		// |offsetRecords| will contain an offsetRecord for *every* chunkRecord whose address
		// prefix matches the prefix of a requested address. If the set of requests contains
		// addresses which share a common prefix, then it's possible for multiple offsetRecords
		// to reference the same table offset position. In that case, we'll see sequential
		// offsetRecords with the same fRec.offset.
		return curEnd, true
	}

	if curEnd-curStart >= maxReadSize {
		return curEnd, false
	}

	if fRec.offset-curEnd > blockSize {
		return curEnd, false
	}

	return fRec.offset + uint64(fRec.length), true
}

func (tr tableReader) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool, err error) {
	var offsetRecords offsetRecSlice
	// Pass #1: Build the set of table locations which must be read in order to find all the elements of |reqs| which are present in this table.
	offsetRecords, remaining, err = tr.findOffsets(reqs)
	if err != nil {
		return 0, false, err
	}

	// Now |offsetRecords| contains all locations within the table which must
	// be searched (note that there may be duplicates of a particular
	// location). Scan forward, grouping sequences of reads into large physical
	// reads.

	var readStart, readEnd uint64
	readStarted := false

	for i := 0; i < len(offsetRecords); {
		rec := offsetRecords[i]
		length := rec.length

		if !readStarted {
			readStarted = true
			reads++
			readStart = rec.offset
			readEnd = readStart + uint64(length)
			i++
			continue
		}

		if newReadEnd, canRead := canReadAhead(rec, readStart, readEnd, tr.blockSize); canRead {
			readEnd = newReadEnd
			i++
			continue
		}

		readStarted = false
	}

	return
}

func (tr tableReader) extract(ctx context.Context, chunks chan<- extractRecord) error {
	sendChunk := func(or offsetRec) error {
		buff := make([]byte, or.length)
		n, err := tr.r.ReadAtWithStats(ctx, buff, int64(or.offset), &Stats{})
		if err != nil {
			return err
		}
		if uint32(n) != or.length {
			return errors.New("did not read all data")
		}
		cmp, err := NewCompressedChunk(hash.Hash(*or.a), buff)

		if err != nil {
			return err
		}

		chnk, err := cmp.ToChunk()

		if err != nil {
			return err
		}

		chunks <- extractRecord{a: *or.a, data: chnk.Data()}
		return nil
	}

	var ors offsetRecSlice
	for i := uint32(0); i < tr.chunkCount; i++ {
		a := new(addr)
		e, err := tr.IndexEntry(i, a)
		if err != nil {
			return err
		}
		ors = append(ors, offsetRec{a, e.Offset(), e.Length()})
	}
	sort.Sort(ors)
	for _, or := range ors {
		err := sendChunk(or)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tr tableReader) reader(ctx context.Context) (io.Reader, error) {
	i, _ := tr.index()
	return io.LimitReader(&readerAdapter{tr.r, 0, ctx}, int64(i.TableFileSize())), nil
}

func (tr tableReader) size() (uint64, error) {
	i, err := tr.index()
	if err != nil {
		return 0, err
	}
	return i.TableFileSize(), nil
}

func (tr tableReader) Close() error {
	return tr.tableIndex.Close()
}

func (tr tableReader) Clone() (tableReader, error) {
	ti, err := tr.tableIndex.Clone()
	if err != nil {
		return tableReader{}, err
	}
	return tableReader{ti, tr.prefixes, tr.chunkCount, tr.totalUncompressedData, tr.r, tr.blockSize}, nil
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
