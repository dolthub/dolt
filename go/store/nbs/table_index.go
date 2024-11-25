// Copyright 2022 Dolthub, Inc.
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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"sync/atomic"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/hash"
)

var (
	ErrWrongBufferSize = errors.New("buffer length and/or capacity incorrect for chunkCount specified in footer")
	ErrWrongCopySize   = errors.New("could not copy enough bytes")
)

// By setting this to false, you can make tablefile index creation cheaper. In
// exchange, the panics which leaked table files create do not come with as
// much information.

var TableIndexGCFinalizerWithStackTrace = true

type tableIndex interface {
	// entrySuffixMatches returns true if the entry at index |idx| matches
	// the suffix of the address |h|. Used by |lookup| after finding
	// matching indexes based on |Prefixes|.
	entrySuffixMatches(idx uint32, h *hash.Hash) (bool, error)

	// indexEntry returns the |indexEntry| at |idx|. Optionally puts the
	// full address of that entry in |a| if |a| is not |nil|.
	indexEntry(idx uint32, a *hash.Hash) (indexEntry, error)

	// lookup returns an |indexEntry| for the chunk corresponding to the
	// provided address |h|. Second returns is |true| if an entry exists
	// and |false| otherwise.
	lookup(h *hash.Hash) (indexEntry, bool, error)

	// Ordinals returns a slice of indexes which maps the |i|th chunk in
	// the indexed file to its corresponding entry in index. The |i|th
	// entry in the result is the |i|th chunk in the indexed file, and its
	// corresponding value in the slice is the index entry that maps to it.
	ordinals() ([]uint32, error)

	// Prefixes returns the sorted slice of |uint64| |addr| prefixes; each
	// entry corresponds to an indexed chunk address.
	prefixes() ([]uint64, error)

	// chunkCount returns the total number of chunks in the indexed file.
	chunkCount() uint32

	// tableFileSize returns the total size of the indexed table file, in bytes.
	tableFileSize() uint64

	// totalUncompressedData returns the total uncompressed data size of
	// the table file. Used for informational statistics only.
	totalUncompressedData() uint64

	// Close releases any resources used by this tableIndex.
	Close() error

	// clone returns a |tableIndex| with the same contents which can be
	// |Close|d independently.
	clone() (tableIndex, error)
}

func ReadTableFooter(rd io.ReadSeeker) (chunkCount uint32, totalUncompressedData uint64, err error) {
	footerSize := int64(magicNumberSize + uint64Size + uint32Size)
	_, err = rd.Seek(-footerSize, io.SeekEnd)

	if err != nil {
		return 0, 0, err
	}

	footer, err := iohelp.ReadNBytes(rd, int(footerSize))

	if err != nil {
		return 0, 0, err
	}

	if string(footer[uint32Size+uint64Size:]) != magicNumber {
		// Give a nice error message if this is a table file format which we will support in the future.
		possibleDarc := string(footer[len(footer)-doltMagicSize:])
		if possibleDarc == doltMagicNumber {
			return 0, 0, ErrUnsupportedTableFileFormat
		}

		return 0, 0, ErrInvalidTableFile
	}

	chunkCount = binary.BigEndian.Uint32(footer)
	totalUncompressedData = binary.BigEndian.Uint64(footer[uint32Size:])

	return
}

// parses a valid nbs tableIndex from a byte stream. |buff| must end with an NBS index
// and footer and its length must match the expected indexSize for the chunkCount specified in the footer.
// Retains the buffer and does not allocate new memory except for offsets, computes on buff in place.
func parseTableIndex(ctx context.Context, buff []byte, q MemoryQuotaProvider) (onHeapTableIndex, error) {
	chunkCount, totalUncompressedData, err := ReadTableFooter(bytes.NewReader(buff))
	if err != nil {
		return onHeapTableIndex{}, err
	}

	chunks2 := chunkCount / 2
	chunks1 := chunkCount - chunks2
	offsetsBuff1, err := q.AcquireQuotaBytes(ctx, int(chunks1*offsetSize))
	if err != nil {
		return onHeapTableIndex{}, err
	}
	idx, err := newOnHeapTableIndex(buff, offsetsBuff1, chunkCount, totalUncompressedData, q)
	if err != nil {
		q.ReleaseQuotaBytes(len(offsetsBuff1))
	}
	return idx, err
}

// similar to parseTableIndex except that it uses the given |offsetsBuff1|
// instead of allocating the additional space.
func parseTableIndexWithOffsetBuff(buff []byte, offsetsBuff1 []byte, q MemoryQuotaProvider) (onHeapTableIndex, error) {
	chunkCount, totalUncompressedData, err := ReadTableFooter(bytes.NewReader(buff))
	if err != nil {
		return onHeapTableIndex{}, err
	}

	return newOnHeapTableIndex(buff, offsetsBuff1, chunkCount, totalUncompressedData, q)
}

// parseTableIndexByCopy reads the footer, copies indexSize(chunkCount) bytes, and parses an on heap table index.
// Useful to create an onHeapTableIndex without retaining the entire underlying array of data.
func parseTableIndexByCopy(ctx context.Context, buff []byte, q MemoryQuotaProvider) (onHeapTableIndex, error) {
	return readTableIndexByCopy(ctx, bytes.NewReader(buff), q)
}

// readTableIndexByCopy loads an index into memory from an io.ReadSeeker
// Caution: Allocates new memory for entire index
func readTableIndexByCopy(ctx context.Context, rd io.ReadSeeker, q MemoryQuotaProvider) (onHeapTableIndex, error) {
	chunkCount, totalUncompressedData, err := ReadTableFooter(rd)
	if err != nil {
		return onHeapTableIndex{}, err
	}
	idxSz := int64(indexSize(chunkCount) + footerSize)
	_, err = rd.Seek(-idxSz, io.SeekEnd)
	if err != nil {
		return onHeapTableIndex{}, err
	}

	if int64(int(idxSz)) != idxSz {
		return onHeapTableIndex{}, fmt.Errorf("table file index is too large to read on this platform. index size %d > max int.", idxSz)
	}

	buff, err := q.AcquireQuotaBytes(ctx, int(idxSz))
	if err != nil {
		return onHeapTableIndex{}, err
	}

	_, err = io.ReadFull(rd, buff)
	if err != nil {
		q.ReleaseQuotaBytes(len(buff))
		return onHeapTableIndex{}, err
	}

	chunks1 := chunkCount - (chunkCount / 2)
	offsets1Buff, err := q.AcquireQuotaBytes(ctx, int(chunks1*offsetSize))
	if err != nil {
		q.ReleaseQuotaBytes(len(buff))
		return onHeapTableIndex{}, err
	}

	idx, err := newOnHeapTableIndex(buff, offsets1Buff, chunkCount, totalUncompressedData, q)
	if err != nil {
		q.ReleaseQuotaBytes(len(buff))
		q.ReleaseQuotaBytes(len(offsets1Buff))
	}
	return idx, err
}

type onHeapTableIndex struct {
	// prefixTuples is a packed array of 12 byte tuples:
	// (8 byte addr prefix, 4 byte uint32 ordinal)
	// it is sorted by addr prefix, the ordinal value
	// can be used to lookup offset and addr suffix
	prefixTuples []byte

	// the offsets arrays contains packed uint64s
	offsets1 []byte
	offsets2 []byte

	// suffixes is a array of 12 byte addr suffixes
	suffixes []byte

	// footer contains in the table file footer
	footer []byte

	q      MemoryQuotaProvider
	refCnt *int32

	count          uint32
	tableFileSz    uint64
	uncompressedSz uint64
}

var _ tableIndex = &onHeapTableIndex{}

// newOnHeapTableIndex converts a table file index with stored lengths on
// |indexBuff| into an index with stored offsets. Since offsets are twice the
// size of a length, we need to allocate additional space to store all the
// offsets. It stores the first n - n/2 offsets in |offsetsBuff1| (the
// additional space) and the rest into the region of |indexBuff| previously
// occupied by lengths. |onHeapTableIndex| computes directly on the given
// |indexBuff| and |offsetsBuff1| buffers.
func newOnHeapTableIndex(indexBuff []byte, offsetsBuff1 []byte, count uint32, totalUncompressedData uint64, q MemoryQuotaProvider) (onHeapTableIndex, error) {
	if len(indexBuff) != int(indexSize(count)+footerSize) {
		return onHeapTableIndex{}, ErrWrongBufferSize
	}

	cnt64 := uint64(count)

	tuples := indexBuff[:prefixTupleSize*cnt64]
	lengths := indexBuff[prefixTupleSize*cnt64 : prefixTupleSize*cnt64+lengthSize*cnt64]
	suffixes := indexBuff[prefixTupleSize*cnt64+lengthSize*cnt64 : indexSize(count)]
	footer := indexBuff[indexSize(count):]

	chunks2 := cnt64 / 2

	r := NewOffsetsReader(bytes.NewReader(lengths))
	_, err := io.ReadFull(r, offsetsBuff1)
	if err != nil {
		return onHeapTableIndex{}, err
	}

	// reuse |lengths| for offsets
	offsetsBuff2 := lengths
	if chunks2 > 0 {
		b := offsetsBuff2[:chunks2*offsetSize]
		if _, err = io.ReadFull(r, b); err != nil {
			return onHeapTableIndex{}, err
		}
	}

	refCnt := new(int32)
	*refCnt = 1

	if TableIndexGCFinalizerWithStackTrace {
		stack := string(debug.Stack())
		runtime.SetFinalizer(refCnt, func(i *int32) {
			panic(fmt.Sprintf("OnHeapTableIndex %x not closed:\n%s", refCnt, stack))
		})
	} else {
		runtime.SetFinalizer(refCnt, func(i *int32) {
			panic(fmt.Sprintf("OnHeapTableIndex %x was not closed", refCnt))
		})
	}

	return onHeapTableIndex{
		refCnt:         refCnt,
		q:              q,
		prefixTuples:   tuples,
		offsets1:       offsetsBuff1,
		offsets2:       offsetsBuff2,
		suffixes:       suffixes,
		footer:         footer,
		count:          count,
		uncompressedSz: totalUncompressedData,
	}, nil
}

func (ti onHeapTableIndex) entrySuffixMatches(idx uint32, h *hash.Hash) (bool, error) {
	ord := ti.ordinalAt(idx)
	o := uint64(ord) * hash.SuffixLen
	b := ti.suffixes[o : o+hash.SuffixLen]
	return bytes.Equal(h[hash.PrefixLen:], b), nil
}

func (ti onHeapTableIndex) indexEntry(idx uint32, a *hash.Hash) (entry indexEntry, err error) {
	prefix, ord := ti.tupleAt(idx)

	if a != nil {
		binary.BigEndian.PutUint64(a[:], prefix)

		o := hash.SuffixLen * uint64(ord)
		b := ti.suffixes[o : o+hash.SuffixLen]
		copy(a[hash.PrefixLen:], b)
	}

	return ti.getIndexEntry(ord), nil
}

func (ti onHeapTableIndex) getIndexEntry(ord uint32) indexEntry {
	var prevOff uint64
	if ord == 0 {
		prevOff = 0
	} else {
		prevOff = ti.offsetAt(ord - 1)
	}
	ordOff := ti.offsetAt(ord)
	length := uint32(ordOff - prevOff)
	return indexResult{
		offset: prevOff,
		length: length,
	}
}

func (ti onHeapTableIndex) lookup(h *hash.Hash) (indexEntry, bool, error) {
	ord, err := ti.lookupOrdinal(h)
	if err != nil {
		return indexResult{}, false, err
	}
	if ord == ti.count {
		return indexResult{}, false, nil
	}
	return ti.getIndexEntry(ord), true, nil
}

// lookupOrdinal returns the ordinal of |h| if present. Returns |ti.count|
// if absent.
func (ti onHeapTableIndex) lookupOrdinal(h *hash.Hash) (uint32, error) {
	prefix := h.Prefix()

	for idx := ti.findPrefix(prefix); idx < ti.count && ti.prefixAt(idx) == prefix; idx++ {
		m, err := ti.entrySuffixMatches(idx, h)
		if err != nil {
			return ti.count, err
		}
		if m {
			return ti.ordinalAt(idx), nil
		}
	}

	return ti.count, nil
}

// findPrefix returns the first position in |tr.prefixes| whose value == |prefix|.
// Returns |tr.chunkCount| if absent
func (ti onHeapTableIndex) findPrefix(prefix uint64) (idx uint32) {
	// NOTE: The golang impl of sort.Search is basically inlined here. This method can be called in
	// an extremely tight loop and inlining the code was a significant perf improvement.
	idx, j := 0, ti.count
	for idx < j {
		h := idx + (j-idx)/2 // avoid overflow when computing h
		// i ≤ h < j
		o := int64(prefixTupleSize * h)
		tmp := binary.BigEndian.Uint64(ti.prefixTuples[o : o+hash.PrefixLen])
		if tmp < prefix {
			idx = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	return
}

func (ti onHeapTableIndex) tupleAt(idx uint32) (prefix uint64, ord uint32) {
	off := prefixTupleSize * int64(idx)
	b := ti.prefixTuples[off : off+prefixTupleSize]

	prefix = binary.BigEndian.Uint64(b[:])
	ord = binary.BigEndian.Uint32(b[hash.PrefixLen:])
	return prefix, ord
}

func (ti onHeapTableIndex) prefixAt(idx uint32) uint64 {
	off := prefixTupleSize * int64(idx)
	b := ti.prefixTuples[off : off+hash.PrefixLen]
	return binary.BigEndian.Uint64(b)
}

func (ti onHeapTableIndex) ordinalAt(idx uint32) uint32 {
	off := prefixTupleSize*int64(idx) + hash.PrefixLen
	b := ti.prefixTuples[off : off+ordinalSize]
	return binary.BigEndian.Uint32(b)
}

// the first n - n/2 offsets are stored in offsetsB1 and the rest in offsetsB2
func (ti onHeapTableIndex) offsetAt(ord uint32) uint64 {
	chunks1 := ti.count - ti.count/2
	var b []byte
	if ord < chunks1 {
		off := offsetSize * int64(ord)
		b = ti.offsets1[off : off+offsetSize]
	} else {
		off := offsetSize * int64(ord-chunks1)
		b = ti.offsets2[off : off+offsetSize]
	}
	return binary.BigEndian.Uint64(b)
}

func (ti onHeapTableIndex) ordinals() ([]uint32, error) {
	// todo: |o| is not accounted for in the memory quota
	o := make([]uint32, ti.count)
	for i, off := uint32(0), uint64(0); i < ti.count; i, off = i+1, off+prefixTupleSize {
		b := ti.prefixTuples[off+hash.PrefixLen : off+prefixTupleSize]
		o[i] = binary.BigEndian.Uint32(b)
	}
	return o, nil
}

func (ti onHeapTableIndex) prefixes() ([]uint64, error) {
	// todo: |p| is not accounted for in the memory quota
	p := make([]uint64, ti.count)
	for i, off := uint32(0), uint64(0); i < ti.count; i, off = i+1, off+prefixTupleSize {
		b := ti.prefixTuples[off : off+hash.PrefixLen]
		p[i] = binary.BigEndian.Uint64(b)
	}
	return p, nil
}

func (ti onHeapTableIndex) hashAt(idx uint32) hash.Hash {
	// Get tuple
	off := prefixTupleSize * int64(idx)
	tuple := ti.prefixTuples[off : off+prefixTupleSize]

	// Get prefix, ordinal, and suffix
	prefix := tuple[:hash.PrefixLen]
	ord := binary.BigEndian.Uint32(tuple[hash.PrefixLen:])
	suffixOffset := uint64(ord) * hash.SuffixLen
	suffix := ti.suffixes[suffixOffset : suffixOffset+hash.SuffixLen]

	// Combine prefix and suffix to get hash
	buf := [hash.ByteLen]byte{}
	copy(buf[:hash.PrefixLen], prefix)
	copy(buf[hash.PrefixLen:], suffix)

	return buf
}

// prefixIdxLBound returns the first position in |tr.prefixes| whose value is <= |prefix|.
// will return index less than where prefix would be if prefix is not found.
func (ti onHeapTableIndex) prefixIdxLBound(prefix uint64) uint32 {
	l, r := uint32(0), ti.count
	for l < r {
		m := l + (r-l)/2 // find middle, rounding down
		if ti.prefixAt(m) < prefix {
			l = m + 1
		} else {
			r = m
		}
	}

	return l
}

// prefixIdxLBound returns the first position in |tr.prefixes| whose value is >= |prefix|.
// will return index greater than where prefix would be if prefix is not found.
func (ti onHeapTableIndex) prefixIdxUBound(prefix uint64) (idx uint32) {
	l, r := uint32(0), ti.count
	for l < r {
		m := l + (r-l+1)/2 // find middle, rounding up
		if m >= ti.count { // prevent index out of bounds
			return r
		}
		pre := ti.prefixAt(m)
		if pre <= prefix {
			l = m
		} else {
			r = m - 1
		}
	}

	return l
}

func (ti onHeapTableIndex) padStringAndDecode(s string, p string) uint64 {
	if len(p) != 1 {
		panic("pad string must be of length 1") // This is a programmer error that should never get out of PR.
	}

	for len(s) < hash.StringLen {
		if p == "0" {
			s = s + p // Pad on the right side.
		} else {
			s = p + s // pad on the left side.
		}
	}

	// Decode
	h := hash.Parse(s)
	return binary.BigEndian.Uint64(h[:])
}

func (ti onHeapTableIndex) chunkCount() uint32 {
	return ti.count
}

// tableFileSize returns the size of the table file that this index references.
// This assumes that the index follows immediately after the last chunk in the
// file and that the last chunk in the file is in the index.
func (ti onHeapTableIndex) tableFileSize() (sz uint64) {
	sz = footerSize
	if ti.count > 0 {
		last := ti.getIndexEntry(ti.count - 1)
		sz += last.Offset()
		sz += uint64(last.Length())
		sz += indexSize(ti.count)
	}
	return
}

func (ti onHeapTableIndex) totalUncompressedData() uint64 {
	return ti.uncompressedSz
}

func (ti onHeapTableIndex) Close() error {
	cnt := atomic.AddInt32(ti.refCnt, -1)
	if cnt < 0 {
		panic("Close() called and reduced ref count to < 0.")
	} else if cnt > 0 {
		return nil
	}

	runtime.SetFinalizer(ti.refCnt, nil)
	ti.q.ReleaseQuotaBytes(len(ti.prefixTuples) + len(ti.offsets1) + len(ti.offsets2) + len(ti.suffixes) + len(ti.footer))
	return nil
}

func (ti onHeapTableIndex) clone() (tableIndex, error) {
	cnt := atomic.AddInt32(ti.refCnt, 1)
	if cnt == 1 {
		panic("Clone() called after last Close(). This index is no longer valid.")
	}
	return ti, nil
}

func (ti onHeapTableIndex) ResolveShortHash(short []byte) ([]string, error) {
	// Convert to string
	shortHash := string(short)

	// Calculate length
	sLen := len(shortHash)

	// Find lower and upper bounds of prefix indexes to check
	var pIdxL, pIdxU uint32
	if sLen >= 13 {
		// Convert short string to prefix
		sPrefix := ti.padStringAndDecode(shortHash, "0")

		// Binary Search for prefix
		pIdxL = ti.findPrefix(sPrefix)

		// Prefix doesn't exist
		if pIdxL == ti.count {
			return []string{}, errors.New("can't find prefix")
		}

		// Find last equal
		pIdxU = pIdxL + 1
		for sPrefix == ti.prefixAt(pIdxU) {
			pIdxU++
		}
	} else {
		// Convert short string to lower and upper bounds
		sPrefixL := ti.padStringAndDecode(shortHash, "0")
		sPrefixU := ti.padStringAndDecode(shortHash, "v")

		// Binary search for lower and upper bounds
		pIdxL = ti.prefixIdxLBound(sPrefixL)
		pIdxU = ti.prefixIdxUBound(sPrefixU)
	}

	// Go through all equal prefixes
	var res []string
	for i := pIdxL; i < pIdxU; i++ {
		// Get full hash at index
		h := ti.hashAt(i)

		// Convert to string representation
		hashStr := h.String()

		// If it matches append to result
		if hashStr[:sLen] == shortHash {
			res = append(res, hashStr)
		}
	}

	return res, nil
}
