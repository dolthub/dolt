package nbs

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"sync/atomic"

	"github.com/dolthub/mmap-go"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

type tableIndex interface {
	// ChunkCount returns the total number of chunks in the indexed file.
	ChunkCount() uint32
	// EntrySuffixMatches returns true if the entry at index |idx| matches
	// the suffix of the address |h|. Used by |Lookup| after finding
	// matching indexes based on |Prefixes|.
	EntrySuffixMatches(idx uint32, h *addr) (bool, error)
	// IndexEntry returns the |indexEntry| at |idx|. Optionally puts the
	// full address of that entry in |a| if |a| is not |nil|.
	IndexEntry(idx uint32, a *addr) (indexEntry, error)
	// Lookup returns an |indexEntry| for the chunk corresponding to the
	// provided address |h|. Second returns is |true| if an entry exists
	// and |false| otherwise.
	Lookup(h *addr) (indexEntry, bool, error)
	// Ordinals returns a slice of indexes which maps the |i|th chunk in
	// the indexed file to its corresponding entry in index. The |i|th
	// entry in the result is the |i|th chunk in the indexed file, and its
	// corresponding value in the slice is the index entry that maps to it.
	Ordinals() ([]uint32, error)
	// Prefixes returns the sorted slice of |uint64| |addr| prefixes; each
	// entry corresponds to an indexed chunk address.
	Prefixes() ([]uint64, error)
	// TableFileSize returns the total size of the indexed table file, in bytes.
	TableFileSize() uint64
	// TotalUncompressedData returns the total uncompressed data size of
	// the table file. Used for informational statistics only.
	TotalUncompressedData() uint64

	// Close releases any resources used by this tableIndex.
	Close() error

	// Clone returns a |tableIndex| with the same contents which can be
	// |Close|d independently.
	Clone() (tableIndex, error)
}

//var _ tableIndex = mmapTableIndex{}
//
//// parses a valid nbs tableIndex from a byte stream. |buff| must end with an NBS index
//// and footer, though it may contain an unspecified number of bytes before that data.
//// |tableIndex| doesn't keep alive any references to |buff|.
//func parseTableIndex(buff []byte) (onHeapTableIndex, error) {
//	return ReadTableIndex(bytes.NewReader(buff))
//}
//
//func ReadTableIndex(rd io.ReadSeeker) (onHeapTableIndex, error) {
//	footerSize := int64(magicNumberSize + uint64Size + uint32Size)
//	_, err := rd.Seek(-footerSize, io.SeekEnd)
//
//	if err != nil {
//		return onHeapTableIndex{}, err
//	}
//
//	footer, err := iohelp.ReadNBytes(rd, int(footerSize))
//
//	if err != nil {
//		return onHeapTableIndex{}, err
//	}
//
//	if string(footer[uint32Size+uint64Size:]) != magicNumber {
//		return onHeapTableIndex{}, ErrInvalidTableFile
//	}
//
//	chunkCount := binary.BigEndian.Uint32(footer)
//	totalUncompressedData := binary.BigEndian.Uint64(footer[uint32Size:])
//
//	// index
//	suffixesSize := int64(chunkCount) * addrSuffixSize
//	lengthsSize := int64(chunkCount) * lengthSize
//	tuplesSize := int64(chunkCount) * prefixTupleSize
//	indexSize := suffixesSize + lengthsSize + tuplesSize
//
//	_, err = rd.Seek(-(indexSize + footerSize), io.SeekEnd)
//	if err != nil {
//		return onHeapTableIndex{}, ErrInvalidTableFile
//	}
//
//	prefixes, ordinals, err := streamComputePrefixes(chunkCount, rd)
//	if err != nil {
//		return onHeapTableIndex{}, ErrInvalidTableFile
//	}
//	lengths, offsets, err := streamComputeOffsets(chunkCount, rd)
//	if err != nil {
//		return onHeapTableIndex{}, ErrInvalidTableFile
//	}
//	suffixes, err := iohelp.ReadNBytes(rd, int(suffixesSize))
//	if err != nil {
//		return onHeapTableIndex{}, ErrInvalidTableFile
//	}
//
//	return onHeapTableIndex{
//		chunkCount, totalUncompressedData,
//		prefixes, offsets,
//		lengths, ordinals,
//		suffixes,
//	}, nil
//}
//
//type onHeapTableIndex struct {
//	chunkCount            uint32
//	totalUncompressedData uint64
//	prefixes, offsets     []uint64
//	lengths, ordinals     []uint32
//	suffixes              []byte
//}
//
//func (ti onHeapTableIndex) ChunkCount() uint32 {
//	return ti.chunkCount
//}
//
//// EntrySuffixMatches returns true IFF the suffix for prefix entry |idx|
//// matches the address |a|.
//func (ti onHeapTableIndex) EntrySuffixMatches(idx uint32, h *addr) bool {
//	li := uint64(ti.ordinals[idx]) * addrSuffixSize
//	return bytes.Equal(h[addrPrefixSize:], ti.suffixes[li:li+addrSuffixSize])
//}
//
//func (ti onHeapTableIndex) IndexEntry(idx uint32, a *addr) indexEntry {
//	ord := ti.ordinals[idx]
//	if a != nil {
//		binary.BigEndian.PutUint64(a[:], ti.prefixes[idx])
//		li := uint64(ord) * addrSuffixSize
//		copy(a[addrPrefixSize:], ti.suffixes[li:li+addrSuffixSize])
//	}
//	return indexResult{ti.offsets[ord], ti.lengths[ord]}
//}
//
//func (ti onHeapTableIndex) Lookup(h *addr) (indexEntry, bool) {
//	ord := ti.lookupOrdinal(h)
//	if ord == ti.chunkCount {
//		return indexResult{}, false
//	}
//	return indexResult{ti.offsets[ord], ti.lengths[ord]}, true
//}
//
//func (ti onHeapTableIndex) Ordinals() []uint32 {
//	return ti.ordinals
//}
//
//func (ti onHeapTableIndex) Prefixes() []uint64 {
//	return ti.prefixes
//}
//
//// TableFileSize returns the size of the table file that this index references.
//// This assumes that the index follows immediately after the last chunk in the
//// file and that the last chunk in the file is in the index.
//func (ti onHeapTableIndex) TableFileSize() uint64 {
//	if ti.chunkCount == 0 {
//		return footerSize
//	}
//	len, offset := ti.offsets[ti.chunkCount-1], uint64(ti.lengths[ti.chunkCount-1])
//	return offset + len + indexSize(ti.chunkCount) + footerSize
//}
//
//func (ti onHeapTableIndex) TotalUncompressedData() uint64 {
//	return ti.totalUncompressedData
//}
//
//func (ti onHeapTableIndex) Close() error {
//	return nil
//}
//
//func (ti onHeapTableIndex) Clone() tableIndex {
//	return ti
//}
//
//func (ti onHeapTableIndex) prefixIdxToOrdinal(idx uint32) uint32 {
//	return ti.ordinals[idx]
//}
//
//// prefixIdx returns the first position in |tr.prefixes| whose value ==
//// |prefix|. Returns |tr.chunkCount| if absent
//func (ti onHeapTableIndex) prefixIdx(prefix uint64) (idx uint32) {
//	// NOTE: The golang impl of sort.Search is basically inlined here. This method can be called in
//	// an extremely tight loop and inlining the code was a significant perf improvement.
//	idx, j := 0, ti.chunkCount
//	for idx < j {
//		h := idx + (j-idx)/2 // avoid overflow when computing h
//		// i ≤ h < j
//		if ti.prefixes[h] < prefix {
//			idx = h + 1 // preserves f(i-1) == false
//		} else {
//			j = h // preserves f(j) == true
//		}
//	}
//
//	return
//}
//
//// lookupOrdinal returns the ordinal of |h| if present. Returns |ti.chunkCount|
//// if absent.
//func (ti onHeapTableIndex) lookupOrdinal(h *addr) uint32 {
//	prefix := h.Prefix()
//
//	for idx := ti.prefixIdx(prefix); idx < ti.chunkCount && ti.prefixes[idx] == prefix; idx++ {
//		if ti.EntrySuffixMatches(idx, h) {
//			return ti.ordinals[idx]
//		}
//	}
//
//	return ti.chunkCount
//}
//
//func computeOffsets(count uint32, buff []byte) (lengths []uint32, offsets []uint64) {
//	lengths = make([]uint32, count)
//	offsets = make([]uint64, count)
//
//	lengths[0] = binary.BigEndian.Uint32(buff)
//
//	for i := uint64(1); i < uint64(count); i++ {
//		lengths[i] = binary.BigEndian.Uint32(buff[i*lengthSize:])
//		offsets[i] = offsets[i-1] + uint64(lengths[i-1])
//	}
//	return
//}
//
//func streamComputeOffsets(count uint32, rd io.Reader) (lengths []uint32, offsets []uint64, err error) {
//	lengths = make([]uint32, count)
//	offsets = make([]uint64, count)
//	buff := make([]byte, lengthSize)
//
//	n, err := rd.Read(buff)
//	if err != nil {
//		return nil, nil, err
//	}
//	if n != lengthSize {
//		return nil, nil, ErrNotEnoughBytes
//	}
//	lengths[0] = binary.BigEndian.Uint32(buff)
//
//	for i := uint64(1); i < uint64(count); i++ {
//		n, err := rd.Read(buff)
//		if err != nil {
//			return nil, nil, err
//		}
//		if n != lengthSize {
//			return nil, nil, ErrNotEnoughBytes
//		}
//		lengths[i] = binary.BigEndian.Uint32(buff)
//		offsets[i] = offsets[i-1] + uint64(lengths[i-1])
//	}
//
//	return
//}
//
//func computePrefixes(count uint32, buff []byte) (prefixes []uint64, ordinals []uint32) {
//	prefixes = make([]uint64, count)
//	ordinals = make([]uint32, count)
//
//	for i := uint64(0); i < uint64(count); i++ {
//		idx := i * prefixTupleSize
//		prefixes[i] = binary.BigEndian.Uint64(buff[idx:])
//		ordinals[i] = binary.BigEndian.Uint32(buff[idx+addrPrefixSize:])
//	}
//	return
//}
//
//func streamComputePrefixes(count uint32, rd io.Reader) (prefixes []uint64, ordinals []uint32, err error) {
//	prefixes = make([]uint64, count)
//	ordinals = make([]uint32, count)
//	buff := make([]byte, prefixTupleSize)
//
//	for i := uint64(0); i < uint64(count); i++ {
//		n, err := rd.Read(buff)
//		if err != nil {
//			return nil, nil, err
//		}
//		if n != prefixTupleSize {
//			return nil, nil, ErrNotEnoughBytes
//		}
//		prefixes[i] = binary.BigEndian.Uint64(buff)
//		ordinals[i] = binary.BigEndian.Uint32(buff[addrPrefixSize:])
//	}
//
//	return
//}

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
		return 0, 0, ErrInvalidTableFile
	}

	chunkCount = binary.BigEndian.Uint32(footer)
	totalUncompressedData = binary.BigEndian.Uint64(footer[uint32Size:])

	return
}

// parses a valid nbs tableIndex from a byte stream. |buff| must end with an NBS index
// and footer, though it may contain an unspecified number of bytes before that data.
// |tableIndex| doesn't keep alive any references to |buff|.
// Does not allocate new memory except for offsets, computes on buff in place.
func parseTableIndex(buff []byte) (onHeapTableIndex, error) {
	chunkCount, totalUncompressedData, err := ReadTableFooter(bytes.NewReader(buff))
	if err != nil {
		return onHeapTableIndex{}, err
	}

	iS := indexSize(chunkCount)
	buff = buff[:len(buff)-footerSize]
	// Trim away any extra bytes
	buff = buff[uint64(len(buff))-iS:]

	return NewOnHeapTableIndex(buff, chunkCount, totalUncompressedData)
}

// ReadTableIndex loads an index into memory from an io.ReadSeeker
// Caution: Allocates new memory for entire index
func ReadTableIndex(rd io.ReadSeeker) (onHeapTableIndex, error) {
	chunkCount, totalUncompressedData, err := ReadTableFooter(rd)
	if err != nil {
		return onHeapTableIndex{}, err
	}
	iS := int64(indexSize(chunkCount))
	_, err = rd.Seek(-(iS + footerSize), io.SeekEnd)
	if err != nil {
		return onHeapTableIndex{}, ErrInvalidTableFile
	}
	buff := make([]byte, iS)
	_, err = io.ReadFull(rd, buff)
	if err != nil {
		return onHeapTableIndex{}, err
	}

	return NewOnHeapTableIndex(buff, chunkCount, totalUncompressedData)
}

type onHeapTableIndex struct {
	tableFileSize uint64
	// Tuple bytes
	tupleB []byte
	// Offset bytes
	offsetB []byte
	// Suffix bytes
	suffixB               []byte
	chunkCount            uint32
	totalUncompressedData uint64
}

var _ tableIndex = &onHeapTableIndex{}

// NewOnHeapTableIndex creates a table index given a buffer of just the table index (no footer)
func NewOnHeapTableIndex(b []byte, chunkCount uint32, totalUncompressedData uint64) (onHeapTableIndex, error) {
	tuples := b[:prefixTupleSize*chunkCount]
	lengths := b[prefixTupleSize*chunkCount : prefixTupleSize*chunkCount+lengthSize*chunkCount]
	suffixes := b[prefixTupleSize*chunkCount+lengthSize*chunkCount:]

	lR := bytes.NewReader(lengths)
	offsets := make([]byte, chunkCount*offsetSize)
	_, err := io.ReadFull(NewOffsetsReader(lR), offsets)
	if err != nil {
		return onHeapTableIndex{}, err
	}

	return onHeapTableIndex{
		tupleB:                tuples,
		offsetB:               offsets,
		suffixB:               suffixes,
		chunkCount:            chunkCount,
		totalUncompressedData: totalUncompressedData,
	}, nil
}

func (ti onHeapTableIndex) ChunkCount() uint32 {
	return ti.chunkCount
}

func (ti onHeapTableIndex) EntrySuffixMatches(idx uint32, h *addr) (bool, error) {
	ord := ti.ordinalAt(idx)
	o := ord * addrSuffixSize
	b := ti.suffixB[o : o+addrSuffixSize]
	return bytes.Equal(h[addrPrefixSize:], b), nil
}

func (ti onHeapTableIndex) IndexEntry(idx uint32, a *addr) (entry indexEntry, err error) {
	prefix, ord := ti.tupleAt(idx)

	if a != nil {
		binary.BigEndian.PutUint64(a[:], prefix)

		o := int64(addrSuffixSize * ord)
		b := ti.suffixB[o : o+addrSuffixSize]
		copy(a[addrPrefixSize:], b)
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
		o: prevOff,
		l: length,
	}
}

func (ti onHeapTableIndex) Lookup(h *addr) (indexEntry, bool, error) {
	ord, err := ti.lookupOrdinal(h)
	if err != nil {
		return indexResult{}, false, err
	}
	if ord == ti.chunkCount {
		return indexResult{}, false, nil
	}
	return ti.getIndexEntry(ord), true, nil
}

// lookupOrdinal returns the ordinal of |h| if present. Returns |ti.chunkCount|
// if absent.
func (ti onHeapTableIndex) lookupOrdinal(h *addr) (uint32, error) {
	prefix := h.Prefix()

	for idx := ti.prefixIdx(prefix); idx < ti.chunkCount && ti.prefixAt(idx) == prefix; idx++ {
		m, err := ti.EntrySuffixMatches(idx, h)
		if err != nil {
			return ti.chunkCount, err
		}
		if m {
			return ti.ordinalAt(idx), nil
		}
	}

	return ti.chunkCount, nil
}

// prefixIdx returns the first position in |tr.prefixes| whose value ==
// |prefix|. Returns |tr.chunkCount| if absent
func (ti onHeapTableIndex) prefixIdx(prefix uint64) (idx uint32) {
	// NOTE: The golang impl of sort.Search is basically inlined here. This method can be called in
	// an extremely tight loop and inlining the code was a significant perf improvement.
	idx, j := 0, ti.chunkCount
	for idx < j {
		h := idx + (j-idx)/2 // avoid overflow when computing h
		// i ≤ h < j
		if ti.prefixAt(h) < prefix {
			idx = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}

	return
}

func (ti onHeapTableIndex) tupleAt(idx uint32) (prefix uint64, ord uint32) {
	off := int64(prefixTupleSize * idx)
	b := ti.tupleB[off : off+prefixTupleSize]

	prefix = binary.BigEndian.Uint64(b[:])
	ord = binary.BigEndian.Uint32(b[addrPrefixSize:])
	return prefix, ord
}

func (ti onHeapTableIndex) prefixAt(idx uint32) uint64 {
	off := int64(prefixTupleSize * idx)
	b := ti.tupleB[off : off+addrPrefixSize]
	return binary.BigEndian.Uint64(b)
}

func (ti onHeapTableIndex) ordinalAt(idx uint32) uint32 {
	off := int64(prefixTupleSize*idx) + addrPrefixSize
	b := ti.tupleB[off : off+ordinalSize]
	return binary.BigEndian.Uint32(b)
}

func (ti onHeapTableIndex) offsetAt(ord uint32) uint64 {
	off := int64(offsetSize * ord)
	b := ti.offsetB[off : off+offsetSize]
	return binary.BigEndian.Uint64(b)
}

func (ti onHeapTableIndex) Ordinals() ([]uint32, error) {
	o := make([]uint32, ti.chunkCount)
	for i, off := uint32(0), 0; i < ti.chunkCount; i, off = i+1, off+prefixTupleSize {
		b := ti.tupleB[off+addrPrefixSize : off+prefixTupleSize]
		o[i] = binary.BigEndian.Uint32(b)
	}
	return o, nil
}

func (ti onHeapTableIndex) Prefixes() ([]uint64, error) {
	p := make([]uint64, ti.chunkCount)
	for i, off := uint32(0), 0; i < ti.chunkCount; i, off = i+1, off+prefixTupleSize {
		b := ti.tupleB[off : off+addrPrefixSize]
		p[i] = binary.BigEndian.Uint64(b)
	}
	return p, nil
}

// TableFileSize returns the size of the table file that this index references.
// This assumes that the index follows immediately after the last chunk in the
// file and that the last chunk in the file is in the index.
func (ti onHeapTableIndex) TableFileSize() uint64 {
	if ti.chunkCount == 0 {
		return footerSize
	}
	entry := ti.getIndexEntry(ti.chunkCount - 1)
	offset, len := entry.Offset(), uint64(entry.Length())
	return offset + len + indexSize(ti.chunkCount) + footerSize
}

func (ti onHeapTableIndex) TotalUncompressedData() uint64 {
	return ti.totalUncompressedData
}

func (ti onHeapTableIndex) Close() error {
	return nil
}

func (ti onHeapTableIndex) Clone() (tableIndex, error) {
	return ti, nil
}

// mmap table index

type mmapIndexEntry []byte

const mmapIndexEntryOffsetStart = addrSuffixSize
const mmapIndexEntryLengthStart = addrSuffixSize + uint64Size

func (e mmapIndexEntry) suffix() []byte {
	return e[:addrSuffixSize]
}

func (e mmapIndexEntry) Offset() uint64 {
	return binary.BigEndian.Uint64(e[mmapIndexEntryOffsetStart:])
}

func (e mmapIndexEntry) Length() uint32 {
	return binary.BigEndian.Uint32(e[mmapIndexEntryLengthStart:])
}

func mmapOffheapSize(chunks int) int {
	pageSize := 4096
	esz := addrSuffixSize + uint64Size + lengthSize
	min := esz * chunks
	if min%pageSize == 0 {
		return min
	} else {
		return (min/pageSize + 1) * pageSize
	}
}

// An mmapIndexEntry is an addrSuffix, a BigEndian uint64 for the offset and a
// BigEnding uint32 for the chunk size.
const mmapIndexEntrySize = addrSuffixSize + uint64Size + lengthSize

type mmapOrdinal struct {
	idx    int
	offset uint64
}
type mmapOrdinalSlice []mmapOrdinal

func (s mmapOrdinalSlice) Len() int           { return len(s) }
func (s mmapOrdinalSlice) Less(i, j int) bool { return s[i].offset < s[j].offset }
func (s mmapOrdinalSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type mmapTableIndex struct {
	chunkCount            uint32
	totalUncompressedData uint64
	fileSz                uint64
	prefixes              []uint64
	data                  mmap.MMap
	refCnt                *int32
}

func newMmapTableIndex(ti onHeapTableIndex, f *os.File) (mmapTableIndex, error) {
	flags := 0
	if f == nil {
		flags = mmap.ANON
	}
	arr, err := mmap.MapRegion(f, mmapOffheapSize(int(ti.chunkCount)), mmap.RDWR, flags, 0)
	if err != nil {
		return mmapTableIndex{}, err
	}
	var a addr
	for i := uint32(0); i < ti.chunkCount; i++ {
		idx := i * mmapIndexEntrySize
		si := addrSuffixSize * ti.ordinalAt(i)
		copy(arr[idx:], ti.suffixB[si:si+addrSuffixSize])

		e, err := ti.IndexEntry(i, &a)
		if err != nil {
			return mmapTableIndex{}, err
		}
		binary.BigEndian.PutUint64(arr[idx+mmapIndexEntryOffsetStart:], e.Offset())
		binary.BigEndian.PutUint32(arr[idx+mmapIndexEntryLengthStart:], e.Length())
	}

	refCnt := new(int32)
	*refCnt = 1
	p, err := ti.Prefixes()
	if err != nil {
		return mmapTableIndex{}, err
	}
	return mmapTableIndex{
		ti.chunkCount,
		ti.totalUncompressedData,
		ti.TableFileSize(),
		p,
		arr,
		refCnt,
	}, nil
}

func (i mmapTableIndex) ChunkCount() uint32 {
	return i.chunkCount
}

func (i mmapTableIndex) EntrySuffixMatches(idx uint32, h *addr) (bool, error) {
	mi := idx * mmapIndexEntrySize
	e := mmapIndexEntry(i.data[mi : mi+mmapIndexEntrySize])
	return bytes.Equal(e.suffix(), h[addrPrefixSize:]), nil
}

func (i mmapTableIndex) IndexEntry(idx uint32, a *addr) (indexEntry, error) {
	mi := idx * mmapIndexEntrySize
	e := mmapIndexEntry(i.data[mi : mi+mmapIndexEntrySize])
	if a != nil {
		binary.BigEndian.PutUint64(a[:], i.prefixes[idx])
		copy(a[addrPrefixSize:], e.suffix())
	}
	return e, nil
}

func (i mmapTableIndex) Lookup(h *addr) (indexEntry, bool, error) {
	prefix := binary.BigEndian.Uint64(h[:])
	for idx := i.prefixIdx(prefix); idx < i.chunkCount && i.prefixes[idx] == prefix; idx++ {
		mi := idx * mmapIndexEntrySize
		e := mmapIndexEntry(i.data[mi : mi+mmapIndexEntrySize])
		if bytes.Equal(e.suffix(), h[addrPrefixSize:]) {
			return e, true, nil
		}
	}
	return mmapIndexEntry{}, false, nil
}

func (i mmapTableIndex) Ordinals() ([]uint32, error) {
	s := mmapOrdinalSlice(make([]mmapOrdinal, i.chunkCount))
	for idx := 0; uint32(idx) < i.chunkCount; idx++ {
		mi := idx * mmapIndexEntrySize
		e := mmapIndexEntry(i.data[mi : mi+mmapIndexEntrySize])
		s[idx] = mmapOrdinal{idx, e.Offset()}
	}
	sort.Sort(s)
	res := make([]uint32, i.chunkCount)
	for j, r := range s {
		res[r.idx] = uint32(j)
	}
	return res, nil
}

func (i mmapTableIndex) Prefixes() ([]uint64, error) {
	return i.prefixes, nil
}

func (i mmapTableIndex) TableFileSize() uint64 {
	return i.fileSz
}

func (i mmapTableIndex) TotalUncompressedData() uint64 {
	return i.totalUncompressedData
}

func (i mmapTableIndex) Close() error {
	cnt := atomic.AddInt32(i.refCnt, -1)
	if cnt == 0 {
		return i.data.Unmap()
	}
	if cnt < 0 {
		panic("Close() called and reduced ref count to < 0.")
	}
	return nil
}

func (i mmapTableIndex) Clone() (tableIndex, error) {
	cnt := atomic.AddInt32(i.refCnt, 1)
	if cnt == 1 {
		panic("Clone() called after last Close(). This index is no longer valid.")
	}
	return i, nil
}

func (i mmapTableIndex) prefixIdx(prefix uint64) (idx uint32) {
	// NOTE: The golang impl of sort.Search is basically inlined here. This method can be called in
	// an extremely tight loop and inlining the code was a significant perf improvement.
	idx, j := 0, i.chunkCount
	for idx < j {
		h := idx + (j-idx)/2 // avoid overflow when computing h
		// i ≤ h < j
		if i.prefixes[h] < prefix {
			idx = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	return
}
