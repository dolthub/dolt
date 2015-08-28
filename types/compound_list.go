package types

import (
	"crypto/sha1"
	"sort"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/kch42/buzhash"
)

const (
	listWindowSize = 8 * sha1.Size // The digest is 20 bytes so this makes the window 8 elements.
	listPattern    = uint32(1<<6 - 1)
)

// compoundList represents a list of lists
// compoundList implements the List interface
// compoundList implements the Value interface
type compoundList struct {
	offsets []uint64 // The offsets are the end offsets between child lists
	lists   []Future
	ref     *ref.Ref
	cs      chunks.ChunkSource
}

// listChunker is used to create a compoundList or a listLeaf.
// For every element in a list we call either writeFuture or writeValue and if
// after the element has been added to the chunker the hash matches the listPattern
// we split the list at that point.
type listChunker struct {
	h           *buzhash.BuzHash
	lists       []Future
	offsets     []uint64
	currentList []Future // Accumulated Futures as the list is built.
	cs          chunks.ChunkSource
}

func newListChunker(cs chunks.ChunkSource) *listChunker {
	return &listChunker{
		h:  buzhash.NewBuzHash(listWindowSize),
		cs: cs,
	}
}

// newListChunkerFromList creates a new listChunker copying the elements from l up to startIdx
func newListChunkerFromList(l compoundList, startIdx uint64) *listChunker {
	lc := newListChunker(l.cs)
	si := findSubIndex(startIdx, l.offsets)
	lc.lists = make([]Future, si)
	copy(lc.lists, l.lists)
	lc.offsets = make([]uint64, si)
	copy(lc.offsets, l.offsets)
	offset := uint64(0)
	if si > 0 {
		offset += l.offsets[si-1]
	}
	lastList := l.lists[si].Deref(l.cs).(List)
	for i := offset; i < startIdx; i++ {
		lc.writeFuture(lastList.getFuture(i - offset))
	}
	return lc
}

func (lc *listChunker) writeValue(v Value) {
	lc.writeFuture(futureFromValue(v))
}

func (lc *listChunker) writeFuture(f Future) {
	digest := f.Ref().Digest()
	_, err := lc.h.Write(digest[:])
	d.Chk.NoError(err)
	lc.currentList = append(lc.currentList, f)
	if lc.h.Sum32()&listPattern == listPattern {
		lc.addChunk()
	}
}

func (lc *listChunker) addChunk() {
	list := listLeafFromFutures(lc.currentList, lc.cs)
	lc.lists = append(lc.lists, futureFromValue(list))
	offset := uint64(len(lc.currentList))
	if len(lc.offsets) > 0 {
		offset += lc.offsets[len(lc.offsets)-1]
	}
	lc.offsets = append(lc.offsets, offset)
	lc.currentList = []Future{}
	lc.h = buzhash.NewBuzHash(listWindowSize)
}

func (lc *listChunker) makeList() List {
	if len(lc.lists) == 0 {
		return listLeafFromFutures(lc.currentList, lc.cs)
	}
	if len(lc.currentList) > 0 {
		lc.addChunk()
	}
	// In case we get a single child list just return that instead.
	if len(lc.lists) == 1 {
		return lc.lists[0].Deref(lc.cs).(List)
	}
	return compoundList{lc.offsets, lc.lists, &ref.Ref{}, lc.cs}
}

func (cl compoundList) Len() uint64 {
	return cl.offsets[len(cl.offsets)-1]
}

func (cl compoundList) Empty() bool {
	return cl.Len() == uint64(0)
}

func findSubIndex(idx uint64, offsets []uint64) int {
	return sort.Search(len(offsets), func(i int) bool {
		return offsets[i] > idx
	})
}

func (cl compoundList) Get(idx uint64) Value {
	return cl.getFuture(idx).Deref(cl.cs)
}

func (cl compoundList) getFuture(idx uint64) Future {
	si := findSubIndex(idx, cl.offsets)
	f := cl.lists[si]
	l := f.Deref(cl.cs).(List)
	if si > 0 {
		idx -= cl.offsets[si-1]
	}
	return l.getFuture(idx)
}

func (cl compoundList) Slice(start uint64, end uint64) List {
	// TODO: Optimize. We should be able to just reuse the chunks between start and end.
	lc := newListChunker(cl.cs)
	for i := start; i < end; i++ {
		lc.writeFuture(cl.getFuture(i))
	}
	return lc.makeList()
}

func (cl compoundList) Set(idx uint64, v Value) List {
	// TODO: Optimize. We reuse everything up to idx but after that we should only need to rechunk 2 (?) chunks.
	lc := newListChunkerFromList(cl, idx)
	lc.writeValue(v)
	for i := idx + 1; i < cl.Len(); i++ {
		lc.writeFuture(cl.getFuture(i))
	}
	return lc.makeList()
}

func (cl compoundList) Append(vs ...Value) List {
	// Redo chunking from last chunk.
	d.Chk.False(cl.Empty())
	d.Chk.True(len(cl.lists) > 1)

	l := len(cl.offsets)
	offsets := make([]uint64, l-1, l)
	copy(offsets, cl.offsets)
	l = len(cl.lists)
	lists := make([]Future, l-1, l)
	copy(lists, cl.lists)
	lastList := cl.lists[l-1].Deref(cl.cs).(List)

	lc := newListChunker(cl.cs)
	lc.lists = lists
	lc.offsets = offsets

	// Append elements from last list again.
	for i := uint64(0); i < lastList.Len(); i++ {
		lc.writeFuture(lastList.getFuture(i))
	}
	for _, v := range vs {
		lc.writeValue(v)
	}
	return lc.makeList()
}

func (cl compoundList) Insert(idx uint64, vs ...Value) List {
	if idx > cl.Len() {
		panic("Insert out of bounds")
	}
	if idx == cl.Len() {
		return cl.Append(vs...)
	}
	// TODO: Optimize. We currently reuse the head but we should be able to reuse the tail too.
	lc := newListChunkerFromList(cl, idx)
	for _, v := range vs {
		lc.writeValue(v)
	}
	for i := idx; i < cl.Len(); i++ {
		lc.writeFuture(cl.getFuture(i))
	}
	return lc.makeList()
}

func (cl compoundList) Remove(start uint64, end uint64) List {
	if start > cl.Len() || end > cl.Len() {
		panic("Remove bounds out of range")
	}
	// TODO: Optimize. We currently reuse the head but we should be able to reuse the tail too.
	lc := newListChunkerFromList(cl, start)
	for i := end; i < cl.Len(); i++ {
		lc.writeFuture(cl.getFuture(i))
	}
	return lc.makeList()
}

func (cl compoundList) RemoveAt(idx uint64) List {
	return cl.Remove(idx, idx+1)
}

func (cl compoundList) Ref() ref.Ref {
	return ensureRef(cl.ref, cl)
}

func (cl compoundList) Release() {
	for _, f := range cl.lists {
		f.Release()
	}
}

func (cl compoundList) Equals(other Value) bool {
	if other == nil {
		return false
	}
	return cl.Ref() == other.Ref()
}

func (cl compoundList) Chunks() (futures []Future) {
	for _, f := range cl.lists {
		if f, ok := f.(*unresolvedFuture); ok {
			futures = append(futures, f)
		}
	}
	return
}

func newCompoundList(vs []Value, cs chunks.ChunkSource) List {
	l := uint64(len(vs))
	// Always use a list leaf for empty and single element lists.
	if l < 2 {
		return newListLeaf(vs...)
	}

	lc := newListChunker(cs)
	for _, v := range vs {
		lc.writeValue(v)
	}
	return lc.makeList()
}
