package types

import (
	"crypto/sha1"
	"runtime"
	"sort"
	"sync"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/kch42/buzhash"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	listWindowSize = 8 * sha1.Size // The digest is 20 bytes so this makes the window 8 elements.
	listPattern    = uint32(1<<6 - 1)
)

// compoundList represents a list of lists
// compoundList implements the List interface
// compoundList implements the Value interface
type compoundList struct {
	compoundObject
}

// listChunker is used to create a compoundList or a listLeaf.
// For every element in a list we call either writeFuture or writeValue and if
// after the element has been added to the chunker the hash matches the listPattern
// we split the list at that point.
type listChunker struct {
	h           *buzhash.BuzHash
	futures     []Future
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
	si := findSubIndex(startIdx, l.offsets)
	return newListChunkerFromListWithSubindex(l, startIdx, si)
}

func newListChunkerFromListWithSubindex(l compoundList, startIdx uint64, si int) *listChunker {
	lc := newListChunker(l.cs)
	lc.futures = make([]Future, si)
	copy(lc.futures, l.futures)
	lc.offsets = make([]uint64, si)
	copy(lc.offsets, l.offsets)
	offset := uint64(0)
	if si > 0 {
		offset += l.offsets[si-1]
	}
	lastList := l.futures[si].Deref(l.cs).(List)
	it := newListIterator(lastList)
	for i := uint64(0); i < startIdx-offset; i++ {
		f, done := it.next()
		d.Chk.False(done)
		lc.writeFuture(f)
	}
	return lc
}

// writeValue writes a Value to the chunker. Returns whether the write caused
// a split after the written Value.
func (lc *listChunker) writeValue(v Value) (split bool) {
	return lc.writeFuture(futureFromValue(v))
}

// writeFuture writes a Future to the chunker. Returns whether the write caused
// a split after the written Future.
func (lc *listChunker) writeFuture(f Future) (split bool) {
	digest := f.Ref().Digest()
	_, err := lc.h.Write(digest[:])
	d.Chk.NoError(err)
	lc.currentList = append(lc.currentList, f)
	if lc.h.Sum32()&listPattern == listPattern {
		lc.addChunk()
		return true
	}
	return false
}

func (lc *listChunker) addChunk() {
	list := listLeafFromFutures(lc.currentList, lc.cs)
	lc.futures = append(lc.futures, futureFromValue(list))
	offset := uint64(len(lc.currentList))
	if len(lc.offsets) > 0 {
		offset += lc.offsets[len(lc.offsets)-1]
	}
	lc.offsets = append(lc.offsets, offset)
	lc.currentList = []Future{}
	lc.h = buzhash.NewBuzHash(listWindowSize)
}

// writeTail adds elements from cl to the chunker starting at idx.
// added is used to figure out what index in the original list idx matches.
func (lc *listChunker) writeTail(cl compoundList, idx, added uint64) {
	// [aaaaaaaaa|bbbb|cccc|dddd|eeeeeeeee]
	// b -> B
	// [aaaaaaaaa|bbBb|cccc |dddd|eeeeeeeee]
	// [aaaaaaaaa|bbBbcccc  |dddd|eeeeeeeee]
	// [aaaaaaaaa|bbB|b|cccc|dddd|eeeeeeeee]
	if idx >= cl.Len() {
		return
	}
	it := newListIteratorAt(cl, idx)
	for i := idx; i < cl.Len(); i++ {
		f, done := it.next()
		d.Chk.False(done)
		if lc.writeFuture(f) {
			// if cl has a split at this index then the rest can be copied.
			if sc, si := cl.startsChunk(i - added + 1); sc {
				lc.futures = append(lc.futures, cl.futures[si:]...)
				lc.offsets = append(lc.offsets, cl.offsets[si:]...)
				break
			}
		}
	}
}

func (lc *listChunker) makeList() List {
	if len(lc.futures) == 0 {
		return listLeafFromFutures(lc.currentList, lc.cs)
	}
	if len(lc.currentList) > 0 {
		lc.addChunk()
	}
	// In case we get a single child list just return that instead.
	if len(lc.futures) == 1 {
		return lc.futures[0].Deref(lc.cs).(List)
	}
	return newCompoundList(lc.offsets, lc.futures, lc.cs)
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

func (cl compoundList) Iter(f listIterFunc) {
	it := newListIterator(cl)
	for {
		fut, done := it.next()
		if done {
			break
		}
		if f(fut.Deref(cl.cs)) {
			fut.Release()
			break
		}
		fut.Release()
	}
}

func (cl compoundList) IterAll(f listIterAllFunc) {
	it := newListIterator(cl)
	for {
		fut, done := it.next()
		if done {
			break
		}
		f(fut.Deref(cl.cs))
		fut.Release()
	}
}

func (cl compoundList) Map(mf MapFunc) []interface{} {
	return cl.MapP(1, mf)
}

func (cl compoundList) MapP(concurrency int, mf MapFunc) []interface{} {
	var limit chan int
	if concurrency == 0 {
		limit = make(chan int, runtime.NumCPU())
	} else {
		limit = make(chan int, concurrency)
	}

	return cl.mapInternal(limit, mf)
}

func (cl compoundList) mapInternal(sem chan int, mf MapFunc) []interface{} {
	values := make([]interface{}, cl.Len(), cl.Len())

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	// TODO: We're spinning up one goroutine for each meta chunk in the list on top of one goroutine per concurrent |mf|. There's probably a more correct way to do this.
	for si := uint64(0); si < uint64(len(cl.futures)); si++ {
		wg.Add(1)

		go func(si uint64) {
			defer wg.Done()

			f := cl.futures[si]
			l := f.Deref(cl.cs).(List)
			f.Release()

			sv := l.mapInternal(sem, mf)
			mu.Lock()
			defer mu.Unlock()

			idx := 0
			if si > 0 {
				idx += int(cl.offsets[si-1])
			}

			copy(values[idx:], sv)
		}(si)
	}

	wg.Wait()
	return values
}

func (cl compoundList) getFuture(idx uint64) Future {
	si := findSubIndex(idx, cl.offsets)
	f := cl.futures[si]
	l := f.Deref(cl.cs).(List)
	if si > 0 {
		idx -= cl.offsets[si-1]
	}
	return l.getFuture(idx)
}

func (cl compoundList) Slice(start uint64, end uint64) List {
	// TODO: Optimize. We should be able to just reuse the chunks between start and end.
	lc := newListChunker(cl.cs)
	it := newListIteratorAt(cl, start)
	for i := start; i < end; i++ {
		f, done := it.next()
		d.Chk.False(done)
		lc.writeFuture(f)
	}
	return lc.makeList()
}

func (cl compoundList) Set(idx uint64, v Value) List {
	lc := newListChunkerFromList(cl, idx)
	lc.writeValue(v)
	lc.writeTail(cl, idx+1, 0)
	return lc.makeList()
}

func (cl compoundList) Append(vs ...Value) List {
	startIdx := cl.Len()
	si := len(cl.offsets) - 1
	lc := newListChunkerFromListWithSubindex(cl, startIdx, si)
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
	lc := newListChunkerFromList(cl, idx)
	for _, v := range vs {
		lc.writeValue(v)
	}
	lc.writeTail(cl, idx, uint64(len(vs)))
	return lc.makeList()
}

func (cl compoundList) Remove(start uint64, end uint64) List {
	if start > cl.Len() || end > cl.Len() {
		panic("Remove bounds out of range")
	}
	lc := newListChunkerFromList(cl, start)
	lc.writeTail(cl, end, end-start)
	return lc.makeList()
}

func (cl compoundList) RemoveAt(idx uint64) List {
	return cl.Remove(idx, idx+1)
}

func (cl compoundList) Ref() ref.Ref {
	return ensureRef(cl.ref, cl)
}

func (cl compoundList) Release() {
	for _, f := range cl.futures {
		f.Release()
	}
}

func (cl compoundList) Equals(other Value) bool {
	if other == nil {
		return false
	}
	return cl.Ref() == other.Ref()
}

// startsChunk determines if idx refers to the first element in one of cl's chunks.
// If so, it also returns the index of the chunk into which idx points.
func (cl compoundList) startsChunk(idx uint64) (bool, uint64) {
	si := findSubIndex(idx, cl.offsets)
	offset := uint64(0)
	if si > 0 {
		offset += cl.offsets[si-1]
	}
	return offset == idx, uint64(si)
}

func newCompoundList(offsets []uint64, futures []Future, cs chunks.ChunkSource) compoundList {
	return compoundList{compoundObject{offsets, futures, &ref.Ref{}, cs}}
}

func newCompoundListFromValues(vs []Value, cs chunks.ChunkSource) List {
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
