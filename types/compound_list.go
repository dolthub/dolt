package types

import (
	"crypto/sha1"

	"runtime"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	// The window size to use for computing the rolling hash.
	listWindowSize = 64
	listPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type compoundList struct {
	metaSequenceObject
	length uint64
	ref    *ref.Ref
	cs     chunks.ChunkStore
}

func buildCompoundList(tuples metaSequenceData, t Type, cs chunks.ChunkStore) Value {
	cl := compoundList{metaSequenceObject{tuples, t}, tuples.uint64ValuesSum(), &ref.Ref{}, cs}
	return valueFromType(cs, cl, t)
}

func listAsSequenceItems(ls listLeaf) []sequenceItem {
	items := make([]sequenceItem, len(ls.values))
	for i, v := range ls.values {
		items[i] = v
	}
	return items
}

func init() {
	registerMetaValue(ListKind, buildCompoundList)
}

func (cl compoundList) Equals(other Value) bool {
	return other != nil && cl.t.Equals(other.Type()) && cl.Ref() == other.Ref()
}

func (cl compoundList) Ref() ref.Ref {
	return EnsureRef(cl.ref, cl)
}

func (cl compoundList) Len() uint64 {
	return cl.length
}

func (cl compoundList) Empty() bool {
	d.Chk.True(cl.Len() > 0) // A compound object should never be empty.
	return false
}

// Returns a cursor pointing to the deepest metaTuple containing |idx| within |cl|, the list leaf that it points to, and the offset within the list that the leaf starts at.
func (cl compoundList) cursorAt(idx uint64) (*sequenceCursor, listLeaf, uint64) {
	d.Chk.True(idx <= cl.Len())
	cursor, leaf := newMetaSequenceCursor(cl, cl.cs)

	chunkStart := cursor.seekLinear(func(carry interface{}, mt sequenceItem) (bool, interface{}) {
		offset := carry.(uint64) + mt.(metaTuple).uint64Value()
		return idx < offset, offset
	}, uint64(0))

	if current := cursor.current().(metaTuple); current.ref != leaf.Ref() {
		leaf = readMetaTupleValue(cursor.current(), cl.cs)
	}

	return cursor, leaf.(listLeaf), chunkStart.(uint64)
}

func (cl compoundList) Get(idx uint64) Value {
	_, l, start := cl.cursorAt(idx)
	return l.Get(idx - start)
}

func (cl compoundList) IterAllP(concurrency int, f listIterAllFunc) {
	start := uint64(0)
	var limit chan int
	if concurrency == 0 {
		limit = make(chan int, runtime.NumCPU())
	} else {
		limit = make(chan int, concurrency)
	}
	iterateMetaSequenceLeaf(cl, cl.cs, func(l Value) bool {
		list := l.(listLeaf)
		list.iterInternal(limit, f, start)
		start += list.Len()
		return false
	})
}

func (cl compoundList) Slice(start uint64, end uint64) List {
	// See https://github.com/attic-labs/noms/issues/744 for a better Slice implementation.
	seq := cl.sequenceCursorAtIndex(start)
	slice := make([]Value, 0, end-start)
	for i := start; i < end; i++ {
		if value, ok := seq.maybeCurrent(); ok {
			slice = append(slice, value.(Value))
		} else {
			break
		}
		seq.advance()
	}
	return NewTypedList(cl.cs, cl.t, slice...)
}

func (cl compoundList) Map(mf MapFunc) []interface{} {
	start := uint64(0)

	results := make([]interface{}, 0, cl.Len())
	iterateMetaSequenceLeaf(cl, cl.cs, func(l Value) bool {
		list := l.(listLeaf)
		for i, v := range list.values {
			res := mf(v, start+uint64(i))
			results = append(results, res)
		}
		start += list.Len()
		return false
	})
	return results
}

func (cl compoundList) MapP(concurrency int, mf MapFunc) []interface{} {
	start := uint64(0)

	var limit chan int
	if concurrency == 0 {
		limit = make(chan int, runtime.NumCPU())
	} else {
		limit = make(chan int, concurrency)
	}

	results := make([]interface{}, 0, cl.Len())
	iterateMetaSequenceLeaf(cl, cl.cs, func(l Value) bool {
		list := l.(listLeaf)
		nl := list.mapInternal(limit, mf, start)
		results = append(results, nl...)
		start += list.Len()
		return false
	})
	return results
}

func (cl compoundList) Set(idx uint64, v Value) List {
	seq := cl.sequenceChunkerAtIndex(idx)
	seq.Skip()
	seq.Append(v)
	return seq.Done().(List)
}

func (cl compoundList) Append(vs ...Value) List {
	return cl.Insert(cl.Len(), vs...)
}

func (cl compoundList) Insert(idx uint64, vs ...Value) List {
	if len(vs) == 0 {
		return cl
	}
	seq := cl.sequenceChunkerAtIndex(idx)
	for _, v := range vs {
		seq.Append(v)
	}
	return seq.Done().(List)
}

func (cl compoundList) sequenceCursorAtIndex(idx uint64) *sequenceCursor {
	// TODO: An optimisation would be to decide at each level whether to step forward or backward across the node to find the insertion point, depending on which is closer. This would make Append much faster.
	metaCur, leaf, start := cl.cursorAt(idx)
	return &sequenceCursor{metaCur, leaf, int(idx - start), len(leaf.values), func(list sequenceItem, idx int) sequenceItem {
		return list.(listLeaf).values[idx]
	}, func(mt sequenceItem) (sequenceItem, int) {
		list := readMetaTupleValue(mt, cl.cs).(listLeaf)
		return list, len(list.values)
	}}
}

func (cl compoundList) sequenceChunkerAtIndex(idx uint64) *sequenceChunker {
	cur := cl.sequenceCursorAtIndex(idx)
	return newSequenceChunker(cur, makeListLeafChunkFn(cl.t, cl.cs), newMetaSequenceChunkFn(cl.t, cl.cs), newListLeafBoundaryChecker(), newMetaSequenceBoundaryChecker)
}

func (cl compoundList) Filter(cb listFilterCallback) List {
	panic("not implemented")
}

func (cl compoundList) Remove(start uint64, end uint64) List {
	if start == end {
		return cl
	}
	d.Chk.True(end > start)
	seq := cl.sequenceChunkerAtIndex(start)
	for i := start; i < end; i++ {
		seq.Skip()
	}
	return seq.Done().(compoundList)
}

func (cl compoundList) RemoveAt(idx uint64) List {
	return cl.Remove(idx, idx+1)
}

func (cl compoundList) Iter(f listIterFunc) {
	start := uint64(0)

	iterateMetaSequenceLeaf(cl, cl.cs, func(l Value) bool {
		list := l.(listLeaf)
		for i, v := range list.values {
			if f(v, start+uint64(i)) {
				return true
			}
		}
		start += list.Len()
		return false
	})
}

func (cl compoundList) IterAll(f listIterAllFunc) {
	start := uint64(0)

	iterateMetaSequenceLeaf(cl, cl.cs, func(l Value) bool {
		list := l.(listLeaf)
		for i, v := range list.values {
			f(v, start+uint64(i))
		}
		start += list.Len()
		return false
	})
}

func newListLeafBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(listWindowSize, sha1.Size, listPattern, func(item sequenceItem) []byte {
		digest := item.(Value).Ref().Digest()
		return digest[:]
	})
}

func makeListLeafChunkFn(t Type, cs chunks.ChunkStore) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		values := make([]Value, len(items))

		for i, v := range items {
			values[i] = v.(Value)
		}

		list := valueFromType(cs, newListLeaf(cs, t, values...), t)
		ref := WriteValue(list, cs)
		return metaTuple{ref, Uint64(len(values))}, list
	}
}
