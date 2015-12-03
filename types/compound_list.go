package types

import (
	"crypto/sha1"

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
	ref *ref.Ref
	cs  chunks.ChunkStore
}

func buildCompoundList(tuples metaSequenceData, t Type, cs chunks.ChunkStore) Value {
	cl := compoundList{metaSequenceObject{tuples, t}, &ref.Ref{}, cs}
	return valueFromType(cl.cs, cl, t)
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
	return cl.tuples[len(cl.tuples)-1].uint64Value()
}

func (cl compoundList) Empty() bool {
	d.Chk.True(cl.Len() > 0) // A compound object should never be empty.
	return false
}

func (cl compoundList) cursorAt(idx uint64) (cursor *metaSequenceCursor, l listLeaf, start uint64) {
	d.Chk.True(idx <= cl.Len())
	cursor, leaf := newMetaSequenceCursor(cl, cl.cs)

	chunkStart := cursor.seek(func(v, parent Value) bool {
		d.Chk.NotNil(v)
		d.Chk.NotNil(parent)

		return idx < uint64(parent.(Uint64))+uint64(v.(Uint64))
	}, func(parent, prev, current Value) Value {
		pv := uint64(0)
		if prev != nil {
			pv = uint64(prev.(Uint64))
		}

		return Uint64(uint64(parent.(Uint64)) + pv)
	}, Uint64(0))

	if cursor.currentRef() != leaf.Ref() {
		leaf = cursor.currentVal()
	}

	l = leaf.(listLeaf)
	start = uint64(chunkStart.(Uint64))
	return
}

func (cl compoundList) Get(idx uint64) Value {
	_, l, start := cl.cursorAt(idx)
	return l.Get(idx - start)
}

func (cl compoundList) IterAllP(concurrency int, f listIterAllFunc) {
	panic("not implemented")
}

func (cl compoundList) Slice(start uint64, end uint64) List {
	panic("not implemented")
}

func (cl compoundList) Map(mf MapFunc) []interface{} {
	panic("not implemented")
}

func (cl compoundList) MapP(concurrency int, mf MapFunc) []interface{} {
	panic("not implemented")
}

func (cl compoundList) Set(idx uint64, v Value) List {
	panic("not implemented")
}

func (cl compoundList) Append(vs ...Value) List {
	metaCur, leaf, start := cl.cursorAt(cl.Len())
	seqCur := newSequenceChunkerCursor(metaCur, listAsSequenceItems(leaf), int(cl.Len()-start), readListLeafChunkFn(cl.cs), cl.cs)
	seq := newSequenceChunker(seqCur, makeListLeafChunkFn(cl.t, cl.cs), newMetaSequenceChunkFn(cl.t, cl.cs), normalizeChunkNoop, normalizeMetaSequenceChunk, newListLeafBoundaryChecker(), newMetaSequenceBoundaryChecker)
	for _, v := range vs {
		seq.Append(v)
	}
	return seq.Done().(List)
}

func (cl compoundList) Filter(cb listFilterCallback) List {
	panic("not implemented")
}

func (cl compoundList) Insert(idx uint64, v ...Value) List {
	panic("not implemented")
}

func (cl compoundList) Remove(start uint64, end uint64) List {
	panic("not implemented")
}

func (cl compoundList) RemoveAt(idx uint64) List {
	panic("not implemented")
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

		list := valueFromType(cs, listLeaf{values, t, &ref.Ref{}, cs}, t)
		ref := WriteValue(list, cs)
		return metaTuple{ref, Uint64(len(values))}, list
	}
}

func readListLeafChunkFn(cs chunks.ChunkStore) readChunkFn {
	return func(item sequenceItem) []sequenceItem {
		mt := item.(metaTuple)
		return listAsSequenceItems(ReadValue(mt.ref, cs).(listLeaf))
	}
}
