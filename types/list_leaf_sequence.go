package types

import "github.com/attic-labs/noms/d"

type listLeafSequence struct {
	values []Value
	t      *Type
	vr     ValueReader
}

func newListLeafSequence(t *Type, vr ValueReader, v ...Value) indexedSequence {
	d.Chk.Equal(ListKind, t.Kind())
	return listLeafSequence{v, t, vr}
}

func (ll listLeafSequence) getItem(idx int) sequenceItem {
	return ll.values[idx]
}

func (ll listLeafSequence) seqLen() int {
	return len(ll.values)
}

func (ll listLeafSequence) numLeaves() uint64 {
	return uint64(len(ll.values))
}

func (ll listLeafSequence) getOffset(idx int) uint64 {
	return uint64(idx)
}

func (ll listLeafSequence) Chunks() (chunks []Ref) {
	for _, v := range ll.values {
		chunks = append(chunks, v.Chunks()...)
	}
	return
}

func (ll listLeafSequence) Type() *Type {
	return ll.t
}

func (ll listLeafSequence) valueReader() ValueReader {
	return ll.vr
}
