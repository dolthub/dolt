package types

type listLeafSequence struct {
	values []Value
	t      *Type
	vr     ValueReader
}

func newListLeafSequence(vr ValueReader, v ...Value) indexedSequence {
	ts := make([]*Type, len(v))
	for i, v := range v {
		ts[i] = v.Type()
	}
	t := MakeListType(MakeUnionType(ts...))
	return listLeafSequence{v, t, vr}
}

// indexedSequence interface
func (ll listLeafSequence) getOffset(idx int) uint64 {
	return uint64(idx)
}

// sequence interface
func (ll listLeafSequence) getItem(idx int) sequenceItem {
	return ll.values[idx]
}

func (ll listLeafSequence) seqLen() int {
	return len(ll.values)
}

func (ll listLeafSequence) numLeaves() uint64 {
	return uint64(len(ll.values))
}

func (ll listLeafSequence) valueReader() ValueReader {
	return ll.vr
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
