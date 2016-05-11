package types

type setLeafSequence struct {
	data []Value // sorted by Ref()
	t    *Type
	vr   ValueReader
}

func newSetLeafSequence(t *Type, vr ValueReader, m ...Value) orderedSequence {
	return setLeafSequence{m, t, vr}
}

func (sl setLeafSequence) getItem(idx int) sequenceItem {
	return sl.data[idx]
}

func (sl setLeafSequence) seqLen() int {
	return len(sl.data)
}

func (sl setLeafSequence) numLeaves() uint64 {
	return uint64(len(sl.data))
}

func (sl setLeafSequence) getKey(idx int) Value {
	return sl.data[idx]
}

func (sl setLeafSequence) Chunks() (chunks []Ref) {
	for _, v := range sl.data {
		chunks = append(chunks, v.Chunks()...)
	}
	return
}

func (sl setLeafSequence) Type() *Type {
	return sl.t
}

func (sl setLeafSequence) valueReader() ValueReader {
	return sl.vr
}
