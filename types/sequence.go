package types

type sequenceItem interface{}

type sequence interface {
	getItem(idx int) sequenceItem
	seqLen() int
	numLeaves() uint64
	valueReader() ValueReader
	Chunks() []Ref
	Type() *Type
}
