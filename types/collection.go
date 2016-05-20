package types

type Collection interface {
	Value
	Len() uint64
	Empty() bool
	sequence() sequence
}
