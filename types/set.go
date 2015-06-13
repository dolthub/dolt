package types

type setIterCallback func(v Value) bool

type Set interface {
	Value
	Len() uint64
	Has(v Value) bool
	Iter(setIterCallback)
	Insert(v ...Value) Set
	Remove(v ...Value) Set
}

func NewSet(v ...Value) Set {
	return newFlatSet(buildInternalMap(setInternalMap{}, v))
}
