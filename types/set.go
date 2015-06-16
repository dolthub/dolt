package types

type setIterCallback func(v Value) bool
type setCombineCallback func(prev Set, v ...Value) Set

type Set interface {
	Value
	Len() uint64
	Has(v Value) bool
	Iter(setIterCallback)
	Insert(v ...Value) Set
	Remove(v ...Value) Set
	Union(others ...Set) Set
	Subtract(others ...Set) Set
}

func NewSet(v ...Value) Set {
	return newFlatSet(buildSetData(setData{}, v))
}
