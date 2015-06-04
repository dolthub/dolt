package types

type List interface {
	Value
	Len() uint64
	Get(idx uint64) Value
	// TODO: iterator
	Slice(idx uint64, end uint64) List
	Set(idx uint64, v Value) List
	Append(v ...Value) List
	Insert(idx uint64, v ...Value) List
	Remove(start uint64, end uint64) List
	RemoveAt(idx uint64) List
}

func NewList(v ...Value) List {
	return flatList{v}
}
