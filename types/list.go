package types

// TODO: I'm not sure we even want this interface in the long term, because noms is strongly-typed, so we should actually have List<T>.
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
