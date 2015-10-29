package types

type listIterFunc func(v Value, index uint64) (stop bool)
type listIterAllFunc func(v Value, index uint64)

type MapFunc func(v Value, index uint64) interface{}

type List interface {
	Value
	Len() uint64
	Empty() bool
	Get(idx uint64) Value
	Slice(start uint64, end uint64) List
	Set(idx uint64, v Value) List
	Append(v ...Value) List
	Insert(idx uint64, v ...Value) List
	Remove(start uint64, end uint64) List
	RemoveAt(idx uint64) List
	Release()
	Iter(f listIterFunc)
	IterAll(f listIterAllFunc)
	IterAllP(concurrency int, f listIterAllFunc)
	iterInternal(sem chan int, f listIterAllFunc, offset uint64)
	Map(mf MapFunc) []interface{}
	MapP(concurrency int, mf MapFunc) []interface{}
	mapInternal(sem chan int, mf MapFunc, offset uint64) []interface{}
}

func NewList(v ...Value) List {
	return newListLeaf(v...)
}

func valuesToFutures(list []Value) []Future {
	f := []Future{}
	for _, v := range list {
		f = append(f, futureFromValue(v))
	}
	return f
}

func ListFromVal(v Value) List {
	return v.(List)
}

var listTypeRef = MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(ValueKind))

func init() {
	RegisterFromValFunction(listTypeRef, func(v Value) Value {
		return v.(List)
	})
}
