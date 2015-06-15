package types

import "github.com/attic-labs/noms/ref"

type MapEntry struct {
	Key   Value
	Value Value
}

type mapIterCallback func(entry MapEntry) bool

type Map interface {
	Value
	Len() uint64
	Has(k Value) bool
	Get(k Value) Value
	Set(k Value, v Value) Map
	SetM(kv ...Value) Map
	Remove(k Value) Map
	Iter(mapIterCallback)
}

func NewMap(kv ...Value) Map {
	return newFlatMap(buildMapData(mapData{}, kv...))
}

type MapEntrySlice []MapEntry

func (mes MapEntrySlice) Len() int {
	return len(mes)
}

func (mes MapEntrySlice) Swap(i, j int) {
	mes[i], mes[j] = mes[j], mes[i]
}

func (mes MapEntrySlice) Less(i, j int) bool {
	return ref.Less(mes[i].Key.Ref(), mes[j].Key.Ref())
}
