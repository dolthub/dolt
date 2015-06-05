package types

type IterCallback func(k string, v Value) bool

type Map interface {
	Value
	// TODO: keys should be able to be any noms type
	Len() uint64
	Has(k string) bool
	Get(k string) Value
	Set(k string, v Value) Map
	SetM(kv ...interface{}) Map
	Remove(k string) Map
	Iter(IterCallback)
}

func NewMap(kv ...interface{}) Map {
	return flatMap{buildMap(nil, kv...)}
}
