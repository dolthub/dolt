package types

import (
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

var (
	emptyString   = string("")
	emptyValuePtr = (*Value)(nil)
)

type internalMap map[string]Value

type flatMap struct {
	m  internalMap
	cr *cachedRef
}

func newFlatMap(m internalMap) flatMap {
	return flatMap{m, &cachedRef{}}
}

func (fm flatMap) Len() uint64 {
	return uint64(len(fm.m))
}

func (fm flatMap) Has(key string) bool {
	_, ok := fm.m[key]
	return ok
}

func (fm flatMap) Get(key string) Value {
	if v, ok := fm.m[key]; ok {
		return v
	} else {
		return nil
	}
}

func (fm flatMap) Set(key string, val Value) Map {
	return newFlatMap(buildMap(fm.m, key, val))
}

func (fm flatMap) SetM(kv ...interface{}) Map {
	return newFlatMap(buildMap(fm.m, kv...))
}

func (fm flatMap) Remove(k string) Map {
	m := buildMap(fm.m)
	delete(m, k)
	return newFlatMap(m)
}

func (fm flatMap) Iter(cb mapIterCallback) {
	for k, v := range fm.m {
		if cb(k, v) {
			break
		}
	}
}

func (fm flatMap) Ref() ref.Ref {
	return fm.cr.Ref(fm)
}

func (fm flatMap) Equals(other Value) (res bool) {
	if other, ok := other.(Map); ok {
		res = true
		fm.Iter(func(k string, v Value) (stop bool) {
			if !v.Equals(other.Get(k)) {
				stop = true
				res = false
			}
			return
		})
		if !res {
			return
		}
		other.Iter(func(k string, v Value) (stop bool) {
			if !fm.Has(k) {
				stop = true
				res = false
			}
			return
		})
	}
	return
}

func buildMap(initialData internalMap, kv ...interface{}) (m internalMap) {
	Chk.Equal(0, len(kv)%2, "Must specify even number of key/value pairs")
	m = internalMap{}
	if initialData != nil {
		for k, v := range initialData {
			m[k] = v
		}
	}
	for i := 0; i < len(kv); i += 2 {
		k := kv[i]
		v := kv[i+1]
		Chk.IsType(emptyString, k)
		Chk.Implements(emptyValuePtr, v)
		m[k.(string)] = v.(Value)
	}
	return
}
