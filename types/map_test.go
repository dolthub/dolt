package types

import (
	"bytes"
	"runtime"
	"sync"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func TestNewMap(t *testing.T) {
	assert := assert.New(t)
	m := NewMap()
	assert.IsType(NewMap(), m)
	assert.Equal(uint64(0), m.Len())
	m = NewMap(NewString("foo"), NewString("foo"), NewString("bar"), NewString("bar"))
	assert.Equal(uint64(2), m.Len())
	assert.True(NewString("foo").Equals(m.Get(NewString("foo"))))
	assert.True(NewString("bar").Equals(m.Get(NewString("bar"))))
}

func TestMapHasRemove(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	assert.False(m1.Has(NewString("foo")))
	m2 := m1.Set(NewString("foo"), NewString("foo"))
	assert.False(m1.Has(NewString("foo")))
	assert.True(m2.Has(NewString("foo")))
	m3 := m1.Remove(NewString("foo"))
	assert.False(m1.Has(NewString("foo")))
	assert.True(m2.Has(NewString("foo")))
	assert.False(m3.Has(NewString("foo")))
}

func TestMapFirst(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	k, v := m1.First()
	assert.Nil(k)
	assert.Nil(v)

	m1 = m1.Set(NewString("foo"), NewString("bar"))
	m1 = m1.Set(NewString("hot"), NewString("dog"))
	ak, av := m1.First()
	var ek, ev Value

	m1.Iter(func(k, v Value) (stop bool) {
		ek, ev = k, v
		return true
	})

	assert.True(ek.Equals(ak))
	assert.True(ev.Equals(av))
}

func TestMapSetGet(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	assert.Nil(m1.Get(NewString("foo")))
	m2 := m1.Set(NewString("foo"), Int32(42))
	assert.Nil(m1.Get(NewString("foo")))
	assert.True(Int32(42).Equals(m2.Get(NewString("foo"))))
	m3 := m2.Set(NewString("foo"), Int32(43))
	assert.Nil(m1.Get(NewString("foo")))
	assert.True(Int32(42).Equals(m2.Get(NewString("foo"))))
	assert.True(Int32(43).Equals(m3.Get(NewString("foo"))))
	m4 := m3.Remove(NewString("foo"))
	assert.Nil(m1.Get(NewString("foo")))
	assert.True(Int32(42).Equals(m2.Get(NewString("foo"))))
	assert.True(Int32(43).Equals(m3.Get(NewString("foo"))))
	assert.Nil(m4.Get(NewString("foo")))
}

func TestMapSetM(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	m2 := m1.SetM()
	assert.True(m1.Equals(m2))
	m3 := m2.SetM(NewString("foo"), NewString("bar"), NewString("hot"), NewString("dog"))
	assert.Equal(uint64(2), m3.Len())
	assert.True(NewString("bar").Equals(m3.Get(NewString("foo"))))
	assert.True(NewString("dog").Equals(m3.Get(NewString("hot"))))
	m4 := m3.SetM(NewString("mon"), NewString("key"))
	assert.Equal(uint64(2), m3.Len())
	assert.Equal(uint64(3), m4.Len())
}

// BUG 98
func TestMapDuplicateSet(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap(Bool(true), Bool(true), Int32(42), Int32(42), Int32(42), Int32(42))
	assert.Equal(uint64(2), m1.Len())
}

func TestMapIter(t *testing.T) {
	assert := assert.New(t)
	m := NewMap()

	type entry struct {
		key   Value
		value Value
	}

	type resultList []entry
	results := resultList{}
	got := func(key, val Value) bool {
		for _, r := range results {
			if key.Equals(r.key) && val.Equals(r.value) {
				return true
			}
		}
		return false
	}

	stop := false
	cb := func(k, v Value) bool {
		results = append(results, entry{k, v})
		return stop
	}

	m.Iter(cb)
	assert.Equal(0, len(results))

	m = m.SetM(NewString("a"), Int32(0), NewString("b"), Int32(1))
	m.Iter(cb)
	assert.Equal(2, len(results))
	assert.True(got(NewString("a"), Int32(0)))
	assert.True(got(NewString("b"), Int32(1)))

	results = resultList{}
	stop = true
	m.Iter(cb)
	assert.Equal(1, len(results))
	// Iteration order not guaranteed, but it has to be one of these.
	assert.True(got(NewString("a"), Int32(0)) || got(NewString("b"), Int32(1)))
}

func TestMapIterAllP(t *testing.T) {
	assert := assert.New(t)

	testIter := func(concurrency, mapLen int) {
		values := make([]Value, 2*mapLen)
		for i := 0; i < mapLen; i++ {
			values[2*i] = UInt64(i)
			values[2*i+1] = UInt64(i)
		}

		m := NewMap(values...)

		cur := 0
		mu := sync.Mutex{}
		getCur := func() int {
			mu.Lock()
			defer mu.Unlock()
			return cur
		}

		expectConcurreny := concurrency
		if concurrency == 0 {
			expectConcurreny = runtime.NumCPU()
		}
		visited := make([]bool, mapLen)
		f := func(k, v Value) {
			mu.Lock()
			cur++
			mu.Unlock()

			for getCur() < expectConcurreny {
			}

			visited[v.(UInt64)] = true
		}

		if concurrency == 1 {
			m.IterAll(f)
		} else {
			m.IterAllP(concurrency, f)
		}
		numVisited := 0
		for _, visit := range visited {
			if visit {
				numVisited++
			}
		}
		assert.Equal(mapLen, numVisited, "IterAllP was not called with every map key")
	}
	testIter(0, 100)
	testIter(10, 100)
	testIter(1, 100)
	testIter(64, 200)
}

func TestMapFilter(t *testing.T) {
	assert := assert.New(t)

	m := NewMap(Int8(0), NewString("a"), Int8(1), NewString("b"), Int8(2), NewString("c"))
	m2 := m.Filter(func(k, v Value) bool {
		return k.Equals(Int8(0)) || v.Equals(NewString("c"))
	})
	assert.True(NewMap(Int8(0), NewString("a"), Int8(2), NewString("c")).Equals(m2))
}

func TestMapEquals(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	m2 := m1
	m3 := NewMap()

	assert.True(m1.Equals(m2))
	assert.True(m2.Equals(m1))
	assert.True(m3.Equals(m2))
	assert.True(m2.Equals(m3))

	m1 = NewMap(NewString("foo"), Float32(0.0), NewString("bar"), NewList())
	m2 = m2.SetM(NewString("foo"), Float32(0.0), NewString("bar"), NewList())
	assert.True(m1.Equals(m2))
	assert.True(m2.Equals(m1))
	assert.False(m2.Equals(m3))
	assert.False(m3.Equals(m2))
}

func TestMapNotStringKeys(t *testing.T) {
	assert := assert.New(t)
	b1, _ := NewMemoryBlob(bytes.NewBufferString("blob1"))
	b2, _ := NewMemoryBlob(bytes.NewBufferString("blob2"))
	l := []Value{
		Bool(true), NewString("true"),
		Bool(false), NewString("false"),
		Int32(1), NewString("int32: 1"),
		Int32(0), NewString("int32: 0"),
		Float64(1), NewString("float64: 1"),
		Float64(0), NewString("float64: 0"),
		b1, NewString("blob1"),
		b2, NewString("blob2"),
		NewList(), NewString("empty list"),
		NewList(NewList()), NewString("list of list"),
		NewMap(), NewString("empty map"),
		NewMap(NewMap(), NewMap()), NewString("map of map/map"),
		NewSet(), NewString("empty set"),
		NewSet(NewSet()), NewString("map of set/set"),
	}
	m1 := NewMap(l...)
	assert.Equal(uint64(14), m1.Len())
	for i := 0; i < len(l); i += 2 {
		assert.True(m1.Get(l[i]).Equals(l[i+1]))
	}
	assert.Nil(m1.Get(Int32(42)))
}

func TestMapEmpty(t *testing.T) {
	assert := assert.New(t)
	m := NewMap()
	assert.True(m.Empty())
	m = m.Set(Bool(false), NewString("hi"))
	assert.False(m.Empty())
	m = m.Set(NewList(), NewMap())
	assert.False(m.Empty())
}

func TestMapTypeRef(t *testing.T) {
	assert := assert.New(t)
	m := NewMap()
	assert.True(m.TypeRef().Equals(MakeCompoundTypeRef(MapKind, MakePrimitiveTypeRef(ValueKind), MakePrimitiveTypeRef(ValueKind))))

	tr := MakeCompoundTypeRef(MapKind, MakePrimitiveTypeRef(StringKind), MakePrimitiveTypeRef(UInt64Kind))
	m = newMapFromData(mapData{}, tr)
	assert.Equal(tr, m.TypeRef())

	m2 := m.Remove(NewString("B"))
	assert.True(tr.Equals(m2.TypeRef()))

	m = m.Filter(func(k, v Value) bool {
		return true
	})
	assert.True(tr.Equals(m2.TypeRef()))

	m2 = m.Set(NewString("A"), UInt64(1))
	assert.True(tr.Equals(m2.TypeRef()))

	m2 = m.SetM(NewString("B"), UInt64(2), NewString("C"), UInt64(2))
	assert.True(tr.Equals(m2.TypeRef()))

	assert.Panics(func() { m.Set(NewString("A"), UInt8(1)) })
	assert.Panics(func() { m.Set(Bool(true), UInt64(1)) })
	assert.Panics(func() { m.SetM(NewString("B"), UInt64(2), NewString("A"), UInt8(1)) })
	assert.Panics(func() { m.SetM(NewString("B"), UInt64(2), Bool(true), UInt64(1)) })
}

func TestMapChunks(t *testing.T) {
	assert := assert.New(t)

	l1 := NewMap(Int32(0), Int32(1))
	c1 := l1.Chunks()
	assert.Len(c1, 0)

	l2 := NewMap(NewRef(Int32(0).Ref()), Int32(1))
	c2 := l2.Chunks()
	assert.Len(c2, 1)

	l3 := NewMap(Int32(0), NewRef(Int32(1).Ref()))
	c3 := l3.Chunks()
	assert.Len(c3, 1)
}
