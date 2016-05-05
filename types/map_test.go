package types

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMap(t *testing.T) {
	assert := assert.New(t)
	m := newMapLeaf(mapType)
	assert.IsType(newMapLeaf(mapType), m)
	assert.Equal(uint64(0), m.Len())
	m = NewMap(NewString("foo"), NewString("foo"), NewString("bar"), NewString("bar"))
	assert.Equal(uint64(2), m.Len())
	assert.True(NewString("foo").Equals(m.Get(NewString("foo"))))
	assert.True(NewString("bar").Equals(m.Get(NewString("bar"))))
}

func TestMapUniqueKeysString(t *testing.T) {
	assert := assert.New(t)
	l := []Value{
		NewString("hello"), NewString("world"),
		NewString("foo"), NewString("bar"),
		NewString("bar"), NewString("foo"),
		NewString("hello"), NewString("foo"),
	}
	m := NewMap(l...)
	assert.Equal(uint64(3), m.Len())
	assert.True(NewString("foo").Equals(m.Get(NewString("hello"))))
}

func TestMapUniqueKeysNumber(t *testing.T) {
	assert := assert.New(t)
	l := []Value{
		Number(4), Number(1),
		Number(0), Number(2),
		Number(1), Number(2),
		Number(3), Number(4),
		Number(1), Number(5),
	}
	m := NewMap(l...)
	assert.Equal(uint64(4), m.Len())
	assert.True(Number(5).Equals(m.Get(Number(1))))
}

func TestMapHasRemove(t *testing.T) {
	assert := assert.New(t)
	m1 := newMapLeaf(mapType)
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
	m1 := newMapLeaf(mapType)
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
	m1 := newMapLeaf(mapType)
	assert.Nil(m1.Get(NewString("foo")))
	m2 := m1.Set(NewString("foo"), Number(42))
	assert.Nil(m1.Get(NewString("foo")))
	assert.True(Number(42).Equals(m2.Get(NewString("foo"))))
	m3 := m2.Set(NewString("foo"), Number(43))
	assert.Nil(m1.Get(NewString("foo")))
	assert.True(Number(42).Equals(m2.Get(NewString("foo"))))
	assert.True(Number(43).Equals(m3.Get(NewString("foo"))))
	m4 := m3.Remove(NewString("foo"))
	assert.Nil(m1.Get(NewString("foo")))
	assert.True(Number(42).Equals(m2.Get(NewString("foo"))))
	assert.True(Number(43).Equals(m3.Get(NewString("foo"))))
	assert.Nil(m4.Get(NewString("foo")))
}

func TestMapSetM(t *testing.T) {
	assert := assert.New(t)
	m1 := newMapLeaf(mapType)
	m2 := m1.SetM()
	assert.True(m1.Equals(m2))
	m3 := m2.SetM(NewString("foo"), NewString("bar"), NewString("hot"), NewString("dog"))
	assert.Equal(uint64(2), m3.Len())
	assert.True(NewString("bar").Equals(m3.Get(NewString("foo"))))
	assert.True(NewString("dog").Equals(m3.Get(NewString("hot"))))
	// TODO: Enable when CompoundMap.Len() is implemented
	// m4 := m3.SetM(NewString("mon"), NewString("key"))
	// assert.Equal(uint64(2), m3.Len())
	// assert.Equal(uint64(3), m4.Len())
}

// BUG 98
func TestMapDuplicateSet(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap(Bool(true), Bool(true), Number(42), Number(42), Number(42), Number(42))
	assert.Equal(uint64(2), m1.Len())
}

func TestMapIter(t *testing.T) {
	assert := assert.New(t)
	m := newMapLeaf(mapType)

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

	m = m.SetM(NewString("a"), Number(0), NewString("b"), Number(1))
	m.Iter(cb)
	assert.Equal(2, len(results))
	assert.True(got(NewString("a"), Number(0)))
	assert.True(got(NewString("b"), Number(1)))

	results = resultList{}
	stop = true
	m.Iter(cb)
	assert.Equal(1, len(results))
	// Iteration order not guaranteed, but it has to be one of these.
	assert.True(got(NewString("a"), Number(0)) || got(NewString("b"), Number(1)))
}

func TestMapFilter(t *testing.T) {
	assert := assert.New(t)

	m := NewMap(Number(0), NewString("a"), Number(1), NewString("b"), Number(2), NewString("c"))
	m2 := m.Filter(func(k, v Value) bool {
		return k.Equals(Number(0)) || v.Equals(NewString("c"))
	})
	assert.True(NewMap(Number(0), NewString("a"), Number(2), NewString("c")).Equals(m2))
}

func TestMapEquals(t *testing.T) {
	assert := assert.New(t)

	m1 := newMapLeaf(mapType)
	m2 := m1
	m3 := newMapLeaf(mapType)

	assert.True(m1.Equals(m2))
	assert.True(m2.Equals(m1))
	assert.True(m3.Equals(m2))
	assert.True(m2.Equals(m3))

	m1 = NewMap(NewString("foo"), Number(0.0), NewString("bar"), NewList())
	m2 = m2.SetM(NewString("foo"), Number(0.0), NewString("bar"), NewList())
	assert.True(m1.Equals(m2))
	assert.True(m2.Equals(m1))
	assert.False(m2.Equals(m3))
	assert.False(m3.Equals(m2))
}

func TestMapNotStringKeys(t *testing.T) {
	assert := assert.New(t)

	b1 := NewBlob(bytes.NewBufferString("blob1"))
	b2 := NewBlob(bytes.NewBufferString("blob2"))
	l := []Value{
		Bool(true), NewString("true"),
		Bool(false), NewString("false"),
		Number(1), NewString("Number: 1"),
		Number(0), NewString("Number: 0"),
		b1, NewString("blob1"),
		b2, NewString("blob2"),
		NewList(), NewString("empty list"),
		NewList(NewList()), NewString("list of list"),
		newMapLeaf(mapType), NewString("empty map"),
		NewMap(newMapLeaf(mapType), newMapLeaf(mapType)), NewString("map of map/map"),
		NewSet(), NewString("empty set"),
		NewSet(NewSet()), NewString("map of set/set"),
	}
	m1 := NewMap(l...)
	assert.Equal(uint64(12), m1.Len())
	for i := 0; i < len(l); i += 2 {
		assert.True(m1.Get(l[i]).Equals(l[i+1]))
	}
	assert.Nil(m1.Get(Number(42)))
}

func testMapOrder(assert *assert.Assertions, keyType, valueType *Type, tuples []Value, expectOrdering []Value) {
	mapTr := MakeMapType(keyType, valueType)
	m := NewTypedMap(mapTr, tuples...)
	i := 0
	m.IterAll(func(key, value Value) {
		assert.Equal(expectOrdering[i].Ref().String(), key.Ref().String())
		i++
	})
}

func TestMapOrdering(t *testing.T) {
	assert := assert.New(t)

	testMapOrder(assert,
		StringType, StringType,
		[]Value{
			NewString("a"), NewString("unused"),
			NewString("z"), NewString("unused"),
			NewString("b"), NewString("unused"),
			NewString("y"), NewString("unused"),
			NewString("c"), NewString("unused"),
			NewString("x"), NewString("unused"),
		},
		[]Value{
			NewString("a"),
			NewString("b"),
			NewString("c"),
			NewString("x"),
			NewString("y"),
			NewString("z"),
		},
	)

	testMapOrder(assert,
		NumberType, StringType,
		[]Value{
			Number(0), NewString("unused"),
			Number(1000), NewString("unused"),
			Number(1), NewString("unused"),
			Number(100), NewString("unused"),
			Number(2), NewString("unused"),
			Number(10), NewString("unused"),
		},
		[]Value{
			Number(0),
			Number(1),
			Number(2),
			Number(10),
			Number(100),
			Number(1000),
		},
	)

	testMapOrder(assert,
		NumberType, StringType,
		[]Value{
			Number(0), NewString("unused"),
			Number(-30), NewString("unused"),
			Number(25), NewString("unused"),
			Number(1002), NewString("unused"),
			Number(-5050), NewString("unused"),
			Number(23), NewString("unused"),
		},
		[]Value{
			Number(-5050),
			Number(-30),
			Number(0),
			Number(23),
			Number(25),
			Number(1002),
		},
	)

	testMapOrder(assert,
		NumberType, StringType,
		[]Value{
			Number(0.0001), NewString("unused"),
			Number(0.000001), NewString("unused"),
			Number(1), NewString("unused"),
			Number(25.01e3), NewString("unused"),
			Number(-32.231123e5), NewString("unused"),
			Number(23), NewString("unused"),
		},
		[]Value{
			Number(-32.231123e5),
			Number(0.000001),
			Number(0.0001),
			Number(1),
			Number(23),
			Number(25.01e3),
		},
	)

	testMapOrder(assert,
		ValueType, StringType,
		[]Value{
			NewString("a"), NewString("unused"),
			NewString("z"), NewString("unused"),
			NewString("b"), NewString("unused"),
			NewString("y"), NewString("unused"),
			NewString("c"), NewString("unused"),
			NewString("x"), NewString("unused"),
		},
		// Ordered by ref
		[]Value{
			NewString("z"),
			NewString("c"),
			NewString("a"),
			NewString("x"),
			NewString("b"),
			NewString("y"),
		},
	)

	testMapOrder(assert,
		BoolType, StringType,
		[]Value{
			Bool(true), NewString("unused"),
			Bool(false), NewString("unused"),
		},
		// Ordered by ref
		[]Value{
			Bool(true),
			Bool(false),
		},
	)
}

func TestMapEmpty(t *testing.T) {
	assert := assert.New(t)

	m := newMapLeaf(mapType)
	assert.True(m.Empty())
	m = m.Set(Bool(false), NewString("hi"))
	assert.False(m.Empty())
	m = m.Set(NewList(), newMapLeaf(mapType))
	assert.False(m.Empty())
}

func TestMapType(t *testing.T) {
	assert := assert.New(t)

	m := newMapLeaf(mapType)
	assert.True(m.Type().Equals(MakeMapType(ValueType, ValueType)))

	tr := MakeMapType(StringType, NumberType)
	m = newMapLeaf(tr)
	assert.Equal(tr, m.Type())

	m2 := m.Remove(NewString("B"))
	assert.True(tr.Equals(m2.Type()))

	m = m.Filter(func(k, v Value) bool {
		return true
	})
	assert.True(tr.Equals(m2.Type()))

	m2 = m.Set(NewString("A"), Number(1))
	assert.True(tr.Equals(m2.Type()))

	m2 = m.SetM(NewString("B"), Number(2), NewString("C"), Number(2))
	assert.True(tr.Equals(m2.Type()))

	assert.Panics(func() { m.Set(NewString("A"), Bool(true)) })
	assert.Panics(func() { m.Set(Bool(true), Number(1)) })
	assert.Panics(func() { m.SetM(NewString("B"), Bool(false), NewString("A"), Bool(true)) })
	assert.Panics(func() { m.SetM(NewString("B"), Number(2), Bool(true), Number(1)) })
}

func TestMapChunks(t *testing.T) {
	assert := assert.New(t)

	l1 := NewMap(Number(0), Number(1))
	c1 := l1.Chunks()
	assert.Len(c1, 0)

	l2 := NewMap(NewRef(Number(0).Ref()), Number(1))
	c2 := l2.Chunks()
	assert.Len(c2, 1)

	l3 := NewMap(Number(0), NewRef(Number(1).Ref()))
	c3 := l3.Chunks()
	assert.Len(c3, 1)
}
