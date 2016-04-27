package types

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetLen(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(Bool(true), Number(1), NewString("hi"))
	assert.Equal(uint64(3), s1.Len())
	s2 := s1.Insert(Bool(false))
	assert.Equal(uint64(4), s2.Len())
	s3 := s2.Remove(Bool(true))
	assert.Equal(uint64(3), s3.Len())
}

func TestSetEmpty(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	assert.True(s.Empty())
	s = s.Insert(Bool(false))
	assert.False(s.Empty())
	s = s.Insert(Number(42))
	assert.False(s.Empty())
}

// BUG 98
func TestSetDuplicateInsert(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(Bool(true), Number(42), Number(42))
	assert.Equal(uint64(2), s1.Len())
}

func TestSetUniqueKeysString(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(NewString("hello"), NewString("world"), NewString("hello"))
	assert.Equal(uint64(2), s1.Len())
	assert.True(s1.Has(NewString("hello")))
	assert.True(s1.Has(NewString("world")))
	assert.False(s1.Has(NewString("foo")))
}

func TestSetUniqueKeysInt64(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(Int64(4), Int64(1), Int64(0), Int64(0), Int64(1), Int64(3))
	assert.Equal(uint64(4), s1.Len())
	assert.True(s1.Has(Int64(4)))
	assert.True(s1.Has(Int64(1)))
	assert.True(s1.Has(Int64(0)))
	assert.True(s1.Has(Int64(3)))
	assert.False(s1.Has(Int64(2)))
}

func TestSetHas(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(Bool(true), Number(1), NewString("hi"))
	assert.True(s1.Has(Bool(true)))
	assert.False(s1.Has(Bool(false)))
	assert.True(s1.Has(Number(1)))
	assert.False(s1.Has(Number(0)))
	assert.True(s1.Has(NewString("hi")))
	assert.False(s1.Has(NewString("ho")))

	s2 := s1.Insert(Bool(false))
	assert.True(s2.Has(Bool(false)))
	assert.True(s2.Has(Bool(true)))

	assert.True(s1.Has(Bool(true)))
	assert.False(s1.Has(Bool(false)))
}

func TestSetInsert(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	v1 := Bool(false)
	v2 := Bool(true)
	v3 := Number(0)

	assert.False(s.Has(v1))
	s = s.Insert(v1)
	assert.True(s.Has(v1))
	s = s.Insert(v2)
	assert.True(s.Has(v1))
	assert.True(s.Has(v2))
	s2 := s.Insert(v3)
	assert.True(s.Has(v1))
	assert.True(s.Has(v2))
	assert.False(s.Has(v3))
	assert.True(s2.Has(v1))
	assert.True(s2.Has(v2))
	assert.True(s2.Has(v3))
}

func TestSetRemove(t *testing.T) {
	assert := assert.New(t)
	v1 := Bool(false)
	v2 := Bool(true)
	v3 := Number(0)
	s := NewSet(v1, v2, v3)
	assert.True(s.Has(v1))
	assert.True(s.Has(v2))
	assert.True(s.Has(v3))
	s = s.Remove(v1)
	assert.False(s.Has(v1))
	assert.True(s.Has(v2))
	assert.True(s.Has(v3))
	s2 := s.Remove(v2)
	assert.False(s.Has(v1))
	assert.True(s.Has(v2))
	assert.True(s.Has(v3))
	assert.False(s2.Has(v1))
	assert.False(s2.Has(v2))
	assert.True(s2.Has(v3))

}

func TestSetUnion(t *testing.T) {
	assert := assert.New(t)
	assert.True(NewSet(Number(1), Number(2)).Union(
		NewSet(Number(2), Number(3)),
		NewSet(Number(-1)),
		NewSet()).Equals(
		NewSet(Number(1), Number(2), Number(3), Number(-1))))
	assert.True(NewSet(Number(1)).Union().Equals(NewSet(Number(1))))
}

func TestSetFirst(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	assert.Nil(s.First())
	s = s.Insert(Number(1))
	assert.NotNil(s.First())
	s = s.Insert(Number(2))
	assert.NotNil(s.First())
}

func TestSetIter(t *testing.T) {
	assert := assert.New(t)
	s := NewSet(Number(0), Number(1), Number(2), Number(3), Number(4))
	acc := NewSet()
	s.Iter(func(v Value) bool {
		_, ok := v.(Number)
		assert.True(ok)
		acc = acc.Insert(v)
		return false
	})
	assert.True(s.Equals(acc))

	acc = NewSet()
	s.Iter(func(v Value) bool {
		return true
	})
	assert.True(acc.Empty())
}

func TestSetIterAll(t *testing.T) {
	assert := assert.New(t)
	s := NewSet(Number(0), Number(1), Number(2), Number(3), Number(4))
	acc := NewSet()
	s.IterAll(func(v Value) {
		_, ok := v.(Number)
		assert.True(ok)
		acc = acc.Insert(v)
	})
	assert.True(s.Equals(acc))
}

func TestSetIterAllP(t *testing.T) {
	assert := assert.New(t)

	testIter := func(concurrency, setLen int) {
		values := make([]Value, setLen)
		for i := 0; i < setLen; i++ {
			values[i] = Number(i)
		}

		s := newSetLeaf(setType, values...)

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
		visited := make([]bool, setLen)
		sf := func(v Value) {
			mu.Lock()
			cur++
			mu.Unlock()

			for getCur() < expectConcurreny {
			}

			visited[uint64(v.(Number))] = true
		}

		if concurrency == 1 {
			s.IterAll(sf)
		} else {
			s.IterAllP(concurrency, sf)
		}
		numVisited := 0
		for _, visit := range visited {
			if visit {
				numVisited++
			}
		}
		assert.Equal(setLen, numVisited, "IterAllP was not called with every index")
	}
	testIter(0, 100)
	testIter(10, 100)
	testIter(1, 100)
	testIter(64, 200)
}

func testSetOrder(assert *assert.Assertions, valueType *Type, value []Value, expectOrdering []Value) {
	mapTr := MakeSetType(valueType)
	m := NewTypedSet(mapTr, value...)
	i := 0
	m.IterAll(func(value Value) {
		assert.Equal(expectOrdering[i].Ref().String(), value.Ref().String())
		i++
	})
}

func TestSetOrdering(t *testing.T) {
	assert := assert.New(t)

	testSetOrder(assert,
		StringType,
		[]Value{
			NewString("a"),
			NewString("z"),
			NewString("b"),
			NewString("y"),
			NewString("c"),
			NewString("x"),
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

	testSetOrder(assert,
		NumberType,
		[]Value{
			Number(0),
			Number(1000),
			Number(1),
			Number(100),
			Number(2),
			Number(10),
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

	testSetOrder(assert,
		NumberType,
		[]Value{
			Number(0),
			Number(-30),
			Number(25),
			Number(1002),
			Number(-5050),
			Number(23),
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

	testSetOrder(assert,
		NumberType,
		[]Value{
			Number(0.0001),
			Number(0.000001),
			Number(1),
			Number(25.01e3),
			Number(-32.231123e5),
			Number(23),
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

	testSetOrder(assert,
		ValueType,
		[]Value{
			NewString("a"),
			NewString("z"),
			NewString("b"),
			NewString("y"),
			NewString("c"),
			NewString("x"),
		},
		// Ordered by ref
		[]Value{
			NewString("x"),
			NewString("c"),
			NewString("y"),
			NewString("z"),
			NewString("a"),
			NewString("b"),
		},
	)

	testSetOrder(assert,
		BoolType,
		[]Value{
			Bool(true),
			Bool(false),
		},
		// Ordered by ref
		[]Value{
			Bool(true),
			Bool(false),
		},
	)
}

func TestSetFilter(t *testing.T) {
	assert := assert.New(t)

	s := NewSet(Number(0), Number(1), Number(2), Number(3), Number(4))
	s2 := s.Filter(func(v Value) bool {
		i, ok := v.(Number)
		assert.True(ok)
		return uint64(i)%2 == 0
	})

	assert.True(NewSet(Number(0), Number(2), Number(4)).Equals(s2))
}

func TestSetType(t *testing.T) {
	assert := assert.New(t)

	s := newSetLeaf(setType)
	assert.True(s.Type().Equals(MakeSetType(ValueType)))

	tr := MakeSetType(NumberType)

	s = newSetLeaf(tr)
	assert.Equal(tr, s.Type())

	s2 := s.Remove(Number(1))
	assert.True(tr.Equals(s2.Type()))

	s2 = s.Filter(func(v Value) bool {
		return true
	})
	assert.True(tr.Equals(s2.Type()))

	s2 = s.Insert(Number(0), Number(1))
	assert.True(tr.Equals(s2.Type()))

	assert.Panics(func() { s.Insert(Bool(true)) })
	assert.Panics(func() { s.Insert(Number(3), Bool(true)) })
	assert.Panics(func() { s.Union(NewSet(Number(2))) })
	assert.Panics(func() { s.Union(NewSet(Bool(true))) })
	assert.Panics(func() { s.Union(s, NewSet(Bool(true))) })
}

func TestSetChunks(t *testing.T) {
	assert := assert.New(t)

	l1 := NewSet(Number(0))
	c1 := l1.Chunks()
	assert.Len(c1, 0)

	l2 := NewSet(NewRef(Number(0).Ref()))
	c2 := l2.Chunks()
	assert.Len(c2, 1)
}
