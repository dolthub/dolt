package types

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListLen(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	assert.Equal(uint64(0), l.Len())
	l = l.Append(Bool(true))
	assert.Equal(uint64(1), l.Len())
	l = l.Append(Bool(false), Bool(false))
	assert.Equal(uint64(3), l.Len())
}

func TestListEmpty(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	assert.True(l.Empty())
	l = l.Append(Bool(true))
	assert.False(l.Empty())
	l = l.Append(Bool(false), Bool(false))
	assert.False(l.Empty())
}

func TestListGet(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	l = l.Append(Int32(0), Int32(1), Int32(2))
	assert.Equal(Int32(0), l.Get(0))
	assert.Equal(Int32(1), l.Get(1))
	assert.Equal(Int32(2), l.Get(2))

	assert.Panics(func() {
		l.Get(3)
	})
}

func TestListSlice(t *testing.T) {
	assert := assert.New(t)

	l1 := NewList()
	l1 = l1.Append(Int32(0), Int32(1), Int32(2), Int32(3))
	l2 := l1.Slice(1, 3)
	assert.Equal(uint64(4), l1.Len())
	assert.Equal(uint64(2), l2.Len())
	assert.Equal(Int32(1), l2.Get(0))
	assert.Equal(Int32(2), l2.Get(1))

	l3 := l1.Slice(0, 0)
	assert.Equal(uint64(0), l3.Len())
	l3 = l1.Slice(1, 1)
	assert.Equal(uint64(0), l3.Len())
	l3 = l1.Slice(1, 2)
	assert.Equal(uint64(1), l3.Len())
	assert.Equal(Int32(1), l3.Get(0))
	l3 = l1.Slice(0, l1.Len())
	assert.True(l1.Equals(l3))

	assert.Panics(func() {
		l3 = l1.Slice(0, l1.Len()+1)
	})
	assert.Panics(func() {
		l3 = l1.Slice(l1.Len()+1, l1.Len()+2)
	})
}

func TestListSet(t *testing.T) {
	assert := assert.New(t)

	l0 := NewList()
	l0 = l0.Append(Float32(0.0))
	l1 := l0.Set(uint64(0), Float32(1.0))
	assert.Equal(Float32(1.0), l1.Get(0))
	assert.Equal(Float32(0.0), l0.Get(0))
	assert.Panics(func() {
		l1.Set(uint64(2), Float32(2.0))
	})
}

func TestListAppend(t *testing.T) {
	assert := assert.New(t)

	l0 := NewList()
	l1 := l0.Append(Bool(false))
	assert.Equal(uint64(0), l0.Len())
	assert.Equal(uint64(1), l1.Len())
	assert.Equal(Bool(false), l1.Get(0))

	// Append(v1, v2)
	l2 := l1.Append(Bool(true), Bool(true))
	assert.Equal(uint64(3), l2.Len())
	assert.Equal(Bool(false), l2.Get(0))
	assert.True(NewList(Bool(true), Bool(true)).Equals(l2.Slice(1, l2.Len())))
	assert.Equal(uint64(1), l1.Len())
}

func TestListInsert(t *testing.T) {
	assert := assert.New(t)

	// Insert(0, v1)
	l0 := NewList()
	l1 := l0.Insert(uint64(0), Int32(-1))
	assert.Equal(uint64(0), l0.Len())
	assert.Equal(uint64(1), l1.Len())
	assert.Equal(Int32(-1), l1.Get(0))

	// Insert(0, v1, v2)
	l2 := l1.Insert(0, Int32(-3), Int32(-2))
	assert.Equal(uint64(3), l2.Len())
	assert.Equal(Int32(-1), l2.Get(2))
	assert.True(NewList(Int32(-3), Int32(-2)).Equals(l2.Slice(0, 2)))
	assert.Equal(uint64(1), l1.Len())

	// Insert(2, v3)
	l3 := l2.Insert(2, Int32(1))
	assert.Equal(Int32(1), l3.Get(2))

	assert.Panics(func() {
		l2.Insert(5, Int32(0))
	})
}

func TestListRemove(t *testing.T) {
	assert := assert.New(t)

	l0 := NewList()
	l0 = l0.Remove(0, 0)
	assert.Equal(uint64(0), l0.Len())

	l0 = l0.Append(Bool(false), Bool(true), Bool(true), Bool(false))
	l1 := l0.Remove(1, 3)
	assert.Equal(uint64(4), l0.Len())
	assert.Equal(uint64(2), l1.Len())
	assert.True(NewList(Bool(false), Bool(false)).Equals(l1))

	l1 = l1.Remove(1, 2)
	assert.True(NewList(Bool(false)).Equals(l1))

	l1 = l1.Remove(0, 1)
	assert.True(NewList().Equals(l1))

	assert.Panics(func() {
		l1.Remove(0, 1)
	})
}

func TestListRemoveAt(t *testing.T) {
	assert := assert.New(t)

	l0 := NewList()
	l0 = l0.Append(Bool(false), Bool(true))
	l1 := l0.RemoveAt(1)
	assert.True(NewList(Bool(false)).Equals(l1))
	l1 = l1.RemoveAt(0)
	assert.True(NewList().Equals(l1))

	assert.Panics(func() {
		l1.RemoveAt(0)
	})
}

func TestListMap(t *testing.T) {
	assert := assert.New(t)

	testMap := func(concurrency, listLen int) {
		values := make([]Value, listLen)
		for i := 0; i < listLen; i++ {
			values[i] = Int64(i)
		}

		l := newListLeaf(listType, values...)

		cur := 0
		mu := sync.Mutex{}
		getCur := func() int {
			mu.Lock()
			defer mu.Unlock()
			return cur
		}

		// Note: The only way I can think of to test that concurrency doesn't go above target is a time out, which is obviously bad.
		expectConcurreny := concurrency
		if concurrency == 0 {
			expectConcurreny = runtime.NumCPU()
		}

		mf := func(v Value, index uint64) interface{} {
			mu.Lock()
			cur++
			mu.Unlock()

			for getCur() < expectConcurreny {
			}

			i := v.(Int64)
			assert.Equal(uint64(i), index, "%d == %d", i, index)
			return int64(i * i)
		}

		var mapped []interface{}
		if concurrency == 1 {
			mapped = l.Map(mf)
		} else {
			mapped = l.MapP(concurrency, mf)
		}

		assert.Equal(uint64(len(mapped)), l.Len())

		for i, v := range mapped {
			val := v.(int64)
			assert.Equal(val, int64(i*i))
		}
	}

	testMap(0, 100)
	testMap(10, 1000)
	testMap(1, 100000)
}

func TestListIter(t *testing.T) {
	assert := assert.New(t)

	l := NewList(Int32(0), Int32(1), Int32(2), Int32(3), Int32(4))
	acc := []int32{}
	i := uint64(0)
	l.Iter(func(v Value, index uint64) bool {
		assert.Equal(i, index)
		i++
		acc = append(acc, int32(v.(Int32)))
		return i > 2
	})
	assert.Equal([]int32{0, 1, 2}, acc)

	cl := NewList(NewString("a"), NewString("b"), NewString("c"), NewString("d"), NewString("e"), NewString("f"))
	acc2 := []string{}
	cl.Iter(func(v Value, i uint64) bool {
		acc2 = append(acc2, v.(String).String())
		return false
	})
	assert.Equal([]string{"a", "b", "c", "d", "e", "f"}, acc2)

	cl2 := cl
	acc3 := []string{}
	i = uint64(0)
	cl2.Iter(func(v Value, index uint64) bool {
		assert.Equal(i, index)
		i++
		acc3 = append(acc3, v.(String).String())
		return i > 3
	})
	assert.Equal([]string{"a", "b", "c", "d"}, acc3)
}

func TestListIterAll(t *testing.T) {
	assert := assert.New(t)

	l := NewList(Int32(0), Int32(1), Int32(2), Int32(3), Int32(4))
	acc := []int32{}
	i := uint64(0)
	l.IterAll(func(v Value, index uint64) {
		assert.Equal(i, index)
		i++
		acc = append(acc, int32(v.(Int32)))
	})
	assert.Equal([]int32{0, 1, 2, 3, 4}, acc)

	cl := NewList(NewString("a"), NewString("b"), NewString("c"), NewString("d"), NewString("e"), NewString("f"))
	acc2 := []string{}
	cl.IterAll(func(v Value, i uint64) {
		acc2 = append(acc2, v.(String).String())
	})
	assert.Equal([]string{"a", "b", "c", "d", "e", "f"}, acc2)
}

func TestListIterAllP(t *testing.T) {
	assert := assert.New(t)

	testIter := func(concurrency, listLen int) {
		values := make([]Value, listLen)
		for i := 0; i < listLen; i++ {
			values[i] = Int64(i)
		}

		l := newListLeaf(listType, values...)

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
		visited := make([]bool, listLen, listLen)
		lf := func(v Value, index uint64) {
			mu.Lock()
			cur++
			mu.Unlock()

			for getCur() < expectConcurreny {
			}

			i := v.(Int64)
			visited[index] = true
			assert.Equal(uint64(i), index, "%d == %d", i, index)
		}

		if concurrency == 1 {
			l.IterAll(lf)
		} else {
			l.IterAllP(concurrency, lf)
		}
		numVisited := 0
		for _, visit := range visited {
			if visit {
				numVisited++
			}
		}
		assert.Equal(listLen, numVisited, "IterAllP was not called with every index")
	}
	testIter(0, 100)
	testIter(10, 1000)
	testIter(1, 100000)
	testIter(64, 100000)
}

func TestListType(t *testing.T) {
	assert := assert.New(t)

	l := NewList(Int32(0))
	assert.True(l.Type().Equals(MakeCompoundType(ListKind, MakePrimitiveType(ValueKind))))

	tr := MakeCompoundType(ListKind, MakePrimitiveType(Uint8Kind))
	l2 := newListLeaf(tr, []Value{Uint8(0), Uint8(1)}...)
	assert.Equal(tr, l2.Type())

	l3 := l2.Slice(0, 1)
	assert.True(tr.Equals(l3.Type()))
	l3 = l2.Remove(0, 1)
	assert.True(tr.Equals(l3.Type()))
	l3 = l2.RemoveAt(0)
	assert.True(tr.Equals(l3.Type()))

	l3 = l2.Set(0, Uint8(11))
	assert.True(tr.Equals(l3.Type()))
	l3 = l2.Append(Uint8(2))
	assert.True(tr.Equals(l3.Type()))
	l3 = l2.Insert(0, Uint8(3))
	assert.True(tr.Equals(l3.Type()))

	assert.Panics(func() { l2.Set(0, NewString("")) })
	assert.Panics(func() { l2.Append(NewString("")) })
	assert.Panics(func() { l2.Insert(0, NewString("")) })
}

func TestListChunks(t *testing.T) {
	assert := assert.New(t)

	l1 := NewList(Int32(0))
	c1 := l1.Chunks()
	assert.Len(c1, 0)

	l2 := NewList(NewRef(Int32(0).Ref()))
	c2 := l2.Chunks()
	assert.Len(c2, 1)

	l3 := NewList(l2)
	c3 := l3.Chunks()
	assert.Len(c3, 1)
	assert.Equal(Int32(0).Ref(), c3[0])
}
