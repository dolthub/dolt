package types

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

type testSimpleList []Value

func (tsl testSimpleList) Set(idx int, v Value) (res testSimpleList) {
	res = append(res, tsl[:idx]...)
	res = append(res, v)
	res = append(res, tsl[idx+1:]...)
	return
}

func (tsl testSimpleList) Insert(idx int, vs ...Value) (res testSimpleList) {
	res = append(res, tsl[:idx]...)
	res = append(res, vs...)
	res = append(res, tsl[idx:]...)
	return
}

func (tsl testSimpleList) Remove(start, end int) (res testSimpleList) {
	res = append(res, tsl[:start]...)
	res = append(res, tsl[end:]...)
	return
}

func (tsl testSimpleList) RemoveAt(idx int) testSimpleList {
	return tsl.Remove(idx, idx+1)
}

func (tsl testSimpleList) toCompoundList() compoundList {
	return NewList(tsl...).(compoundList)
}

func getTestSimpleListLen() uint64 {
	return uint64(listPattern) * 50
}

func getTestSimpleList() testSimpleList {
	length := int(getTestSimpleListLen())
	s := rand.NewSource(42)
	values := make([]Value, length)
	for i := 0; i < length; i++ {
		values[i] = Int64(s.Int63() & 0xff)
	}

	return values
}

func getTestSimpleListUnique() testSimpleList {
	length := int(getTestSimpleListLen())
	s := rand.NewSource(42)
	uniques := map[int64]bool{}
	for len(uniques) < length {
		uniques[s.Int63()] = true
	}
	values := make([]Value, 0, length)
	for k := range uniques {
		values = append(values, Int64(k))
	}
	return values
}

func testSimpleListFromNomsList(list List) testSimpleList {
	simple := make(testSimpleList, list.Len())
	list.IterAll(func(v Value, offset uint64) {
		simple[offset] = v
	})
	return simple
}

func TestStreamingCompoundListCreation(t *testing.T) {
	assert := assert.New(t)

	vs := NewTestValueStore()
	simpleList := getTestSimpleList()

	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(tr, simpleList...)
	valueChan := make(chan Value)
	listChan := NewStreamingTypedList(tr, vs, valueChan)
	for _, v := range simpleList {
		valueChan <- v
	}
	close(valueChan)
	sl := <-listChan
	assert.True(cl.Equals(sl))
	cl.Iter(func(v Value, idx uint64) (done bool) {
		done = !assert.EqualValues(v, sl.Get(idx))
		return
	})
}

func TestCompoundListGet(t *testing.T) {
	assert := assert.New(t)

	vs := NewTestValueStore()
	simpleList := getTestSimpleList()

	testGet := func(cl compoundList) {
		// Incrementing by len(simpleList)/10 because Get() is too slow to run on every index.
		for i := 0; i < len(simpleList); i += len(simpleList) / 10 {
			assert.Equal(simpleList[i], cl.Get(uint64(i)))
		}
	}

	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(tr, simpleList...).(compoundList)
	testGet(cl)
	testGet(vs.ReadValue(vs.WriteValue(cl)).(compoundList))
}

func TestCompoundListIter(t *testing.T) {
	assert := assert.New(t)

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(tr, simpleList...)

	expectIdx := uint64(0)
	endAt := getTestSimpleListLen() / 2
	cl.Iter(func(v Value, idx uint64) bool {
		assert.Equal(expectIdx, idx)
		expectIdx++
		assert.Equal(simpleList[idx], v)
		return expectIdx == endAt
	})

	assert.Equal(endAt, expectIdx)
}

func TestCompoundListIterAll(t *testing.T) {
	assert := assert.New(t)

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(tr, simpleList...)

	expectIdx := uint64(0)
	cl.IterAll(func(v Value, idx uint64) {
		assert.Equal(expectIdx, idx)
		expectIdx++
		assert.Equal(simpleList[idx], v)
	})

	assert.Equal(getTestSimpleListLen(), expectIdx)
}

func TestCompoundListIterAllP(t *testing.T) {
	assert := assert.New(t)

	mu := sync.Mutex{}

	simpleList := getTestSimpleListUnique()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(tr, simpleList...)

	indexes := map[Value]uint64{}
	for i, v := range simpleList {
		indexes[v] = uint64(i)
	}
	visited := map[Value]bool{}
	cl.IterAllP(64, func(v Value, idx uint64) {
		mu.Lock()
		_, seen := visited[v]
		visited[v] = true
		mu.Unlock()
		assert.False(seen)
		assert.Equal(idx, indexes[v])
	})

	assert.Equal(len(simpleList), len(visited))
}

func TestCompoundListMap(t *testing.T) {
	assert := assert.New(t)

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(tr, simpleList...)

	l := cl.Map(func(v Value, i uint64) interface{} {
		v1 := v.(Int64)
		return v1 + Int64(i)
	})

	assert.Equal(uint64(len(l)), cl.Len())
	for i := 0; i < len(l); i++ {
		assert.Equal(l[i], cl.Get(uint64(i)).(Int64)+Int64(i))
	}
}

func TestCompoundListMapP(t *testing.T) {
	assert := assert.New(t)

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(tr, simpleList...)

	l := cl.MapP(64, func(v Value, i uint64) interface{} {
		v1 := v.(Int64)
		return v1 + Int64(i)
	})

	assert.Equal(uint64(len(l)), cl.Len())
	for i := 0; i < len(l); i++ {
		assert.Equal(l[i], cl.Get(uint64(i)).(Int64)+Int64(i))
	}
}

func TestCompoundListLen(t *testing.T) {
	assert := assert.New(t)

	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))

	cl := NewTypedList(tr, getTestSimpleList()...).(compoundList)
	assert.Equal(getTestSimpleListLen(), cl.Len())
	cl = NewTypedList(tr, append(getTestSimpleList(), getTestSimpleList()...)...).(compoundList)
	assert.Equal(getTestSimpleListLen()*2, cl.Len())
}

func TestCompoundListCursorAt(t *testing.T) {
	assert := assert.New(t)

	listLen := func(at uint64, next func(*sequenceCursor) bool) (size uint64) {
		cs := NewTestValueStore()
		tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
		cl := NewTypedList(tr, getTestSimpleList()...).(compoundList)
		cur, _, _ := cl.cursorAt(at)
		for {
			size += readMetaTupleValue(cur.current(), cs).(List).Len()
			if !next(cur) {
				return
			}
		}
	}

	assert.Equal(getTestSimpleListLen(), listLen(0, func(cur *sequenceCursor) bool {
		return cur.advance()
	}))
	assert.Equal(getTestSimpleListLen(), listLen(getTestSimpleListLen(), func(cur *sequenceCursor) bool {
		return cur.retreat()
	}))
}

func TestCompoundListAppend(t *testing.T) {
	assert := assert.New(t)

	newCompoundList := func(items testSimpleList) compoundList {
		tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
		return NewTypedList(tr, items...).(compoundList)
	}

	compoundToSimple := func(cl List) (simple testSimpleList) {
		cl.IterAll(func(v Value, offset uint64) {
			simple = append(simple, v)
		})
		return
	}

	cl := newCompoundList(getTestSimpleList())
	cl2 := cl.Append(Int64(42))
	cl3 := cl2.Append(Int64(43))
	cl4 := cl3.Append(getTestSimpleList()...)
	cl5 := cl4.Append(Int64(44), Int64(45))
	cl6 := cl5.Append(getTestSimpleList()...)

	expected := getTestSimpleList()
	assert.Equal(expected, compoundToSimple(cl))
	assert.Equal(getTestSimpleListLen(), cl.Len())
	assert.True(newCompoundList(expected).Equals(cl))

	expected = append(expected, Int64(42))
	assert.Equal(expected, compoundToSimple(cl2))
	assert.Equal(getTestSimpleListLen()+1, cl2.Len())
	assert.True(newCompoundList(expected).Equals(cl2))

	expected = append(expected, Int64(43))
	assert.Equal(expected, compoundToSimple(cl3))
	assert.Equal(getTestSimpleListLen()+2, cl3.Len())
	assert.True(newCompoundList(expected).Equals(cl3))

	expected = append(expected, getTestSimpleList()...)
	assert.Equal(expected, compoundToSimple(cl4))
	assert.Equal(2*getTestSimpleListLen()+2, cl4.Len())
	assert.True(newCompoundList(expected).Equals(cl4))

	expected = append(expected, Int64(44), Int64(45))
	assert.Equal(expected, compoundToSimple(cl5))
	assert.Equal(2*getTestSimpleListLen()+4, cl5.Len())
	assert.True(newCompoundList(expected).Equals(cl5))

	expected = append(expected, getTestSimpleList()...)
	assert.Equal(expected, compoundToSimple(cl6))
	assert.Equal(3*getTestSimpleListLen()+4, cl6.Len())
	assert.True(newCompoundList(expected).Equals(cl6))
}

func TestCompoundListInsertNothing(t *testing.T) {
	assert := assert.New(t)

	cl := getTestSimpleList().toCompoundList()

	assert.True(cl.Equals(cl.Insert(0)))
	for i := uint64(1); i < getTestSimpleListLen(); i *= 2 {
		assert.True(cl.Equals(cl.Insert(i)))
	}
	assert.True(cl.Equals(cl.Insert(cl.Len() - 1)))
	assert.True(cl.Equals(cl.Insert(cl.Len())))
}

func TestCompoundListInsertStart(t *testing.T) {
	assert := assert.New(t)

	cl := getTestSimpleList().toCompoundList()
	cl2 := cl.Insert(0, Int64(42))
	cl3 := cl2.Insert(0, Int64(43))
	cl4 := cl3.Insert(0, getTestSimpleList()...)
	cl5 := cl4.Insert(0, Int64(44), Int64(45))
	cl6 := cl5.Insert(0, getTestSimpleList()...)

	expected := getTestSimpleList()
	assert.Equal(expected, testSimpleListFromNomsList(cl))
	assert.Equal(getTestSimpleListLen(), cl.Len())
	assert.True(expected.toCompoundList().Equals(cl))

	expected = expected.Insert(0, Int64(42))
	assert.Equal(expected, testSimpleListFromNomsList(cl2))
	assert.Equal(getTestSimpleListLen()+1, cl2.Len())
	assert.True(expected.toCompoundList().Equals(cl2))

	expected = expected.Insert(0, Int64(43))
	assert.Equal(expected, testSimpleListFromNomsList(cl3))
	assert.Equal(getTestSimpleListLen()+2, cl3.Len())
	assert.True(expected.toCompoundList().Equals(cl3))

	expected = expected.Insert(0, getTestSimpleList()...)
	assert.Equal(expected, testSimpleListFromNomsList(cl4))
	assert.Equal(2*getTestSimpleListLen()+2, cl4.Len())
	assert.True(expected.toCompoundList().Equals(cl4))

	expected = expected.Insert(0, Int64(44), Int64(45))
	assert.Equal(expected, testSimpleListFromNomsList(cl5))
	assert.Equal(2*getTestSimpleListLen()+4, cl5.Len())
	assert.True(expected.toCompoundList().Equals(cl5))

	expected = expected.Insert(0, getTestSimpleList()...)
	assert.Equal(expected, testSimpleListFromNomsList(cl6))
	assert.Equal(3*getTestSimpleListLen()+4, cl6.Len())
	assert.True(expected.toCompoundList().Equals(cl6))
}

func TestCompoundListInsertMiddle(t *testing.T) {
	assert := assert.New(t)

	cl := getTestSimpleList().toCompoundList()
	cl2 := cl.Insert(100, Int64(42))
	cl3 := cl2.Insert(200, Int64(43))
	cl4 := cl3.Insert(300, getTestSimpleList()...)
	cl5 := cl4.Insert(400, Int64(44), Int64(45))
	cl6 := cl5.Insert(500, getTestSimpleList()...)
	cl7 := cl6.Insert(600, Int64(100))

	expected := getTestSimpleList()
	assert.Equal(expected, testSimpleListFromNomsList(cl))
	assert.Equal(getTestSimpleListLen(), cl.Len())
	assert.True(expected.toCompoundList().Equals(cl))

	expected = expected.Insert(100, Int64(42))
	assert.Equal(expected, testSimpleListFromNomsList(cl2))
	assert.Equal(getTestSimpleListLen()+1, cl2.Len())
	assert.True(expected.toCompoundList().Equals(cl2))

	expected = expected.Insert(200, Int64(43))
	assert.Equal(expected, testSimpleListFromNomsList(cl3))
	assert.Equal(getTestSimpleListLen()+2, cl3.Len())
	assert.True(expected.toCompoundList().Equals(cl3))

	expected = expected.Insert(300, getTestSimpleList()...)
	assert.Equal(expected, testSimpleListFromNomsList(cl4))
	assert.Equal(2*getTestSimpleListLen()+2, cl4.Len())
	assert.True(expected.toCompoundList().Equals(cl4))

	expected = expected.Insert(400, Int64(44), Int64(45))
	assert.Equal(expected, testSimpleListFromNomsList(cl5))
	assert.Equal(2*getTestSimpleListLen()+4, cl5.Len())
	assert.True(expected.toCompoundList().Equals(cl5))

	expected = expected.Insert(500, getTestSimpleList()...)
	assert.Equal(expected, testSimpleListFromNomsList(cl6))
	assert.Equal(3*getTestSimpleListLen()+4, cl6.Len())
	assert.True(expected.toCompoundList().Equals(cl6))

	expected = expected.Insert(600, Int64(100))
	assert.Equal(expected, testSimpleListFromNomsList(cl7))
	assert.Equal(3*getTestSimpleListLen()+5, cl7.Len())
	assert.True(expected.toCompoundList().Equals(cl7))
}

func TestCompoundListInsertRanges(t *testing.T) {
	assert := assert.New(t)

	testList := getTestSimpleList()
	whole := testList.toCompoundList()

	// Compare list equality. Increment by 256 (16^2) because each iteration requires building a new list, which is slow.
	for incr, i := 256, 0; i < len(testList)-incr; i += incr {
		for window := 1; window <= incr; window *= 16 {
			testListPart := testList.Remove(i, i+window)
			actual := testListPart.toCompoundList().Insert(uint64(i), testList[i:i+window]...)
			assert.Equal(whole.Len(), actual.Len())
			assert.True(whole.Equals(actual))
		}
	}

	// Compare list length, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList); i += incr {
		assert.Equal(len(testList)+incr, int(whole.Insert(uint64(i), testList[0:incr]...).Len()))
	}
}

func TestCompoundListInsertTypeError(t *testing.T) {
	assert := assert.New(t)

	testList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(tr, testList...)
	assert.Panics(func() {
		cl.Insert(2, Bool(true))
	})
}

func TestCompoundListRemoveNothing(t *testing.T) {
	assert := assert.New(t)

	cl := getTestSimpleList().toCompoundList()

	assert.True(cl.Equals(cl.Remove(0, 0)))
	for i := uint64(1); i < getTestSimpleListLen(); i *= 2 {
		assert.True(cl.Equals(cl.Remove(i, i)))
	}
	assert.True(cl.Equals(cl.Remove(cl.Len()-1, cl.Len()-1)))
	assert.True(cl.Equals(cl.Remove(cl.Len(), cl.Len())))
}

func TestCompoundListRemoveEverything(t *testing.T) {
	assert := assert.New(t)

	cl := getTestSimpleList().toCompoundList().Remove(0, getTestSimpleListLen())

	assert.True(NewList().Equals(cl))
	assert.Equal(0, int(cl.Len()))
}

func TestCompoundListRemoveAtMiddle(t *testing.T) {
	assert := assert.New(t)

	cl := getTestSimpleList().toCompoundList()
	cl2 := cl.RemoveAt(100)
	cl3 := cl2.RemoveAt(200)

	expected := getTestSimpleList()
	assert.Equal(expected, testSimpleListFromNomsList(cl))
	assert.Equal(getTestSimpleListLen(), cl.Len())
	assert.True(expected.toCompoundList().Equals(cl))

	expected = expected.RemoveAt(100)
	assert.Equal(expected, testSimpleListFromNomsList(cl2))
	assert.Equal(getTestSimpleListLen()-1, cl2.Len())
	assert.True(expected.toCompoundList().Equals(cl2))

	expected = expected.RemoveAt(200)
	assert.Equal(expected, testSimpleListFromNomsList(cl3))
	assert.Equal(getTestSimpleListLen()-2, cl3.Len())
	assert.True(expected.toCompoundList().Equals(cl3))
}

func TestCompoundListRemoveRanges(t *testing.T) {
	assert := assert.New(t)

	testList := getTestSimpleList()
	whole := testList.toCompoundList()

	// Compare list equality. Increment by 256 (16^2) because each iteration requires building a new list, which is slow.
	for incr, i := 256, 0; i < len(testList)-incr; i += incr {
		for window := 1; window <= incr; window *= 16 {
			testListPart := testList.Remove(i, i+window)
			expected := testListPart.toCompoundList()
			actual := whole.Remove(uint64(i), uint64(i+window))
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
		}
	}

	// Compare list length, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList)-incr; i += incr {
		assert.Equal(len(testList)-incr, int(whole.Remove(uint64(i), uint64(i+incr)).Len()))
	}
}

func TestCompoundListSet(t *testing.T) {
	assert := assert.New(t)

	testList := getTestSimpleList()
	cl := testList.toCompoundList()

	testIdx := func(idx int, testEquality bool) {
		newVal := Int64(-1) // Test values are never < 0
		cl2 := cl.Set(uint64(idx), newVal)
		assert.False(cl.Equals(cl2))
		if testEquality {
			assert.True(testList.Set(idx, newVal).toCompoundList().Equals(cl2))
		}
	}

	// Compare list equality. Increment by 100 because each iteration requires building a new list, which is slow, but always test the last index.
	for incr, i := 100, 0; i < len(testList); i += incr {
		testIdx(i, true)
	}
	testIdx(len(testList)-1, true)

	// Compare list unequality, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList); i += incr {
		testIdx(i, false)
	}

	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl2 := NewTypedList(tr, testList...)
	assert.Panics(func() {
		cl2.Set(0, Bool(true))
	})
}

func TestCompoundListSlice(t *testing.T) {
	assert := assert.New(t)

	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	testList := getTestSimpleList()

	cl := NewTypedList(tr, testList...)
	empty := NewTypedList(tr)

	assert.True(cl.Equals(cl.Slice(0, cl.Len())))
	assert.True(cl.Equals(cl.Slice(0, cl.Len()+1)))
	assert.True(cl.Equals(cl.Slice(0, cl.Len()*2)))

	assert.True(empty.Equals(cl.Slice(0, 0)))
	assert.True(empty.Equals(cl.Slice(1, 1)))
	assert.True(empty.Equals(cl.Slice(cl.Len()/2, cl.Len()/2)))
	assert.True(empty.Equals(cl.Slice(cl.Len()-1, cl.Len()-1)))
	assert.True(empty.Equals(cl.Slice(cl.Len(), cl.Len())))
	assert.True(empty.Equals(cl.Slice(cl.Len(), cl.Len()+1)))
	assert.True(empty.Equals(cl.Slice(cl.Len(), cl.Len()*2)))

	cl2 := NewTypedList(tr, testList[0:1]...)
	cl3 := NewTypedList(tr, testList[1:2]...)
	cl4 := NewTypedList(tr, testList[len(testList)/2:len(testList)/2+1]...)
	cl5 := NewTypedList(tr, testList[len(testList)-2:len(testList)-1]...)
	cl6 := NewTypedList(tr, testList[len(testList)-1:]...)

	assert.True(cl2.Equals(cl.Slice(0, 1)))
	assert.True(cl3.Equals(cl.Slice(1, 2)))
	assert.True(cl4.Equals(cl.Slice(cl.Len()/2, cl.Len()/2+1)))
	assert.True(cl5.Equals(cl.Slice(cl.Len()-2, cl.Len()-1)))
	assert.True(cl6.Equals(cl.Slice(cl.Len()-1, cl.Len())))
	assert.True(cl6.Equals(cl.Slice(cl.Len()-1, cl.Len()+1)))
	assert.True(cl6.Equals(cl.Slice(cl.Len()-1, cl.Len()*2)))

	cl7 := NewTypedList(tr, testList[:len(testList)/2]...)
	cl8 := NewTypedList(tr, testList[len(testList)/2:]...)

	assert.True(cl7.Equals(cl.Slice(0, cl.Len()/2)))
	assert.True(cl8.Equals(cl.Slice(cl.Len()/2, cl.Len())))
	assert.True(cl8.Equals(cl.Slice(cl.Len()/2, cl.Len()+1)))
	assert.True(cl8.Equals(cl.Slice(cl.Len()/2, cl.Len()*2)))
}

func TestCompoundListFilter(t *testing.T) {
	assert := assert.New(t)

	simple := getTestSimpleList()
	filterCb := func(v Value, idx uint64) bool {
		return v.(Int64)%5 != 0
	}

	expected := testSimpleList{}
	for i, v := range simple {
		if filterCb(v, uint64(i)) {
			expected = append(expected, v)
		}

	}
	cl := simple.toCompoundList()

	res := cl.Filter(filterCb)
	assert.Equal(len(expected), int(res.Len()))
	res.IterAll(func(v Value, idx uint64) {
		assert.Equal(expected[idx], v)
	})
	assert.True(expected.toCompoundList().Equals(res))
}

func TestCompoundListFirstNNumbers(t *testing.T) {
	assert := assert.New(t)

	listType := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))

	firstNNumbers := func(n int) []Value {
		nums := []Value{}
		for i := 0; i < n; i++ {
			nums = append(nums, Int64(i))
		}

		return nums
	}

	nums := firstNNumbers(5000)
	s := NewTypedList(listType, nums...)
	assert.Equal(s.Ref().String(), "sha1-11e947e8aacfda8e9052bb57e661da442b26c625")
}

func TestCompoundListRefOfStructFirstNNumbers(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	structTypeDef := MakeStructType("num", []Field{
		Field{"n", MakePrimitiveType(Int64Kind), false},
	}, Choices{})
	pkg := NewPackage([]Type{structTypeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	structType := MakeType(pkgRef, 0)
	refOfTypeStructType := MakeCompoundType(RefKind, structType)
	listType := MakeCompoundType(ListKind, refOfTypeStructType)

	firstNNumbers := func(n int) []Value {
		nums := []Value{}
		for i := 0; i < n; i++ {
			r := vs.WriteValue(NewStruct(structType, structTypeDef, structData{"n": Int64(i)}))
			tr := newRef(r, refOfTypeStructType)
			nums = append(nums, tr)
		}

		return nums
	}

	nums := firstNNumbers(5000)
	s := NewTypedList(listType, nums...)
	assert.Equal(s.Ref().String(), "sha1-324e4faa5d80df9942627fe9848e0689261cbbc5")
}

func TestCompoundListModifyAfterRead(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	list := getTestSimpleList().toCompoundList()
	// Drop chunk values.
	list = vs.ReadValue(vs.WriteValue(list)).(compoundList)
	// Modify/query. Once upon a time this would crash.
	llen := list.Len()
	z := list.Get(0)
	list = list.RemoveAt(0).(compoundList)
	assert.Equal(llen-1, list.Len())
	list = list.Append(z).(compoundList)
	assert.Equal(llen, list.Len())
}
