package types

import (
	"math/rand"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

type testSimpleList []Value

func getTestSimpleListLen() uint64 {
	return uint64(listPattern) * 200
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

func TestCompoundListGet(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(cs, tr, simpleList...)

	// Incrementing by len(simpleList)/10 because Get() is too slow to run on every index.
	for i := 0; i < len(simpleList); i += len(simpleList) / 10 {
		assert.Equal(simpleList[i], cl.Get(uint64(i)))
	}
}

func TestCompoundListIter(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(cs, tr, simpleList...)

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

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(cs, tr, simpleList...)

	expectIdx := uint64(0)
	cl.IterAll(func(v Value, idx uint64) {
		assert.Equal(expectIdx, idx)
		expectIdx += 1
		assert.Equal(simpleList[idx], v)
	})

	assert.Equal(getTestSimpleListLen(), expectIdx)
}

func TestCompoundListLen(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))

	cl := NewTypedList(cs, tr, getTestSimpleList()...).(compoundList)
	assert.Equal(getTestSimpleListLen(), cl.Len())
	cl = NewTypedList(cs, tr, append(getTestSimpleList(), getTestSimpleList()...)...).(compoundList)
	assert.Equal(getTestSimpleListLen()*2, cl.Len())
}

func TestCompoundListCursorAt(t *testing.T) {
	assert := assert.New(t)

	listLen := func(at uint64, next func(*sequenceCursor) bool) (size uint64) {
		cs := chunks.NewMemoryStore()
		tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
		cl := NewTypedList(cs, tr, getTestSimpleList()...).(compoundList)
		cur, _, _ := cl.cursorAt(at)
		for {
			size += readMetaTupleValue(cur.current(), cs).(List).Len()
			if !next(cur) {
				return
			}
		}
		panic("not reachable")
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
		cs := chunks.NewMemoryStore()
		tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
		return NewTypedList(cs, tr, items...).(compoundList)
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

func TestCompoundListSlice(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	testList := getTestSimpleList()

	cl := NewTypedList(cs, tr, testList...)
	empty := NewTypedList(cs, tr)

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

	cl2 := NewTypedList(cs, tr, testList[0:1]...)
	cl3 := NewTypedList(cs, tr, testList[1:2]...)
	cl4 := NewTypedList(cs, tr, testList[len(testList)/2:len(testList)/2+1]...)
	cl5 := NewTypedList(cs, tr, testList[len(testList)-2:len(testList)-1]...)
	cl6 := NewTypedList(cs, tr, testList[len(testList)-1:]...)

	assert.True(cl2.Equals(cl.Slice(0, 1)))
	assert.True(cl3.Equals(cl.Slice(1, 2)))
	assert.True(cl4.Equals(cl.Slice(cl.Len()/2, cl.Len()/2+1)))
	assert.True(cl5.Equals(cl.Slice(cl.Len()-2, cl.Len()-1)))
	assert.True(cl6.Equals(cl.Slice(cl.Len()-1, cl.Len())))
	assert.True(cl6.Equals(cl.Slice(cl.Len()-1, cl.Len()+1)))
	assert.True(cl6.Equals(cl.Slice(cl.Len()-1, cl.Len()*2)))

	cl7 := NewTypedList(cs, tr, testList[:len(testList)/2]...)
	cl8 := NewTypedList(cs, tr, testList[len(testList)/2:]...)

	assert.True(cl7.Equals(cl.Slice(0, cl.Len()/2)))
	assert.True(cl8.Equals(cl.Slice(cl.Len()/2, cl.Len())))
	assert.True(cl8.Equals(cl.Slice(cl.Len()/2, cl.Len()+1)))
	assert.True(cl8.Equals(cl.Slice(cl.Len()/2, cl.Len()*2)))
}
