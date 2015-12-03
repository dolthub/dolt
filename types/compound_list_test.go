package types

import (
	"math/rand"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

type testSimpleList []Value

func (tsl testSimpleList) Get(idx uint64) Value {
	return tsl[idx]
}

func getTestSimpleListLen() int {
	return int(listPattern * 50)
}

func getTestSimpleList() testSimpleList {
	length := getTestSimpleListLen()
	s := rand.NewSource(42)
	values := make([]Value, length)
	for i := 0; i < length; i++ {
		values[i] = Int64(s.Int63() & 0xff)
	}

	return values
}

func TestCompoundListGet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(cs, tr, simpleList...)

	for i, v := range simpleList {
		assert.Equal(v, cl.Get(uint64(i)))
	}
}

func TestCompoundListIter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(cs, tr, simpleList...)

	expectIdx := uint64(0)
	endAt := uint64(listPattern)
	cl.Iter(func(v Value, idx uint64) bool {
		assert.Equal(expectIdx, idx)
		expectIdx += 1
		assert.Equal(simpleList.Get(idx), v)
		if expectIdx == endAt {
			return true
		}
		return false
	})

	assert.Equal(endAt, expectIdx)
}

func TestCompoundListIterAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(cs, tr, simpleList...)

	expectIdx := uint64(0)
	cl.IterAll(func(v Value, idx uint64) {
		assert.Equal(expectIdx, idx)
		expectIdx += 1
		assert.Equal(simpleList.Get(idx), v)
	})
}

func TestCompoundListCurAt(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	listLen := func(at int, next func(*metaSequenceCursor) bool) (size int) {
		cs := chunks.NewMemoryStore()
		tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
		cl := NewTypedList(cs, tr, getTestSimpleList()...).(compoundList)
		cur, _, _ := cl.cursorAt(uint64(at))
		for {
			size += int(ReadValue(cur.currentRef(), cs).(List).Len())
			if !next(cur) {
				return
			}
		}
		panic("not reachable")
	}

	assert.Equal(getTestSimpleListLen(), listLen(0, func(cur *metaSequenceCursor) bool {
		return cur.advance()
	}))
	assert.Equal(getTestSimpleListLen(), listLen(getTestSimpleListLen(), func(cur *metaSequenceCursor) bool {
		return cur.retreat()
	}))
}

func TestCompoundListAppend(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
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
	assert.Equal(getTestSimpleListLen(), int(cl.Len()))
	assert.True(newCompoundList(expected).Equals(cl))

	expected = append(expected, Int64(42))
	assert.Equal(expected, compoundToSimple(cl2))
	assert.Equal(getTestSimpleListLen()+1, int(cl2.Len()))
	assert.True(newCompoundList(expected).Equals(cl2))

	expected = append(expected, Int64(43))
	assert.Equal(expected, compoundToSimple(cl3))
	assert.Equal(getTestSimpleListLen()+2, int(cl3.Len()))
	assert.True(newCompoundList(expected).Equals(cl3))

	expected = append(expected, getTestSimpleList()...)
	assert.Equal(expected, compoundToSimple(cl4))
	assert.Equal(2*getTestSimpleListLen()+2, int(cl4.Len()))
	assert.True(newCompoundList(expected).Equals(cl4))

	expected = append(expected, Int64(44), Int64(45))
	assert.Equal(expected, compoundToSimple(cl5))
	assert.Equal(2*getTestSimpleListLen()+4, int(cl5.Len()))
	assert.True(newCompoundList(expected).Equals(cl5))

	expected = append(expected, getTestSimpleList()...)
	assert.Equal(expected, compoundToSimple(cl6))
	assert.Equal(3*getTestSimpleListLen()+4, int(cl6.Len()))
	assert.True(newCompoundList(expected).Equals(cl6))
}
