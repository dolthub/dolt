package types

import (
	"io/ioutil"
	"os"
	"regexp"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

func getFakeCompoundList(datas ...string) compoundList {
	futures := make([]Future, len(datas))
	offsets := make([]uint64, len(datas))
	length := uint64(0)
	for i, s := range datas {
		l := NewList()
		for _, r := range s {
			l = l.Append(NewString(string(r)))
		}
		futures[i] = futureFromValue(l)
		length += l.Len()
		offsets[i] = length
	}
	return newCompoundList(offsets, futures, nil)
}

func getTestCompoundList(t *testing.T) List {
	assert := assert.New(t)

	l1 := NewList()
	for i := 0; i < 0xff; i++ {
		l1 = l1.Append(UInt8(i))
	}
	cl, ok := l1.(compoundList)
	assert.True(ok)
	return cl
}

func getWordsInAlice(t *testing.T) []Value {
	assert := assert.New(t)
	f, err := os.Open("alice-short.txt")
	assert.NoError(err)
	defer f.Close()

	bs, err := ioutil.ReadAll(f)
	assert.NoError(err)
	re := regexp.MustCompile(`(?:\w|\d)+`)
	fields := re.FindAllString(string(bs), -1)
	vs := make([]Value, len(fields), len(fields))
	for i, s := range fields {
		vs[i] = NewString(s)
	}
	return vs
}

func getAliceList(t *testing.T) compoundList {
	return newCompoundListFromValues(getWordsInAlice(t), nil).(compoundList)
}

func TestCompoundListLen(t *testing.T) {
	assert := assert.New(t)
	cl := getFakeCompoundList("hi", "bye")
	assert.Equal(uint64(5), cl.Len())
	assert.False(cl.Empty())

	al := getAliceList(t)
	assert.Equal(uint64(5747), al.Len())
}

func TestCompoundListChunks(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.MemoryStore{}

	cl := getFakeCompoundList("hi", "bye")
	assert.Equal(0, len(cl.Chunks()))

	ll1 := NewList(NewString("h"), NewString("i"))
	llr1 := ll1.Ref()
	ll2 := NewList(NewString("b"), NewString("y"), NewString("e"))
	cl = newCompoundList([]uint64{2, 5}, []Future{futureFromRef(llr1), futureFromValue(ll2)}, cs)
	assert.Equal(1, len(cl.Chunks()))
}

func TestCompoundListGet(t *testing.T) {
	assert := assert.New(t)
	cl := getFakeCompoundList("hi", "bye")

	for i, r := range "hibye" {
		assert.True(NewString(string(r)).Equals(cl.Get(uint64(i))))
	}

	assert.Panics(func() {
		cl.Get(5)
	})
}

func TestCompoundListReadWriteValue(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.MemoryStore{}
	cl := getFakeCompoundList("hi", "bye")
	r := WriteValue(cl, cs)
	v := ReadValue(r, cs)
	assert.True(v.Equals(cl))
}

func TestnewCompoundListFromValues(t *testing.T) {
	assert := assert.New(t)

	vs := newCompoundListFromValues([]Value{}, nil)
	assert.Equal(uint64(0), vs.Len())

	vs = newCompoundListFromValues([]Value{NewString("a")}, nil)
	assert.Equal(uint64(1), vs.Len())

	vs = newCompoundListFromValues([]Value{NewString("h"), NewString("i")}, nil)
	assert.Equal(uint64(2), vs.Len())
}

func TestCompoundListAppend(t *testing.T) {
	assert := assert.New(t)

	var l List = getFakeCompoundList("hi", "bye")
	l2 := l.Append(NewString("x"), NewString("y"), NewString("z"))

	assert.False(l.Equals(l2))
	assert.Equal(uint64(5), l.Len())
	assert.Equal(uint64(8), l2.Len())

	for i, r := range "hibyexyz" {
		assert.True(NewString(string(r)).Equals(l2.Get(uint64(i))))
	}

	cl2, ok := l2.(compoundList)
	assert.True(ok)
	assert.Equal(2, len(cl2.futures))

	// It should not matter how the list was made
	words := getWordsInAlice(t)
	al1 := newCompoundListFromValues(words, nil)
	al2 := newCompoundListFromValues(words[0:len(words)/2], nil)
	al2 = al2.Append(words[len(words)/2:]...)
	assert.True(al1.Equals(al2))

	al3 := NewList()
	for _, w := range words {
		al3 = al3.Append(w)
	}
	assert.Equal(int(al1.Len()), int(al3.Len()))
	assert.True(al1.Equals(al3))
}

func TestCompoundListSlice(t *testing.T) {
	assert := assert.New(t)

	l1 := getTestCompoundList(t)

	l2 := l1.Slice(0, l1.Len())
	assert.True(l1.Equals(l2))

	l3 := l1.Slice(1, l1.Len()-1)
	assert.Equal(l1.Len()-2, l3.Len())

	assert.Panics(func() {
		l1.Slice(0, l1.Len()+1)
	})
	assert.Panics(func() {
		l1.Slice(l1.Len()+1, l1.Len()+2)
	})
}

func TestCompoundListSet(t *testing.T) {
	assert := assert.New(t)

	l1 := getTestCompoundList(t)

	l2 := l1.Set(0, Int32(1))
	assert.False(l1.Equals(l2))
	assert.True(Int32(1).Equals(l2.Get(0)))

	l3 := l2.Set(0, l1.Get(0))
	assert.True(l1.Equals(l3))

	l4 := l3.Set(l1.Len()-1, Bool(true))
	assert.True(Bool(true).Equals(l4.Get(l1.Len() - 1)))
}

func TestCompoundListInsert(t *testing.T) {
	assert := assert.New(t)

	l1 := getTestCompoundList(t)

	assert.Panics(func() {
		l1.Insert(l1.Len()+1, Int32(0))
	})

	l2 := l1.Insert(l1.Len(), Int32(0), Int32(1), Int32(3))
	l3 := l1.Append(Int32(0), Int32(1), Int32(3))
	assert.False(l2.Equals(l1))
	assert.False(l3.Equals(l1))
	assert.True(l2.Equals(l3))

	l4 := l2.Insert(l1.Len(), Int32(-1))
	l5 := l1.Append(Int32(-1), Int32(0), Int32(1), Int32(3))
	assert.True(l4.Equals(l5))
}

func TestCompoundListRemove(t *testing.T) {
	assert := assert.New(t)

	l1 := getTestCompoundList(t)

	assert.Panics(func() {
		l1.Remove(l1.Len()-1, l1.Len()+1)
	})
	assert.Panics(func() {
		l1.Remove(l1.Len()+1, l1.Len()+1)
	})

	l2 := l1.Remove(uint64(1), uint64(1))
	assert.True(l1.Equals(l2))

	l3 := l1.Remove(uint64(1), uint64(3))
	assert.True(UInt8(0).Equals(l3.Get(0)))
	assert.True(UInt8(3).Equals(l3.Get(1)))
	assert.True(UInt8(4).Equals(l3.Get(2)))

	l4 := l3.RemoveAt(2)
	assert.True(UInt8(0).Equals(l4.Get(0)))
	assert.True(UInt8(3).Equals(l4.Get(1)))
	assert.True(UInt8(5).Equals(l4.Get(2)))
}
