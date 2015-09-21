package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/types"
)

func TestRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	l := ListOfStringDef{"a", "b", "c"}.New()
	l2 := ListOfStringDef{"d", "e", "f"}.New()
	lRef := l.Ref()
	r := NewRefOfListOfString(lRef)

	v := types.ReadValue(l.Ref(), cs)
	assert.Nil(v)

	assert.Panics(func() { r.GetValue(cs) })

	r2 := r.SetValue(l, cs)
	assert.True(r.Equals(r2))
	v2 := r2.GetValue(cs)
	v3 := r.GetValue(cs)
	assert.True(v2.Equals(v3))

	r3 := r2.SetValue(l2, cs)
	assert.False(r.Equals(r3))
}

func TestRefFromValAndNomsValue(t *testing.T) {
	assert := assert.New(t)

	l := ListOfStringDef{"a", "b", "c"}.New()
	rv := types.Ref{R: l.Ref()}
	r := RefOfListOfStringFromVal(rv)
	r2 := NewRefOfListOfString(l.Ref())
	assert.True(r.Equals(r2))

	rv2 := r.NomsValue()
	assert.True(rv.Equals(rv2))
}

func TestListOfRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := types.Float32(0)
	ra := a.Ref()

	l := NewListOfRefOfFloat32()
	r := NewRefOfFloat32(ra)
	l = l.Append(r)
	r2 := l.Get(0)
	assert.True(r.Equals(r2))

	l = l.Set(0, r.SetValue(1, cs))
	r3 := l.Get(0)
	assert.False(r.Equals(r3))
	assert.Panics(func() { r.GetValue(cs) })
}

func TestStructWithRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	set := SetOfFloat32Def{0: true, 1: true, 2: true}.New()
	str := StructWithRefDef{
		R: set.Ref(),
	}.New()

	r := str.R()
	r2 := NewRefOfSetOfFloat32(set.Ref())
	assert.True(r.Equals(r2))

	assert.Panics(func() { r2.GetValue(cs) })

	types.WriteValue(str.NomsValue(), cs)
	assert.Panics(func() { r2.GetValue(cs) })

	types.WriteValue(set.NomsValue(), cs)
	set2 := r2.GetValue(cs)
	assert.True(set.Equals(set2))
}
