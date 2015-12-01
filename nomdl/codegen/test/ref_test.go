package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
	"github.com/attic-labs/noms/types"
)

func TestRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	l := gen.ListOfStringDef{"a", "b", "c"}.New(cs)
	l2 := gen.ListOfStringDef{"d", "e", "f"}.New(cs)
	lRef := l.Ref()
	r := gen.NewRefOfListOfString(lRef)

	v := types.ReadValue(l.Ref(), cs)
	assert.Nil(v)

	assert.Panics(func() { r.TargetValue(cs) })

	r2 := r.SetTargetValue(l, cs)
	assert.True(r.Equals(r2))
	v2 := r2.TargetValue(cs)
	v3 := r.TargetValue(cs)
	assert.True(v2.Equals(v3))

	r3 := r2.SetTargetValue(l2, cs)
	assert.False(r.Equals(r3))
}

func TestListOfRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := types.Float32(0)
	ra := a.Ref()

	l := gen.NewListOfRefOfFloat32(cs)
	r := gen.NewRefOfFloat32(ra)
	l = l.Append(r)
	r2 := l.Get(0)
	assert.True(r.Equals(r2))

	def := l.Def()
	assert.EqualValues(ra, def[0])

	l = l.Set(0, r.SetTargetValue(1, cs))
	r3 := l.Get(0)
	assert.False(r.Equals(r3))
	assert.Panics(func() { r.TargetValue(cs) })
}

func TestStructWithRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	set := gen.SetOfFloat32Def{0: true, 1: true, 2: true}.New(cs)
	str := gen.StructWithRefDef{
		R: set.Ref(),
	}.New(cs)

	r := str.R()
	r2 := gen.NewRefOfSetOfFloat32(set.Ref())
	assert.True(r.Equals(r2))

	assert.Panics(func() { r2.TargetValue(cs) })

	types.WriteValue(str, cs)
	assert.Panics(func() { r2.TargetValue(cs) })

	types.WriteValue(set, cs)
	set2 := r2.TargetValue(cs)
	assert.True(set.Equals(set2))

	def := str.Def()
	assert.EqualValues(set.Ref(), def.R)
}

func TestListOfRefChunks(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := types.Float32(0)
	ra := a.Ref()

	l := gen.NewListOfRefOfFloat32(cs)
	r := gen.NewRefOfFloat32(ra)

	assert.Len(l.Chunks(), 0)

	l2 := l.Append(r)
	assert.Len(l2.Chunks(), 1)
}

func TestStructWithRefChunks(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	set := gen.SetOfFloat32Def{0: true}.New(cs)
	str := gen.StructWithRefDef{
		R: set.Ref(),
	}.New(cs)

	// 1 for the Type and 1 for the ref in the R field.
	assert.Len(str.Chunks(), 2)
}
