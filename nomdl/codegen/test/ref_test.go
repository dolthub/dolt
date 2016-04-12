package test

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestListOfRef(t *testing.T) {
	assert := assert.New(t)
	a := types.Float32(0)
	ra := a.Ref()

	l := gen.NewListOfRefOfFloat32()
	r := gen.NewRefOfFloat32(ra)
	l = l.Append(r)
	r2 := l.Get(0)
	assert.True(r.Equals(r2))

	def := l.Def()
	assert.EqualValues(ra, def[0])
}

func TestStructWithRef(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDataStore(chunks.NewMemoryStore())

	set := gen.SetOfFloat32Def{0: true, 1: true, 2: true}.New()
	ds.WriteValue(set)

	str := gen.StructWithRefDef{
		R: set.Ref(),
	}.New()
	ds.WriteValue(str)

	r := str.R()
	r2 := gen.NewRefOfSetOfFloat32(set.Ref())
	assert.True(r.Equals(r2))
	assert.True(r2.TargetValue(ds).Equals(set))

	set2 := r2.TargetValue(ds)
	assert.True(set.Equals(set2))

	def := str.Def()
	assert.EqualValues(set.Ref(), def.R)
}

func TestListOfRefChunks(t *testing.T) {
	assert := assert.New(t)

	a := types.Float32(0)
	ra := a.Ref()

	l := gen.NewListOfRefOfFloat32()
	r := gen.NewRefOfFloat32(ra)

	assert.Len(l.Chunks(), 0)

	l2 := l.Append(r)
	assert.Len(l2.Chunks(), 1)
}

func TestStructWithRefChunks(t *testing.T) {
	assert := assert.New(t)

	set := gen.SetOfFloat32Def{0: true}.New()
	str := gen.StructWithRefDef{
		R: set.Ref(),
	}.New()

	// 1 for the Type and 1 for the ref in the R field.
	assert.Len(str.Chunks(), 2)
}
