package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
)

func TestStructWithUnions(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	def := gen.StructWithUnionsDef{}
	def.A = def.A.SetB(cs, 42)
	b, ok := def.A.B()
	assert.True(ok)
	assert.Equal(float64(42), b)
	def.D = def.D.SetF(cs, "hi")
	f, ok := def.D.F()
	assert.True(ok)
	assert.Equal("hi", f)

	st := def.New(cs)
	b, ok = st.A().B()
	assert.True(ok)
	assert.Equal(float64(42), b)
	f, ok = st.D().F()
	assert.True(ok)
	assert.Equal("hi", f)

	def2 := st.Def()
	assert.Equal(def, def2)

	st2 := gen.NewStructWithUnions(cs)
	st2 = st2.SetA(st2.A().SetB(42)).SetD(st2.D().SetF("hi"))
	assert.True(st.Equals(st2))

	def3 := st2.Def()
	assert.Equal(def, def3)
}
