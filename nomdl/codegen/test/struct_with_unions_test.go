package test

import (
	"testing"

	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
	"github.com/stretchr/testify/assert"
)

func TestStructWithUnions(t *testing.T) {
	assert := assert.New(t)

	def := gen.StructWithUnionsDef{}
	def.A = def.A.SetB(42)
	b, ok := def.A.B()
	assert.True(ok)
	assert.Equal(float64(42), b)
	def.D = def.D.SetF("hi")
	f, ok := def.D.F()
	assert.True(ok)
	assert.Equal("hi", f)

	st := def.New()
	b, ok = st.A().B()
	assert.True(ok)
	assert.Equal(float64(42), b)
	f, ok = st.D().F()
	assert.True(ok)
	assert.Equal("hi", f)

	def2 := st.Def()
	assert.Equal(def, def2)

	st2 := gen.NewStructWithUnions()
	st2 = st2.SetA(st2.A().SetB(42)).SetD(st2.D().SetF("hi"))
	assert.True(st.Equals(st2))

	def3 := st2.Def()
	assert.Equal(def, def3)
}
