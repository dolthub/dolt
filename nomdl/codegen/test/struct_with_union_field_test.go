package test

import (
	"testing"

	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestStructWithUnionField(t *testing.T) {
	assert := assert.New(t)

	def := gen.StructWithUnionFieldDef{
		A: float32(1),
	}
	def = def.SetC("s")
	_, ok := def.B()
	assert.False(ok)
	c, ok := def.C()
	assert.True(ok)
	assert.Equal("s", c)

	st := def.New()
	_, ok = st.F()
	assert.False(ok)
	c, ok = st.C()
	assert.True(ok)
	assert.Equal("s", c)

	st2 := gen.NewStructWithUnionField().SetA(1).SetC("s")
	assert.True(st.Equals(st2))

	st3 := gen.NewStructWithUnionField().SetC("s").SetA(1)
	assert.True(st.Equals(st3))

	def2 := st3.Def()
	assert.Equal(def, def2)
}

func TestStructWithUnionFieldListPart(t *testing.T) {
	assert := assert.New(t)

	st := gen.NewStructWithUnionField().SetF(gen.SetOfUint8Def{2: true, 4: true}.New())
	f, ok := st.F()
	assert.True(ok)
	assert.True(f.Has(2))
	assert.False(f.Has(1))

	st2 := st.SetF(f)
	assert.True(st.Equals(st2))

	st3 := st.SetD(types.NewEmptyBlob())
	assert.False(st.Equals(st3))
	_, ok = st3.F()
	assert.False(ok)
}
