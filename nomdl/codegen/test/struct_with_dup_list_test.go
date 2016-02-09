package test

import (
	"testing"

	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
	"github.com/stretchr/testify/assert"
)

func TestStructWithDupList(t *testing.T) {
	assert := assert.New(t)

	def := gen.StructWithListDef{
		L: gen.ListOfUint8Def{0, 1, 2},
		B: true,
		S: "world",
		I: 42,
	}

	st := def.New()
	l := st.L()
	assert.Equal(uint64(3), l.Len())

	dupList := gen.NewStructWithDupList().SetL(st.L())

	assert.EqualValues(st.L(), dupList.L())
}
