package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
)

func TestStructWithDupList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	def := gen.StructWithListDef{
		L: gen.ListOfUint8Def{0, 1, 2},
		B: true,
		S: "world",
		I: 42,
	}

	st := def.New(cs)
	l := st.L()
	assert.Equal(uint64(3), l.Len())

	dupList := gen.NewStructWithDupList(cs).SetL(st.L())

	assert.EqualValues(st.L(), dupList.L())
}
