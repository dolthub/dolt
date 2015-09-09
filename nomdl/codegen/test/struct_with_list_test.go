package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func TestStructWithList(t *testing.T) {
	assert := assert.New(t)

	def := StructWithListDef{
		L: ListOfUInt8Def{0, 1, 2},
		B: true,
		S: "world",
		I: 42,
	}

	st := def.New()
	l := st.L()
	assert.Equal(uint64(3), l.Len())

	def2 := st.Def()
	assert.Equal(def, def2)

	def2.L[2] = 22
	st2 := def2.New()
	assert.Equal(uint8(22), st2.L().Get(2))
}
