package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	leaf "github.com/attic-labs/noms/nomdl/codegen/test/gen/sha1_f1a192312c01fb47e8e329471242e475eb7001a4"
	dep "github.com/attic-labs/noms/nomdl/codegen/test/gen/sha1_f9397427926127f67d8f3edb21c92bf642262e9b"
)

func TestWithImportsDef(t *testing.T) {
	assert := assert.New(t)

	def := ImportUserDef{
		ImportedStruct: dep.DDef{
			StructField: leaf.SDef{S: "hi", B: true},
			EnumField:   leaf.E2,
		},
		Enum: E1,
	}
	st := def.New()

	def2 := st.Def()
	st2 := def.New()

	assert.Equal(def, def2)
	assert.True(st.Equals(st2))

	ds := dep.NewD().SetStructField(leaf.NewS().SetS("hi").SetB(true)).SetEnumField(leaf.E2)
	st3 := NewImportUser()
	st3 = st3.SetImportedStruct(ds).SetEnum(E1)
	assert.True(st.Equals(st3))
	ddef := st3.ImportedStruct().Def()
	assert.Equal("hi", ddef.StructField.S)
	assert.Equal(true, ddef.StructField.B)
	assert.Equal(leaf.E2, ddef.EnumField)
}
