package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	dep "github.com/attic-labs/noms/nomdl/codegen/test/gen/sha1_afe4d81896c95439605cca7c1b4cb6ab9ba7cfd3"
	leaf "github.com/attic-labs/noms/nomdl/codegen/test/gen/sha1_c3ae18e55c636f0c84ab1adab44e57e9bfb86723"
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

func TestListOfImportsDef(t *testing.T) {
	assert := assert.New(t)
	lDef := ListOfsha1_afe4d81896c95439605cca7c1b4cb6ab9ba7cfd3_DDef{
		dep.DDef{EnumField: leaf.E3},
		dep.DDef{EnumField: leaf.E2},
		dep.DDef{EnumField: leaf.E1},
	}

	l := lDef.New()
	assert.EqualValues(3, l.Len())
	assert.EqualValues(leaf.E3, l.Get(0).EnumField())
	assert.EqualValues(leaf.E2, l.Get(1).EnumField())
	assert.EqualValues(leaf.E1, l.Get(2).EnumField())
}
