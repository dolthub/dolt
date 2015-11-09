package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
	"github.com/attic-labs/noms/types"
)

func TestWithImportsDef(t *testing.T) {
	assert := assert.New(t)

	def := gen.ImportUserDef{
		ImportedStruct: gen.DDef{
			StructField: gen.SDef{S: "hi", B: true},
			EnumField:   gen.E2,
		},
		Enum: gen.LocalE1,
	}
	st := def.New()

	def2 := st.Def()
	st2 := def.New()

	assert.Equal(def, def2)
	assert.True(st.Equals(st2))

	ds := gen.NewD().SetStructField(gen.NewS().SetS("hi").SetB(true)).SetEnumField(gen.E2)
	st3 := gen.NewImportUser()
	st3 = st3.SetImportedStruct(ds).SetEnum(gen.LocalE1)
	assert.True(st.Equals(st3))
	ddef := st3.ImportedStruct().Def()
	assert.Equal("hi", ddef.StructField.S)
	assert.Equal(true, ddef.StructField.B)
	assert.Equal(gen.E2, ddef.EnumField)
}

func TestListOfImportsDef(t *testing.T) {
	assert := assert.New(t)
	lDef := gen.ListOfDDef{
		gen.DDef{EnumField: gen.E3},
		gen.DDef{EnumField: gen.E2},
		gen.DDef{EnumField: gen.E1},
	}

	l := lDef.New()
	assert.EqualValues(3, l.Len())
	assert.EqualValues(gen.E3, l.Get(0).EnumField())
	assert.EqualValues(gen.E2, l.Get(1).EnumField())
	assert.EqualValues(gen.E1, l.Get(2).EnumField())
}

func TestDepsAndPackageRefs(t *testing.T) {
	assert := assert.New(t)
	tr := gen.NewImportUser().ImportedStruct().Type()
	assert.Equal(types.UnresolvedKind, tr.Kind())
	assert.True(tr.HasPackageRef())
	p := types.LookupPackage(tr.PackageRef())
	assert.NotNil(p)
	assert.IsType(types.Package{}, *p)
}
