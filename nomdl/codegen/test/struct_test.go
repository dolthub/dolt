package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/types"
)

func TestDef(t *testing.T) {
	assert := assert.New(t)

	def := StructDef{"hi", true}
	st := def.New()

	def2 := st.Def()
	st2 := def.New()

	assert.Equal(def, def2)
	assert.True(st.Equals(st2))

	st3 := NewStruct()
	st3 = st3.SetS("hi").SetB(true)
	assert.Equal("hi", st3.S())
	assert.Equal(true, st3.B())
}

func TestValue(t *testing.T) {
	assert := assert.New(t)

	def := StructDef{"hi", true}
	st := def.New()
	val := st.NomsValue()
	st2 := StructFromVal(val)
	assert.True(st.Equals(st2))
}

func TestType(t *testing.T) {
	assert := assert.New(t)

	def := StructDef{"hi", true}
	st := def.New()
	typ := st.Type()
	assert.EqualValues(types.NewString("Struct"), typ.Name())
	assert.EqualValues(__testPackageInFile_struct_Ref(), typ.PackageRef())
}
