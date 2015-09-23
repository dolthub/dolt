package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	assert := assert.New(t)

	st := NewPackage()
	typ := st.Type()
	name := NewString("Package")
	assert.EqualValues(name, typ.Name())
	assert.Equal(StructKind, typ.Kind())
	fields, choices := typ.StructDesc()
	assert.Nil(choices)
	assert.EqualValues(4, fields.Len())
	find := func(s string) TypeRef {
		for i := uint64(0); i < fields.Len(); i++ {
			if i%2 == 0 {
				f := fields.Get(i).(String)
				if f.Equals(NewString(s)) {
					return fields.Get(i + 1).(TypeRef)
				}
			}
		}
		return TypeRef{}
	}
	tr := find("Dependencies")
	if assert.Equal(SetKind, tr.Kind()) {
		elemType := tr.ElemDesc()
		if assert.Equal(RefKind, elemType.Kind()) {
			assert.EqualValues(name, elemType.ElemDesc().Name())
		}
	}
	tr = find("Types")
	if assert.Equal(MapKind, tr.Kind()) {
		keyType, valueType := tr.MapElemDesc()
		assert.Equal(StringKind, keyType.Kind())
		assert.Equal(TypeRefKind, valueType.Kind())
	}
}
