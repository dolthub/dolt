package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
)

func TestType(t *testing.T) {
	assert := assert.New(t)

	st := NewPackage()
	typ := st.TypeRef()
	ordinal := int16(0)
	assert.Equal(ordinal, typ.Ordinal())
	assert.Equal(TypeRefKind, typ.Kind())
	assert.Equal(__typesPackageInFile_package_CachedRef, typ.PackageRef())

	typ = LookupPackage(__typesPackageInFile_package_CachedRef).Types().Get(0)
	desc := typ.Desc.ToValue().(Map)
	fields := desc.Get(NewString("fields")).(List)
	choices := desc.Get(NewString("choices")).(List)

	assert.EqualValues(NewList(), choices)
	assert.EqualValues(6, fields.Len())
	find := func(s string) TypeRef {
		for i := uint64(0); i < fields.Len(); i += 3 {
			f := fields.Get(i).(String)
			if f.Equals(NewString(s)) {
				return fields.Get(i + 1).(TypeRef)
			}
		}
		assert.Fail("Did not find desired field", "Field name: %s", s)
		return TypeRef{ref: &ref.Ref{}}
	}
	tr := find("Dependencies")
	if assert.Equal(SetKind, tr.Kind()) {
		elemType := tr.Desc.ToValue().(TypeRef)
		if assert.Equal(RefKind, elemType.Kind()) {
			assert.Equal(ordinal, elemType.Desc.ToValue().(TypeRef).Ordinal())
		}
	}
	tr = find("Types")
	if assert.Equal(ListKind, tr.Kind()) {
		typeRef := tr.Desc.ToValue().(TypeRef)
		assert.Equal(TypeRefKind, typeRef.Kind())
	}
}
