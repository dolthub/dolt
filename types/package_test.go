package types

import (
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	assert := assert.New(t)

	st := NewPackage()
	typ := st.TypeRef()
	name := "Package"
	assert.EqualValues(name, typ.Name())
	assert.Equal(TypeRefKind, typ.Kind())
	assert.Equal(__typesPackageInFile_package_CachedRef, typ.PackageRef())

	typ = LookupPackage(__typesPackageInFile_package_CachedRef).NamedTypes().Get(name)
	desc := typ.Desc.ToValue().(Map)
	fields := desc.Get(NewString("fields")).(List)
	choices := desc.Get(NewString("choices")).(List)

	assert.EqualValues(NewList(), choices)
	assert.EqualValues(6, fields.Len())
	find := func(s string) TypeRef {
		for i := uint64(0); i < fields.Len(); i++ {
			if i%3 == 0 {
				f := fields.Get(i).(String)
				if f.Equals(NewString(s)) {
					return fields.Get(i + 1).(TypeRef)
				}
			}
		}
		assert.Fail("Did not find desired field", "Field name: %s", s)
		return TypeRef{ref: &ref.Ref{}}
	}
	tr := find("Dependencies")
	if assert.Equal(SetKind, tr.Kind()) {
		elemType := tr.Desc.ToValue().(TypeRef)
		if assert.Equal(RefKind, elemType.Kind()) {
			assert.EqualValues(name, elemType.Desc.ToValue().(TypeRef).Name())
		}
	}
	tr = find("NamedTypes")
	if assert.Equal(MapKind, tr.Kind()) {
		desc := tr.Desc.ToValue().(List)
		assert.Equal(StringKind, desc.Get(0).(TypeRef).Kind())
		assert.Equal(TypeRefKind, desc.Get(1).(TypeRef).Kind())
	}
}
