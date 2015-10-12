package code

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type testResolver struct {
	assert *assert.Assertions
	deps   map[ref.Ref]types.Package
}

func (res *testResolver) Resolve(t types.TypeRef) types.TypeRef {
	if !t.IsUnresolved() {
		return t
	}

	dep, ok := res.deps[t.PackageRef()]
	res.assert.True(ok, "Package %s is referenced in %+v, but is not a dependency.", t.PackageRef().String(), t)
	depTypes := dep.NamedTypes()
	res.assert.True(depTypes.Has(t.Name()), "Cannot import type %s from package %s.", t.Name(), t.PackageRef().String())
	return depTypes.Get(t.Name()).MakeImported(t.PackageRef())
}

func TestUserName(t *testing.T) {
	assert := assert.New(t)

	imported := types.PackageDef{
		NamedTypes: types.MapOfStringToTypeRefDef{
			"E1": types.MakeEnumTypeRef("E1", "a", "b"),
			"S1": types.MakeStructTypeRef("S1", []types.Field{
				types.Field{"f", types.MakePrimitiveTypeRef(types.BoolKind), false},
			}, types.Choices{})},
	}.New()

	res := testResolver{assert, map[ref.Ref]types.Package{imported.Ref(): imported}}

	localStructName := "Local"
	resolved := types.MakeStructTypeRef(localStructName, []types.Field{
		types.Field{"a", types.MakePrimitiveTypeRef(types.Int8Kind), false},
	}, types.Choices{})

	g := Generator{&res}
	assert.Equal(localStructName, g.UserName(resolved))

	listOfImported := types.MakeCompoundTypeRef("", types.ListKind, types.MakeTypeRef("S1", imported.Ref()))
	assert.Equal(fmt.Sprintf("ListOf%s_%s", ToTag(imported.Ref().String()), "S1"), g.UserName(listOfImported))
}
