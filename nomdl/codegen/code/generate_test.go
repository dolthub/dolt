package code

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

type testResolver struct {
	assert *assert.Assertions
	deps   map[ref.Ref]types.Package
}

func (res *testResolver) Resolve(t types.Type, pkg *types.Package) types.Type {
	if !t.IsUnresolved() {
		return t
	}

	if !t.HasPackageRef() {
		res.assert.Fail("Test does not handle local references")
	}

	if t.PackageRef() == pkg.Ref() {
		return pkg.Types()[t.Ordinal()]
	}

	dep, ok := res.deps[t.PackageRef()]
	res.assert.True(ok, "Package %s is referenced in %+v, but is not a dependency.", t.PackageRef().String(), t)
	return dep.Types()[t.Ordinal()]
}

func TestUserName(t *testing.T) {
	assert := assert.New(t)

	imported := types.NewPackage([]types.Type{
		types.MakeEnumType("E1", "a", "b"),
		types.MakeStructType("S1", []types.Field{
			types.Field{"f", types.MakePrimitiveType(types.BoolKind), false},
		}, types.Choices{}),
	}, []ref.Ref{})

	res := testResolver{assert, map[ref.Ref]types.Package{imported.Ref(): imported}}

	localStructName := "Local"
	resolved := types.MakeStructType(localStructName, []types.Field{
		types.Field{"a", types.MakePrimitiveType(types.Int8Kind), false},
	}, types.Choices{})

	g := Generator{R: &res, Package: &imported}
	assert.Equal(localStructName, g.UserName(resolved))

	listOfImported := types.MakeCompoundType(types.ListKind, types.MakeType(imported.Ref(), 1))
	assert.Equal(fmt.Sprintf("ListOfS1"), g.UserName(listOfImported))
}
