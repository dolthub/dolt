package pkg

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
)

func TestImportSuite(t *testing.T) {
	suite.Run(t, &ImportTestSuite{})
}

type ImportTestSuite struct {
	suite.Suite
	vrw       types.ValueReadWriter
	imported  types.Package
	importRef ref.Ref
	nested    types.Package
	nestedRef ref.Ref
}

func (suite *ImportTestSuite) SetupTest() {
	suite.vrw = datas.NewDataStore(chunks.NewMemoryStore())

	ns := types.MakeStructType("NestedDepStruct", []types.Field{}, types.Choices{
		types.Field{"b", types.MakePrimitiveType(types.BoolKind), false},
		types.Field{"i", types.MakePrimitiveType(types.Int8Kind), false},
	})
	suite.nested = types.NewPackage([]types.Type{ns}, []ref.Ref{})
	suite.nestedRef = suite.vrw.WriteValue(suite.nested).TargetRef()

	fs := types.MakeStructType("ForeignStruct", []types.Field{
		types.Field{"b", types.MakeType(ref.Ref{}, 1), false},
		types.Field{"n", types.MakeType(suite.nestedRef, 0), false},
	},
		types.Choices{})
	fe := types.MakeEnumType("ForeignEnum", "uno", "dos")
	suite.imported = types.NewPackage([]types.Type{fs, fe}, []ref.Ref{suite.nestedRef})
	suite.importRef = suite.vrw.WriteValue(suite.imported).TargetRef()
}

func (suite *ImportTestSuite) TestGetDeps() {
	deps := getDeps([]ref.Ref{suite.importRef}, suite.vrw)
	suite.Len(deps, 1)
	imported, ok := deps[suite.importRef]
	suite.True(ok, "%s is a dep; should have been found.", suite.importRef.String())

	deps = getDeps(imported.Dependencies(), suite.vrw)
	suite.Len(deps, 1)
	imported, ok = deps[suite.nestedRef]
	suite.True(ok, "%s is a dep; should have been found.", suite.nestedRef.String())
}

func (suite *ImportTestSuite) TestResolveNamespace() {
	deps := getDeps([]ref.Ref{suite.importRef}, suite.vrw)
	t := resolveNamespace(types.MakeUnresolvedType("Other", "ForeignEnum"), map[string]ref.Ref{"Other": suite.importRef}, deps)
	suite.EqualValues(types.MakeType(suite.importRef, 1), t)
}

func (suite *ImportTestSuite) TestUnknownAlias() {
	deps := getDeps([]ref.Ref{suite.importRef}, suite.vrw)
	suite.Panics(func() {
		resolveNamespace(types.MakeUnresolvedType("Bother", "ForeignEnum"), map[string]ref.Ref{"Other": suite.importRef}, deps)
	})
}

func (suite *ImportTestSuite) TestUnknownImportedType() {
	deps := getDeps([]ref.Ref{suite.importRef}, suite.vrw)
	suite.Panics(func() {
		resolveNamespace(types.MakeUnresolvedType("Other", "NotThere"), map[string]ref.Ref{"Other": suite.importRef}, deps)
	})
}

func (suite *ImportTestSuite) TestDetectFreeVariable() {
	ls := types.MakeStructType("Local", []types.Field{
		types.Field{"b", types.MakePrimitiveType(types.BoolKind), false},
		types.Field{"n", types.MakeUnresolvedType("", "OtherLocal"), false},
	},
		types.Choices{})
	suite.Panics(func() {
		inter := intermediate{Types: []types.Type{ls}}
		resolveLocalOrdinals(&inter)
	})
}

func (suite *ImportTestSuite) TestImports() {
	find := func(n string, typ types.Type) types.Field {
		suite.Equal(types.StructKind, typ.Kind())
		for _, f := range typ.Desc.(types.StructDesc).Fields {
			if f.Name == n {
				return f
			}
		}
		suite.Fail("Could not find field", "%s not present", n)
		return types.Field{}
	}
	findChoice := func(n string, typ types.Type) types.Field {
		suite.Equal(types.StructKind, typ.Kind())
		for _, f := range typ.Desc.(types.StructDesc).Union {
			if f.Name == n {
				return f
			}
		}
		suite.Fail("Could not find choice", "%s not present", n)
		return types.Field{}
	}
	refFromNomsFile := func(path string) ref.Ref {
		ds := datas.NewDataStore(chunks.NewMemoryStore())
		inFile, err := os.Open(path)
		suite.NoError(err)
		defer inFile.Close()
		parsedDep := ParseNomDL("", inFile, filepath.Dir(path), ds)
		return ds.WriteValue(parsedDep.Package).TargetRef()
	}

	dir, err := ioutil.TempDir("", "")
	suite.NoError(err)
	defer os.RemoveAll(dir)

	byPathNomDL := filepath.Join(dir, "filedep.noms")
	err = ioutil.WriteFile(byPathNomDL, []byte("struct FromFile{i:Int8}"), 0600)
	suite.NoError(err)

	r := strings.NewReader(fmt.Sprintf(`
		alias Other = import "%s"
		alias ByPath = import "%s"

		using List<Other.ForeignEnum>
		using List<Local1>
		struct Local1 {
			a: Other.ForeignStruct
			b: Int16
			c: Local2
		}
		struct Local2 {
			a: ByPath.FromFile
			b: Other.ForeignEnum
		}
		struct Union {
			union {
				a: Other.ForeignStruct
				b: Local2
			}
		}
		struct WithUnion {
			a: Other.ForeignStruct
			b: union {
				s: Local1
				t: Other.ForeignEnum
			}
		}`, suite.importRef, filepath.Base(byPathNomDL)))
	p := ParseNomDL("testing", r, dir, suite.vrw)

	named := p.Types()[0]
	suite.Equal("Local1", named.Name())
	field := find("a", named)
	suite.EqualValues(suite.importRef, field.T.PackageRef())
	field = find("c", named)
	suite.EqualValues(ref.Ref{}, field.T.PackageRef())

	named = p.Types()[1]
	suite.Equal("Local2", named.Name())
	field = find("a", named)
	suite.EqualValues(refFromNomsFile(byPathNomDL), field.T.PackageRef())
	field = find("b", named)
	suite.EqualValues(suite.importRef, field.T.PackageRef())

	named = p.Types()[2]
	suite.Equal("Union", named.Name())
	field = findChoice("a", named)
	suite.EqualValues(suite.importRef, field.T.PackageRef())
	field = findChoice("b", named)
	suite.EqualValues(ref.Ref{}, field.T.PackageRef())

	named = p.Types()[3]
	suite.Equal("WithUnion", named.Name())
	field = find("a", named)
	suite.EqualValues(suite.importRef, field.T.PackageRef())
	namedUnion := find("b", named).T
	suite.True(namedUnion.IsUnresolved())
	namedUnion = p.Types()[namedUnion.Ordinal()]
	field = findChoice("s", namedUnion)
	suite.EqualValues(ref.Ref{}, field.T.PackageRef())
	field = findChoice("t", namedUnion)
	suite.EqualValues(suite.importRef, field.T.PackageRef())

	usings := p.UsingDeclarations
	suite.Len(usings, 2)
	suite.EqualValues(types.ListKind, usings[0].Kind())
	suite.EqualValues(suite.importRef, usings[0].Desc.(types.CompoundDesc).ElemTypes[0].PackageRef())
	suite.EqualValues(types.ListKind, usings[1].Kind())
	suite.EqualValues(0, usings[1].Desc.(types.CompoundDesc).ElemTypes[0].Ordinal())
}

func (suite *ImportTestSuite) TestImportWithLocalRef() {
	dir, err := ioutil.TempDir("", "")
	suite.NoError(err)
	defer os.RemoveAll(dir)

	byPathNomDL := filepath.Join(dir, "filedep.noms")
	err = ioutil.WriteFile(byPathNomDL, []byte("struct FromFile{i:Int8}"), 0600)
	suite.NoError(err)

	r1 := strings.NewReader(`
		struct A {
			B: B
		}
		struct B {
			X: Int64
		}`)
	pkg1 := ParseNomDL("test1", r1, dir, suite.vrw)
	pkgRef1 := suite.vrw.WriteValue(pkg1.Package).TargetRef()

	r2 := strings.NewReader(fmt.Sprintf(`
		alias Other = import "%s"
		struct C {
			C: Map<Int64, Other.A>
		}
		`, pkgRef1))
	pkg2 := ParseNomDL("test2", r2, dir, suite.vrw)

	ts := pkg2.Types()
	suite.Len(ts, 1)
	suite.EqualValues(types.StructKind, ts[0].Kind())
	mapType := ts[0].Desc.(types.StructDesc).Fields[0].T
	suite.EqualValues(types.MapKind, mapType.Kind())
	otherAType := mapType.Desc.(types.CompoundDesc).ElemTypes[1]
	suite.EqualValues(types.UnresolvedKind, otherAType.Kind())
	suite.EqualValues(pkgRef1, otherAType.PackageRef())
}
