package main

import (
	"bytes"
	"fmt"
	"go/build"
	"go/parser"
	"go/token"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/tools/imports"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/nomdl/codegen/code"
	"github.com/attic-labs/noms/nomdl/pkg"
	"github.com/attic-labs/noms/types"
)

func assertOutput(inPath, goldenPath string, t *testing.T) {
	assert := assert.New(t)
	emptyCS := chunks.NewMemoryStore() // Will be ChunkSource containing imports

	depsDir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	defer os.RemoveAll(depsDir)

	inFile, err := os.Open(inPath)
	assert.NoError(err)
	defer inFile.Close()

	goldenFile, err := os.Open(goldenPath)
	assert.NoError(err)
	defer goldenFile.Close()
	goldenBytes, err := ioutil.ReadAll(goldenFile)
	d.Chk.NoError(err)

	var buf bytes.Buffer
	pkg := pkg.ParseNomDL("test", inFile, emptyCS)
	gen := NewCodeGen(&buf, getBareFileName(inPath), nil, depsMap{}, pkg)
	gen.WritePackage()

	bs, err := imports.Process("", buf.Bytes(), nil)
	d.Chk.NoError(err)

	assert.Equal(string(goldenBytes), string(bs))
}

func TestGeneratedFiles(t *testing.T) {
	files, err := filepath.Glob("test/gen/*.noms")
	d.Chk.NoError(err)
	for _, n := range files {
		_, file := filepath.Split(n)
		assertOutput(n, "test/"+file[:len(file)-5]+".go", t)
	}
}

func TestCanUseDef(t *testing.T) {
	assert := assert.New(t)
	emptyCS := chunks.NewMemoryStore()

	depsDir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	defer os.RemoveAll(depsDir)

	assertCanUseDef := func(s string, using, named bool) {
		pkg := pkg.ParseNomDL("fakefile", bytes.NewBufferString(s), emptyCS)
		gen := NewCodeGen(nil, "fakefile", nil, depsMap{}, pkg)
		for _, t := range pkg.UsingDeclarations {
			assert.Equal(using, gen.canUseDef(t))
		}
		for _, t := range pkg.Types {
			assert.Equal(named, gen.canUseDef(t))
		}
	}

	good := `
		using List(Int8)
		using Set(Int8)
		using Map(Int8, Int8)
		using Map(Int8, Set(Int8))
		using Map(Int8, Map(Int8, Int8))

		struct Simple {
			x: Int8
		}
		using Set(Simple)
		using Map(Simple, Int8)
		using Map(Simple, Simple)
		`
	assertCanUseDef(good, true, true)

	good = `
		struct Tree {
		  children: List(Tree)
		}
		`
	assertCanUseDef(good, true, true)

	bad := `
		struct WithList {
			x: List(Int8)
		}
		using Set(WithList)
		using Map(WithList, Int8)

		struct WithSet {
			x: Set(Int8)
		}
		using Set(WithSet)
		using Map(WithSet, Int8)

		struct WithMap {
			x: Map(Int8, Int8)
		}
		using Set(WithMap)
		using Map(WithMap, Int8)
		`
	assertCanUseDef(bad, false, true)

	bad = `
		struct Commit {
			value: Value
			parents: Set(Commit)
		}
		`
	assertCanUseDef(bad, false, false)

	bad = `
		Set(Set(Int8))
		Set(Map(Int8, Int8))
		Set(List(Int8))
		Map(Set(Int8), Int8)
		Map(Map(Int8, Int8), Int8)
		Map(List(Int8), Int8)
		`

	for _, line := range strings.Split(bad, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		assertCanUseDef(fmt.Sprintf("using %s", line), false, false)
		assertCanUseDef(fmt.Sprintf("struct S { x: %s }", line), false, false)
	}
}

func TestImportedTypes(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDataStore(chunks.NewMemoryStore())
	pkgDS := dataset.NewDataset(ds, "packages")

	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	imported := types.PackageDef{
		Types: types.ListOfTypeRefDef{
			types.MakeEnumTypeRef("E1", "a", "b"),
			types.MakeStructTypeRef("S1", []types.Field{
				types.Field{"f", types.MakePrimitiveTypeRef(types.BoolKind), false},
			}, types.Choices{})},
	}.New()
	importedRef := types.WriteValue(imported.NomsValue(), ds)
	pkgDS, ok := pkgDS.Commit(types.NewSetOfRefOfPackage().Insert(types.NewRefOfPackage(importedRef)).NomsValue())
	assert.True(ok)
	good := fmt.Sprintf(`
		alias Other = import "%s"

		using List(Other.S1)
		struct Simple {
			E: Other.E1
			S: Other.S1
		}
		`, importedRef)

	inFile := filepath.Join(dir, "in.noms")
	err = ioutil.WriteFile(inFile, []byte(good), 0600)
	assert.NoError(err)

	depsDir := filepath.Join(thisFileDir(), "deps")
	defer os.RemoveAll(depsDir)

	outFile := filepath.Join(dir, "out.go")
	pkgDS = generate("name", inFile, outFile, depsDir, pkgDS)

	// Check that dependency code was generated.
	expectedDepPkgAbs := filepath.Join(depsDir, code.ToTag(importedRef.String()))
	_, err = os.Stat(expectedDepPkgAbs)
	assert.NoError(err)

	// Get the imports from out.go
	ast, err := parser.ParseFile(token.NewFileSet(), outFile, nil, parser.ImportsOnly)
	assert.NoError(err)
	imports := []string{}
	for _, s := range ast.Imports {
		//Strip enclosing quotes from s.Path.Value
		imports = append(imports, s.Path.Value[1:len(s.Path.Value)-1])
	}
	// Get the canonical import path for the generated dependency code.
	expectedDepPkg, err := build.ImportDir(expectedDepPkgAbs, build.FindOnly)
	assert.NoError(err)

	// Make sure that out.go imported the dependency code.
	assert.Contains(imports, expectedDepPkg.ImportPath)
}

func thisFileDir() string {
	_, filename, _, _ := runtime.Caller(1)
	return path.Dir(filename)
}

func TestGenerateDeps(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()
	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	leaf1 := types.PackageDef{Types: types.ListOfTypeRefDef{types.MakeEnumTypeRef("e1", "a", "b")}}.New()
	leaf1Ref := types.WriteValue(leaf1.NomsValue(), cs)
	leaf2 := types.PackageDef{Types: types.ListOfTypeRefDef{types.MakePrimitiveTypeRef(types.BoolKind)}}.New()
	leaf2Ref := types.WriteValue(leaf2.NomsValue(), cs)

	depender := types.PackageDef{Dependencies: types.SetOfRefOfPackageDef{leaf1Ref: true}}.New()
	dependerRef := types.WriteValue(depender.NomsValue(), cs)

	top := types.PackageDef{Dependencies: types.SetOfRefOfPackageDef{leaf2Ref: true, dependerRef: true}}.New()
	types.RegisterPackage(&top)

	generateDepCode(dir, top, cs)

	leaf1Path := filepath.Join(dir, code.ToTag(leaf1.Ref().String()), code.ToTag(leaf1.Ref().String())+".go")
	leaf2Path := filepath.Join(dir, code.ToTag(leaf2.Ref().String()), code.ToTag(leaf2.Ref().String())+".go")
	leaf3Path := filepath.Join(dir, code.ToTag(depender.Ref().String()), code.ToTag(depender.Ref().String())+".go")
	_, err = os.Stat(leaf1Path)
	assert.NoError(err)
	_, err = os.Stat(leaf2Path)
	assert.NoError(err)
	_, err = os.Stat(leaf3Path)
	assert.NoError(err)
}

func TestCommitNewPackages(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDataStore(chunks.NewMemoryStore())
	pkgDS := dataset.NewDataset(ds, "packages")

	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	defer os.RemoveAll(dir)
	inFile := filepath.Join(dir, "in.noms")
	err = ioutil.WriteFile(inFile, []byte("struct Simple{a:Bool}"), 0600)
	assert.NoError(err)

	pkgDS = generate("name", inFile, filepath.Join(dir, "out.go"), dir, pkgDS)
	s := types.SetOfRefOfPackageFromVal(pkgDS.Head().Value())
	assert.EqualValues(1, s.Len())
	tr := s.Any().GetValue(ds).GetNamedType("Simple")
	assert.EqualValues(types.StructKind, tr.Kind())
}
