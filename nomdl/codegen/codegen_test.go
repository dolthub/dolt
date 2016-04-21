package main

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/nomdl/codegen/code"
	"github.com/attic-labs/noms/nomdl/pkg"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func assertOutput(inPath, goldenPath string, t *testing.T) {
	assert := assert.New(t)
	emptyDS := datas.NewDataStore(chunks.NewMemoryStore()) // Will be DataStore containing imports

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
	pkg := pkg.ParseNomDL("gen", inFile, filepath.Dir(inPath), emptyDS)
	written := map[string]bool{}
	gen := newCodeGen(&buf, getBareFileName(inPath), written, depsMap{}, pkg)
	gen.WritePackage()

	bs := buf.Bytes()
	assert.Equal(string(goldenBytes), string(bs), "%s did not generate the same string", inPath)
}

func TestGeneratedFiles(t *testing.T) {
	files, err := filepath.Glob("test/*.noms")
	d.Chk.NoError(err)
	assert.NotEmpty(t, files)
	for _, n := range files {
		_, file := filepath.Split(n)
		if file == "struct_with_imports.noms" {
			// We are not writing deps in this test so lookup by ref does not work.
			continue
		}
		if file == "struct_with_list.noms" || file == "struct_with_dup_list.noms" {
			// These two files race to write ListOfUint8
			continue
		}
		assertOutput(n, filepath.Join("test", "gen", file+".js"), t)
	}
}

func TestSkipDuplicateTypes(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "codegen_test_")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	leaf1 := types.NewPackage([]types.Type{
		types.MakeEnumType("E1", "a", "b"),
		types.MakeStructType("S1", []types.Field{
			types.Field{"f", types.MakeCompoundType(types.ListKind, types.MakePrimitiveType(types.Uint16Kind)), false},
			types.Field{"e", types.MakeType(ref.Ref{}, 0), false},
		}, types.Choices{}),
	}, []ref.Ref{})
	leaf2 := types.NewPackage([]types.Type{
		types.MakeStructType("S2", []types.Field{
			types.Field{"f", types.MakeCompoundType(types.ListKind, types.MakePrimitiveType(types.Uint16Kind)), false},
		}, types.Choices{}),
	}, []ref.Ref{})

	written := map[string]bool{}
	tag1 := code.ToTag(leaf1.Ref())
	leaf1Path := filepath.Join(dir, tag1+".js")
	generateAndEmit(tag1, leaf1Path, written, depsMap{}, pkg.Parsed{Package: leaf1, Name: "p"})

	tag2 := code.ToTag(leaf2.Ref())
	leaf2Path := filepath.Join(dir, tag2+".js")
	generateAndEmit(tag2, leaf2Path, written, depsMap{}, pkg.Parsed{Package: leaf2, Name: "p"})

	code, err := ioutil.ReadFile(leaf2Path)
	assert.NoError(err)
	assert.NotContains(string(code), "type ListOfUint16")
}

func TestGenerateDeps(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDataStore(chunks.NewMemoryStore())
	dir, err := ioutil.TempDir("", "codegen_test_")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	leaf1 := types.NewPackage([]types.Type{types.MakeEnumType("e1", "a", "b")}, []ref.Ref{})
	leaf1Ref := ds.WriteValue(leaf1).TargetRef()
	leaf2 := types.NewPackage([]types.Type{types.MakePrimitiveType(types.BoolKind)}, []ref.Ref{})
	leaf2Ref := ds.WriteValue(leaf2).TargetRef()

	depender := types.NewPackage([]types.Type{}, []ref.Ref{leaf1Ref})
	dependerRef := ds.WriteValue(depender).TargetRef()

	top := types.NewPackage([]types.Type{}, []ref.Ref{leaf2Ref, dependerRef})
	types.RegisterPackage(&top)

	localPkgs := refSet{top.Ref(): true}
	generateDepCode(filepath.Base(dir), dir, map[string]bool{}, top, localPkgs, ds)

	leaf1Path := filepath.Join(dir, code.ToTag(leaf1.Ref())+".js")
	leaf2Path := filepath.Join(dir, code.ToTag(leaf2.Ref())+".js")
	leaf3Path := filepath.Join(dir, code.ToTag(depender.Ref())+".js")
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

	p := parsePackageFile("name", inFile, pkgDS)
	localPkgs := refSet{p.Ref(): true}
	pkgDS = generate("name", inFile, filepath.Join(dir, "out.js"), dir, map[string]bool{}, p, localPkgs, pkgDS)
	s := pkgDS.Head().Value().(types.Set)
	assert.EqualValues(1, s.Len())
	tr := s.First().(types.Ref).TargetValue(ds).(types.Package).Types()[0]
	assert.EqualValues(types.StructKind, tr.Kind())
}
