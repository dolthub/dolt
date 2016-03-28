package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
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
	"golang.org/x/tools/imports"
)

func assertOutput(inPath, lang, goldenPath string, t *testing.T) {
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
	_, file := filepath.Split(inPath)
	if file == "struct_with_list.noms" {
		// List<Uint8> is provided twice in the noms files to ensure it is only written once. Therefore we emulate that it was already written for struct_with_list.noms.
		written["ListOfUint8"] = true
	}
	gen := newCodeGen(&buf, getBareFileName(inPath), lang, written, depsMap{}, pkg)
	gen.WritePackage()

	bs := buf.Bytes()
	if lang == "go" {
		bs, err = imports.Process("", bs, nil)
		d.Chk.NoError(err)
	}

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
		assertOutput(n, "go", filepath.Join("test", "gen", file+".go"), t)
		assertOutput(n, "js", filepath.Join("test", "gen", file+".js"), t)
	}
}

func TestCanUseDef(t *testing.T) {
	assert := assert.New(t)
	emptyDS := datas.NewDataStore(chunks.NewMemoryStore())

	depsDir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	defer os.RemoveAll(depsDir)

	assertCanUseDef := func(s string, using, named bool) {
		pkg := pkg.ParseNomDL("fakefile", bytes.NewBufferString(s), "", emptyDS)
		gen := newCodeGen(nil, "fakefile", "go", map[string]bool{}, depsMap{}, pkg)
		for _, t := range pkg.UsingDeclarations {
			assert.Equal(using, gen.canUseDef(t, gen.pkg.Package))
		}
		for _, t := range pkg.Types() {
			assert.Equal(named, gen.canUseDef(t, gen.pkg.Package))
		}
	}

	good := `
		using List<Int8>
		using Set<Int8>
		using Map<Int8, Int8>
		using Map<Int8, Set<Int8>>
		using Map<Int8, Map<Int8, Int8>>

		struct Simple {
			x: Int8
		}
		using Set<Simple>
		using Map<Simple, Int8>
		using Map<Simple, Simple>
		`
	assertCanUseDef(good, true, true)

	good = `
		struct Tree {
		  children: List<Tree>
		}
		`
	assertCanUseDef(good, true, true)

	bad := `
		struct WithList {
			x: List<Int8>
		}
		using Set<WithList>
		using Map<WithList, Int8>

		struct WithSet {
			x: Set<Int8>
		}
		using Set<WithSet>
		using Map<WithSet, Int8>

		struct WithMap {
			x: Map<Int8, Int8>
		}
		using Set<WithMap>
		using Map<WithMap, Int8>
		`
	assertCanUseDef(bad, false, true)

	bad = `
		struct Commit {
			value: Value
			parents: Set<Commit>
		}
		`
	assertCanUseDef(bad, false, false)

	bad = `
		Set<Set<Int8>>
		Set<Map<Int8, Int8>>
		Set<List<Int8>>
		Map<Set<Int8>, Int8>
		Map<Map<Int8, Int8>, Int8>
		Map<List<Int8>, Int8>
		`

	for _, line := range strings.Split(bad, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		assertCanUseDef(fmt.Sprintf("using %s", line), false, false)
		assertCanUseDef(fmt.Sprintf("struct S { x: %s }", line), false, false)
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
	leaf1Path := filepath.Join(dir, tag1+".go")
	generateAndEmit(tag1, leaf1Path, written, depsMap{}, pkg.Parsed{Package: leaf1, Name: "p"})

	tag2 := code.ToTag(leaf2.Ref())
	leaf2Path := filepath.Join(dir, tag2+".go")
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
	leaf1Ref := ds.WriteValue(leaf1)
	leaf2 := types.NewPackage([]types.Type{types.MakePrimitiveType(types.BoolKind)}, []ref.Ref{})
	leaf2Ref := ds.WriteValue(leaf2)

	depender := types.NewPackage([]types.Type{}, []ref.Ref{leaf1Ref})
	dependerRef := ds.WriteValue(depender)

	top := types.NewPackage([]types.Type{}, []ref.Ref{leaf2Ref, dependerRef})
	types.RegisterPackage(&top)

	localPkgs := refSet{top.Ref(): true}
	generateDepCode(filepath.Base(dir), dir, map[string]bool{}, top, localPkgs, ds)

	leaf1Path := filepath.Join(dir, code.ToTag(leaf1.Ref())+".go")
	leaf2Path := filepath.Join(dir, code.ToTag(leaf2.Ref())+".go")
	leaf3Path := filepath.Join(dir, code.ToTag(depender.Ref())+".go")
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
	pkgDS = generate("name", inFile, filepath.Join(dir, "out.go"), dir, map[string]bool{}, p, localPkgs, pkgDS)
	s := pkgDS.Head().Value().(types.SetOfRefOfPackage)
	assert.EqualValues(1, s.Len())
	tr := s.First().TargetValue(ds).Types()[0]
	assert.EqualValues(types.StructKind, tr.Kind())
}

func TestMakeGoIdentifier(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("hello", makeGoIdentifier("hello"))
	assert.Equal("hello88", makeGoIdentifier("hello88"))
	assert.Equal("_88hello", makeGoIdentifier("88hello"))
	assert.Equal("h_e_l_l_0", makeGoIdentifier("h-e-l-l-0"))
	assert.Equal("_hello", makeGoIdentifier("\u2318hello"))
	assert.Equal("he_llo", makeGoIdentifier("he\u2318llo"))

}

func TestCanUseDefFromImport(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDataStore(chunks.NewMemoryStore())

	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	byPathNomDL := filepath.Join(dir, "filedep.noms")
	err = ioutil.WriteFile(byPathNomDL, []byte("struct FromFile{i:Int8}"), 0600)
	assert.NoError(err)

	r1 := strings.NewReader(`
		struct A {
			B: B
		}
		struct B {
			X: Int64
		}`)
	pkg1 := pkg.ParseNomDL("test1", r1, dir, ds)
	pkgRef1 := ds.WriteValue(pkg1.Package)

	r2 := strings.NewReader(fmt.Sprintf(`
		alias Other = import "%s"
		struct C {
			C: Map<Int64, Other.A>
		}
		`, pkgRef1))
	pkg2 := pkg.ParseNomDL("test2", r2, dir, ds)
	gen2 := newCodeGen(nil, "test2", "go", map[string]bool{}, depsMap{pkg1.Ref(): pkg1.Package}, pkg2)

	assert.True(gen2.canUseDef(pkg2.Types()[0], gen2.pkg.Package))
}
