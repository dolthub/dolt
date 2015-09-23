package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/tools/imports"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/nomdl/parse"
)

func assertOutput(inPath, goldenPath string, t *testing.T) {
	assert := assert.New(t)

	inFile, err := os.Open(inPath)
	assert.NoError(err)
	defer inFile.Close()

	goldenFile, err := os.Open(goldenPath)
	assert.NoError(err)
	defer goldenFile.Close()
	goldenBytes, err := ioutil.ReadAll(goldenFile)
	d.Chk.NoError(err)

	var buf bytes.Buffer
	pkg := parse.ParsePackage("", inFile)
	gen := NewCodeGen(&buf, getBareFileName(inPath), pkg)
	gen.WritePackage("test")

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

	assertCanUseDef := func(s string, using, named bool) {
		pkg := parse.ParsePackage("", bytes.NewBufferString(s))
		gen := NewCodeGen(nil, "fakefile", pkg)
		for _, t := range pkg.UsingDeclarations {
			assert.Equal(using, gen.canUseDef(t), s)
		}
		for _, t := range pkg.NamedTypes {
			assert.Equal(named, gen.canUseDef(t), s)
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
		Set(Set(Int8))
		Set(Map(Int, Int8))
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
