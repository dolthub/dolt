package main

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
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
	gen := NewCodeGen(&buf, pkg)
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
