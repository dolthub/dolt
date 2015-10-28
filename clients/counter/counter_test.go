package main

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
)

var (
	dir string
	ldb string
)

func TestMain(m *testing.M) {
	dir, err := ioutil.TempDir(os.TempDir(), "nomstest")
	d.Chk.NoError(err)
	ldb = path.Join(dir, "ldb")

	defer d.Chk.NoError(os.RemoveAll(dir))

	os.Exit(m.Run())
}

func TestCounter(t *testing.T) {
	assert := assert.New(t)
	args := []string{"counter", "-ldb", ldb, "-ds", "counter"}
	assert.Equal("1\n", util.Run(main, args))
	assert.Equal("2\n", util.Run(main, args))
	assert.Equal("3\n", util.Run(main, args))
}
