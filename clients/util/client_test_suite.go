package util

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/d"
)

type ClientTestSuite struct {
	suite.Suite
	TempDir string
	LdbDir  string
	out     *os.File
}

func (suite *ClientTestSuite) SetupSuite() {
	dir, err := ioutil.TempDir(os.TempDir(), "nomstest")
	d.Chk.NoError(err)
	out, err := ioutil.TempFile(dir, "out")
	d.Chk.NoError(err)

	suite.TempDir = dir
	suite.LdbDir = path.Join(dir, "ldb")
	suite.out = out
}

func (suite *ClientTestSuite) TearDownSuite() {
	suite.out.Close()
	defer d.Chk.NoError(os.RemoveAll(suite.TempDir))
}

func (suite *ClientTestSuite) Run(m func(), args []string) string {
	origArgs := os.Args
	origOut := os.Stdout
	origErr := os.Stderr

	os.Args = append([]string{"testcmd", "-ldb", suite.LdbDir}, args...)
	os.Stdout = suite.out

	// TODO: If some tests need this, we can return it as a separate out param. But more convenient to swallow it until then.
	devnull, err := os.Open(os.DevNull)
	d.Chk.NoError(err)
	os.Stderr = devnull

	defer func() {
		os.Args = origArgs
		os.Stdout = origOut
		os.Stderr = origErr
	}()

	m()

	_, err = suite.out.Seek(0, 0)
	d.Chk.NoError(err)
	b, err := ioutil.ReadAll(os.Stdout)
	d.Chk.NoError(err)

	_, err = suite.out.Seek(0, 0)
	d.Chk.NoError(err)
	err = suite.out.Truncate(0)
	d.Chk.NoError(err)

	return string(b)
}
