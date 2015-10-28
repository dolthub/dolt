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
	tempDir string
	ldbDir  string
	out     *os.File
}

func (suite *ClientTestSuite) SetupSuite() {
	dir, err := ioutil.TempDir(os.TempDir(), "nomstest")
	d.Chk.NoError(err)
	out, err := ioutil.TempFile(dir, "out")
	d.Chk.NoError(err)

	suite.tempDir = dir
	suite.ldbDir = path.Join(dir, "ldb")
	suite.out = out
}

func (suite *ClientTestSuite) TearDownSuite() {
	suite.out.Close()
	defer d.Chk.NoError(os.RemoveAll(suite.tempDir))
}

func (suite *ClientTestSuite) Run(m func(), args []string) string {
	origArgs := os.Args
	origOut := os.Stdout

	os.Args = append([]string{"testcmd", "-ldb", suite.ldbDir}, args...)
	os.Stdout = suite.out

	defer func() {
		os.Args = origArgs
		os.Stdout = origOut
	}()

	m()

	_, err := suite.out.Seek(0, 0)
	d.Chk.NoError(err)
	b, err := ioutil.ReadAll(os.Stdout)
	d.Chk.NoError(err)

	_, err = suite.out.Seek(0, 0)
	d.Chk.NoError(err)
	err = suite.out.Truncate(0)
	d.Chk.NoError(err)

	return string(b)
}
