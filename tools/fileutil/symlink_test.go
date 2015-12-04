package fileutil

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/d"
)

func TestSymlinkTestSuite(t *testing.T) {
	suite.Run(t, &SymlinkTestSuite{})
}

type SymlinkTestSuite struct {
	suite.Suite
	dir, oldname, newname, expected string
}

func (suite *SymlinkTestSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.NoError(err)
	suite.expected = "hello"
	suite.oldname = filepath.Join(suite.dir, "oldname")
	suite.newname = filepath.Join(suite.dir, "newname")

	suite.NoError(ioutil.WriteFile(suite.oldname, []byte(suite.expected), 0644))
}

func (suite *SymlinkTestSuite) TestMissingAltogether() {
	ForceSymlink(suite.oldname, suite.newname)

	info, err := os.Lstat(suite.newname)
	suite.NoError(err)
	suite.Equal(os.ModeSymlink, info.Mode()&os.ModeSymlink)

	read, err := ioutil.ReadFile(suite.newname)
	suite.NoError(err)
	suite.EqualValues(suite.expected, string(read))
}

func (suite *SymlinkTestSuite) TestClobberRegularFile() {
	suite.NoError(ioutil.WriteFile(suite.newname, []byte("bad!"), 0644))
	ForceSymlink(suite.oldname, suite.newname)

	info, err := os.Lstat(suite.newname)
	suite.NoError(err)
	suite.Equal(os.ModeSymlink, info.Mode()&os.ModeSymlink)

	read, err := ioutil.ReadFile(suite.newname)
	suite.NoError(err)
	suite.EqualValues(suite.expected, string(read))
}

func (suite *SymlinkTestSuite) TestWontClobberDir() {
	suite.NoError(os.Mkdir(suite.newname, 0755))
	err := d.Try(func() { ForceSymlink(suite.oldname, suite.newname) })
	suite.NotNil(err)
	suite.IsType(d.UsageError{}, err)
}

func (suite *SymlinkTestSuite) TearDownTest() {
	os.Remove(suite.dir)
}
