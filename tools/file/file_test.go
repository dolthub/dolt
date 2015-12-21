package file

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/d"
)

const (
	contents = "hey"
)

func TestSerialRunnerTestSuite(t *testing.T) {
	suite.Run(t, &FileTestSuite{})
}

type FileTestSuite struct {
	suite.Suite
	dir, src string
}

func (suite *FileTestSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.NoError(err)
	suite.src = filepath.Join(suite.dir, "srcfile")
	suite.NoError(ioutil.WriteFile(suite.src, []byte(contents), 0644))
}

func (suite *FileTestSuite) TearDownTest() {
	os.Remove(suite.dir)
}

func (suite *FileTestSuite) TestCopyFile() {
	dst := filepath.Join(suite.dir, "dstfile")
	DumbCopy(suite.src, dst)

	out, err := ioutil.ReadFile(dst)
	suite.NoError(err)
	suite.Equal(contents, string(out))
}

func (suite *FileTestSuite) TestCopyLink() {
	link := filepath.Join(suite.dir, "link")
	suite.NoError(os.Symlink(suite.src, link))

	dst := filepath.Join(suite.dir, "dstfile")
	DumbCopy(link, dst)

	info, err := os.Lstat(dst)
	suite.NoError(err)
	suite.True(info.Mode().IsRegular())

	out, err := ioutil.ReadFile(dst)
	suite.NoError(err)
	suite.Equal(contents, string(out))
}

func (suite *FileTestSuite) TestNoCopyDir() {
	dir, err := ioutil.TempDir(suite.dir, "")
	suite.NoError(err)

	dst := filepath.Join(suite.dir, "dst")
	suite.Error(d.Try(func() { DumbCopy(dir, dst) }))
}
