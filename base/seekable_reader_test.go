package base

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestSeekableReaderTestSuite(t *testing.T) {
	suite.Run(t, &SeekableReaderTestSuite{})
}

type SeekableReaderTestSuite struct {
	suite.Suite
	dir        string
	content    []byte
	contentLen int64
	contentRSC ReadSeekCloser
}

func (suite *SeekableReaderTestSuite) SetupTest() {
	suite.content = []byte("0123456789")
	suite.contentLen = int64(len(suite.content))

	var err error
	suite.dir, err = ioutil.TempDir("", "")
	suite.NoError(err)
	cache, err := ioutil.TempFile(suite.dir, "")
	suite.NoError(err)

	suite.contentRSC = &seekableReader{
		r:      bytes.NewReader(suite.content),
		cache:  cache,
		length: suite.contentLen,
	}
}

func (suite *SeekableReaderTestSuite) TearDownTest() {
	suite.contentRSC.Close()
	os.Remove(suite.dir)
}

func (suite *SeekableReaderTestSuite) TestRead() {
	suite.readAndExpect(suite.content)
}

func (suite *SeekableReaderTestSuite) TestReadAll() {
	b, err := ioutil.ReadAll(suite.contentRSC)
	suite.NoError(err)
	suite.Equal(string(suite.content), string(b))
}

func (suite *SeekableReaderTestSuite) TestSeekFromStart() {
	offset := suite.contentLen - 2
	ret, err := suite.contentRSC.Seek(offset, 0)
	suite.NoError(err)
	suite.EqualValues(offset, ret)
	suite.readAndExpect(suite.content[offset:])
}

func (suite *SeekableReaderTestSuite) TestSeekFromEnd() {
	// Seek to last two bytes
	offset := suite.contentLen - 2
	ret, err := suite.contentRSC.Seek(2, 2)
	suite.NoError(err)
	suite.EqualValues(offset, ret)
	suite.readAndExpect(suite.content[offset:])
}

func (suite *SeekableReaderTestSuite) TestSeekFromCur() {
	// Seek to last two bytes
	offset := suite.contentLen - 2
	ret, err := suite.contentRSC.Seek(2, 1)
	suite.NoError(err)
	ret, err = suite.contentRSC.Seek(offset-2, 1)
	suite.NoError(err)
	suite.EqualValues(offset, ret)
	suite.readAndExpect(suite.content[offset:])
}

func (suite *SeekableReaderTestSuite) TestReadSeekRead() {
	suite.readAndExpect(suite.content[:2])
	suite.contentRSC.Seek(2, 2)
	suite.readAndExpect(suite.content[suite.contentLen-2:])
}

func (suite *SeekableReaderTestSuite) TestReadSeekBackRead() {
	suite.readAndExpect(suite.content[:2])
	suite.contentRSC.Seek(0, 0)
	suite.readAndExpect(suite.content)
}

func (suite *SeekableReaderTestSuite) TestSeekFwdSeekBackReadSome() {
	suite.contentRSC.Seek(0, 2)
	suite.contentRSC.Seek(0, 0)
	suite.readAndExpect(suite.content[:4])
}

func (suite *SeekableReaderTestSuite) readAndExpect(expected []byte) {
	expectedLen := len(expected)
	p := make([]byte, expectedLen)
	n, err := io.ReadFull(suite.contentRSC, p)

	suite.NoError(err)
	suite.EqualValues(expectedLen, n, "Didn't read all the data")
	suite.True(bytes.Equal(expected, p), "%s != %s", string(expected), string(p))
}
