package nbs

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestBlockBufferTableSink(t *testing.T) {
	createSink := func() ByteSink {
		return NewBlockBufferTableSink(128)
	}

	suite.Run(t, &TableSinkSuite{createSink, t})
}

func TestFixedBufferTableSink(t *testing.T) {
	createSink := func() ByteSink {
		return NewFixedBufferTableSink(make([]byte, 32*1024))
	}

	suite.Run(t, &TableSinkSuite{createSink, t})
}

func TestBufferedFileByteSink(t *testing.T) {
	createSink := func() ByteSink {
		sink, err := NewBufferedFileByteSink(4*1024, 16)
		require.NoError(t, err)

		return sink
	}

	suite.Run(t, &TableSinkSuite{createSink, t})
}

type TableSinkSuite struct {
	sinkFactory func() ByteSink
	t    *testing.T
}

func (suite *TableSinkSuite) SetT(t *testing.T) {
	suite.t = t
}

func (suite *TableSinkSuite) T() *testing.T {
	return suite.t
}

func writeToSink(sink ByteSink) error {
	data := make([]byte, 64)
	for i := 0; i < 64; i++ {
		data[i] = byte(i)
	}

	for i := 0; i < 32; i++ {
		_, err := sink.Write(data)

		if err != nil {
			return err
		}
	}

	return nil
}

func verifyContents(t *testing.T, bytes []byte) {
	for i := 0; i < 64*32; i++ {
		assert.Equal(t, byte(i%64), bytes[i])
	}
}

func (suite *TableSinkSuite) TestWriteAndFlush() {
	sink := suite.sinkFactory()
	err := writeToSink(sink)
	require.NoError(suite.t, err)

	bb := bytes.NewBuffer(nil)
	err = sink.Flush(bb)
	assert.NoError(suite.t, err)

	verifyContents(suite.t, bb.Bytes())
}

func (suite *TableSinkSuite) TestWriteAndFlushToFile() {
	sink := suite.sinkFactory()
	err := writeToSink(sink)
	require.NoError(suite.t, err)

	path := filepath.Join(os.TempDir(), uuid.New().String())
	err = sink.FlushToFile(path)
	require.NoError(suite.t, err)

	data, err := ioutil.ReadFile(path)
	require.NoError(suite.t, err)

	verifyContents(suite.t, data)
}