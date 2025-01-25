// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nbs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestBlockBufferTableSink(t *testing.T) {
	createSink := func(*testing.T) ByteSink {
		return NewBlockBufferByteSink(128)
	}

	suite.Run(t, &TableSinkSuite{createSink, t})
}

func TestFixedBufferTableSink(t *testing.T) {
	createSink := func(*testing.T) ByteSink {
		return NewFixedBufferByteSink(make([]byte, 32*1024))
	}

	suite.Run(t, &TableSinkSuite{createSink, t})
}

func TestBufferedFileByteSink(t *testing.T) {
	createSink := func(t *testing.T) ByteSink {
		sink, err := NewBufferedFileByteSink(t.TempDir(), 4*1024, 16)
		require.NoError(t, err)

		return sink
	}

	suite.Run(t, &TableSinkSuite{createSink, t})

	t.Run("ReaderTwice", func(t *testing.T) {
		sink, err := NewBufferedFileByteSink(t.TempDir(), 4*1024, 16)
		require.NoError(t, err)
		_, err = sink.Write([]byte{1, 2, 3, 4})
		require.NoError(t, err)
		r, err := sink.Reader()
		require.NoError(t, err)
		require.NotNil(t, r)
		var readbytes [5]byte
		n, err := r.Read(readbytes[:])
		require.Equal(t, 4, n)
		require.True(t, bytes.Equal(readbytes[:4], []byte{1, 2, 3, 4}))
		r.Close()
		r, err = sink.Reader()
		require.NoError(t, err)
		require.NotNil(t, r)
		n, err = r.Read(readbytes[:])
		require.Equal(t, 4, n)
		require.True(t, bytes.Equal(readbytes[:4], []byte{1, 2, 3, 4}))
		r.Close()
	})
}

type TableSinkSuite struct {
	sinkFactory func(*testing.T) ByteSink
	t           *testing.T
}

func (suite2 *TableSinkSuite) SetS(suite suite.TestingSuite) {}

var _ suite.TestingSuite = (*TableSinkSuite)(nil)

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
	sink := suite.sinkFactory(suite.t)
	err := writeToSink(sink)
	require.NoError(suite.t, err)

	bb := bytes.NewBuffer(nil)
	err = sink.Flush(bb)
	require.NoError(suite.t, err)

	verifyContents(suite.t, bb.Bytes())
}

func (suite *TableSinkSuite) TestWriteAndFlushToFile() {
	sink := suite.sinkFactory(suite.t)
	err := writeToSink(sink)
	require.NoError(suite.t, err)

	path := filepath.Join(os.TempDir(), uuid.New().String())
	err = sink.FlushToFile(path)
	require.NoError(suite.t, err)

	data, err := os.ReadFile(path)
	require.NoError(suite.t, err)

	verifyContents(suite.t, data)
}
