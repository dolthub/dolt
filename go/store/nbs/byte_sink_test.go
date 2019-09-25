// Copyright 2019 Liquidata, Inc.
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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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
	t           *testing.T
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
