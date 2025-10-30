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
	createSink := func() ByteSink {
		bs, err := NewBlockBufferByteSink(t.Context(), 128, NewUnlimitedMemQuotaProvider())
		require.NoError(t, err)
		return bs
	}

	suite.Run(t, &TableSinkSuite{createSink, t})
}

func TestFixedBufferTableSink(t *testing.T) {
	createSink := func() ByteSink {
		return NewFixedBufferByteSink(make([]byte, 32*1024))
	}

	suite.Run(t, &TableSinkSuite{createSink, t})
}

func TestBufferedFileByteSink(t *testing.T) {
	createSink := func() ByteSink {
		sink, err := NewBufferedFileByteSink("", 4*1024, 16)
		require.NoError(t, err)

		return sink
	}

	suite.Run(t, &TableSinkSuite{createSink, t})

	t.Run("ReaderTwice", func(t *testing.T) {
		sink, err := NewBufferedFileByteSink("", 4*1024, 16)
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

func TestBlockBufferByteSink(t *testing.T) {
	t.Run("Quota", func(t *testing.T) {
		t.Run("NoError", func(t *testing.T) {
			q := NewUnlimitedMemQuotaProvider()
			sink, err := NewBlockBufferByteSink(t.Context(), 4096, q)
			require.NoError(t, err)
			assert.Equal(t, uint64(4096), q.Usage())
			_, err = sink.Write(make([]byte, 4096))
			require.NoError(t, err)
			assert.Equal(t, uint64(4096), q.Usage())
			_, err = sink.Write(make([]byte, 4096))
			require.NoError(t, err)
			assert.Equal(t, uint64(8192), q.Usage())
			_, err = sink.Write(make([]byte, 0))
			require.NoError(t, err)
			assert.Equal(t, uint64(8192), q.Usage())
			_, err = sink.Write(make([]byte, 1))
			require.NoError(t, err)
			assert.Equal(t, uint64(12288), q.Usage())
			_, err = sink.Write(make([]byte, 4095))
			require.NoError(t, err)
			assert.Equal(t, uint64(12288), q.Usage())
			_, err = sink.Write(make([]byte, 65536))
			require.NoError(t, err)
			assert.Equal(t, uint64(77824), q.Usage())
			var buf bytes.Buffer
			err = sink.Flush(&buf)
			require.NoError(t, err)
			assert.Equal(t, uint64(77824), q.Usage())
			assert.Equal(t, 77824, buf.Len())
			sink.Close()
			assert.Equal(t, uint64(0), q.Usage())
		})
		t.Run("Error", func(t *testing.T) {
			t.Run("Init", func(t *testing.T) {
				q := &errorQuota{NewUnlimitedMemQuotaProvider(), 0}
				_, err := NewBlockBufferByteSink(t.Context(), 4096, q)
				assert.Error(t, err)
				assert.Equal(t, uint64(0), q.Usage())
				q = &errorQuota{NewUnlimitedMemQuotaProvider(), 2048}
				_, err = NewBlockBufferByteSink(t.Context(), 4096, q)
				assert.Error(t, err)
				assert.Equal(t, uint64(0), q.Usage())
			})
			t.Run("AfterWrite", func(t *testing.T) {
				q := &errorQuota{NewUnlimitedMemQuotaProvider(), 8192}
				sink, err := NewBlockBufferByteSink(t.Context(), 4096, q)
				require.NoError(t, err)
				assert.Equal(t, uint64(4096), q.Usage())
				_, err = sink.Write(make([]byte, 4096))
				require.NoError(t, err)
				_, err = sink.Write(make([]byte, 2048))
				require.NoError(t, err)
				n, err := sink.Write(make([]byte, 4096))
				assert.Error(t, err)
				assert.Equal(t, 2048, n)
				assert.Equal(t, uint64(8192), q.Usage())
				n, err = sink.Write(make([]byte, 1))
				assert.Error(t, err)
				assert.Equal(t, 0, n)
				sink.Close()
				assert.Equal(t, uint64(0), q.Usage())
			})
		})
	})
}

type TableSinkSuite struct {
	sinkFactory func() ByteSink
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
	sink := suite.sinkFactory()
	err := writeToSink(sink)
	require.NoError(suite.t, err)

	bb := bytes.NewBuffer(nil)
	err = sink.Flush(bb)
	require.NoError(suite.t, err)

	verifyContents(suite.t, bb.Bytes())
}

func (suite *TableSinkSuite) TestWriteAndFlushToFile() {
	sink := suite.sinkFactory()
	err := writeToSink(sink)
	require.NoError(suite.t, err)

	path := filepath.Join(os.TempDir(), uuid.New().String())
	err = sink.FlushToFile(path)
	require.NoError(suite.t, err)

	data, err := os.ReadFile(path)
	require.NoError(suite.t, err)

	verifyContents(suite.t, data)
}
