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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestBlockBufferTableSink(t *testing.T) {
	suite.Run(t, &TableSinkSuite{sink: NewBlockBufferTableSink(128)})
}

func TestFixedBufferTableSink(t *testing.T) {
	suite.Run(t, &TableSinkSuite{sink: NewFixedBufferTableSink(make([]byte, 32*1024))})
}

type TableSinkSuite struct {
	sink TableSink
	t *testing.T
}

func (suite *TableSinkSuite) SetT(t *testing.T) {
	suite.t = t
}

func (suite *TableSinkSuite) T() *testing.T {
	return suite.t
}

func (suite *TableSinkSuite) TestWriteAndPos() {
	data := make([]byte, 64)
	for i := 0; i < 64; i++ {
		data[i] = byte(i)
	}

	for i := 0; i < 32; i++ {
		_, err := suite.sink.Write(data)
		assert.NoError(suite.t, err)
	}
}

func (suite *TableSinkSuite) TestCmpChunkTableWriter() {

}
