// Copyright 2019-2022 Dolthub, Inc.
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

package sysbench_runner

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReadResultsCsv(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "TestWriteResultsCsv")
	err = os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	expectedOne := genRandomResult()
	expectedTwo := genRandomResult()

	filename := filepath.Join(tmpDir, fmt.Sprintf("test-results%s", CsvExt))
	err = WriteResultsCsv(filename, []*Result{expectedOne, expectedTwo})
	require.NoError(t, err)

	actual, err := ReadResultsCsv(filename)
	require.NoError(t, err)
	assert.Equal(t, len(actual), 2)
	assert.Equal(t, expectedOne.SqlTotalQueries, actual[0].SqlTotalQueries)
	assert.Equal(t, expectedOne.LatencySumMS, actual[0].LatencySumMS)
	assert.Equal(t, expectedTwo.SqlTotalQueries, actual[1].SqlTotalQueries)
	assert.Equal(t, expectedTwo.LatencySumMS, actual[1].LatencySumMS)
}
