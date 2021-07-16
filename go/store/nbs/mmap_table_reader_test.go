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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/file"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMmapTableReader(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer file.RemoveAll(dir)

	fc := newFDCache(1)
	defer fc.Drop()

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	tableData, h, err := buildTable(chunks)
	require.NoError(t, err)
	err = ioutil.WriteFile(filepath.Join(dir, h.String()), tableData, 0666)
	require.NoError(t, err)

	trc, err := newMmapTableReader(dir, h, uint32(len(chunks)), nil, fc)
	require.NoError(t, err)
	assertChunksInReader(chunks, trc, assert)
}
