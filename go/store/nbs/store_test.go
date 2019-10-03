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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestNBSAsTableFileStore(t *testing.T) {
	ctx := context.Background()
	testDir := filepath.Join(os.TempDir(), uuid.New().String())

	err := os.MkdirAll(testDir, os.ModePerm)
	require.NoError(t, err)

	numTableFiles := 128

	st, err := NewLocalStore(ctx, types.Format_Default.VersionString(), testDir, defaultMemTableSize)
	require.NoError(t, err)

	fileToData := make(map[string][]byte, numTableFiles)
	for i := 0; i < numTableFiles; i++ {
		var chunkData [][]byte
		for j := 0; j < i+1; j++ {
			chunkData = append(chunkData, []byte(fmt.Sprintf("%d:%d", i, j)))
		}
		data, addr, err := buildTable(chunkData)
		fileID := addr.String()
		fileToData[fileID] = data
		err = st.WriteTableFile(ctx, fileID, i+1, bytes.NewReader(data))
		require.NoError(t, err)
	}

	_, sources, err := st.Sources(ctx)
	require.NoError(t, err)

	assert.Equal(t, numTableFiles, len(sources))

	for _, src := range sources {
		fileID := src.FileID()
		expected, ok := fileToData[fileID]
		require.True(t, ok)

		rd, err := src.Open()
		require.NoError(t, err)

		data, err := ioutil.ReadAll(rd)
		require.NoError(t, err)

		err = rd.Close()
		require.NoError(t, err)

		assert.Equal(t, expected, data)
	}
}
