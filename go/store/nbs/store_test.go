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
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

func makeTestLocalStore(t *testing.T, maxTableFiles int) (st *NomsBlockStore, nomsDir string) {
	ctx := context.Background()
	nomsDir = filepath.Join(tempfiles.MovableTempFileProvider.GetTempDir(), "noms_"+uuid.New().String()[:8])

	err := os.MkdirAll(nomsDir, os.ModePerm)
	require.NoError(t, err)

	st, err = newLocalStore(ctx, types.Format_Default.VersionString(), nomsDir, defaultMemTableSize, maxTableFiles)
	require.NoError(t, err)
	return st, nomsDir
}

type fileToData map[string][]byte

func populateLocalStore(t *testing.T, st *NomsBlockStore, numTableFiles int) fileToData {
	ctx := context.Background()
	fileToData := make(fileToData, numTableFiles)
	for i := 0; i < numTableFiles; i++ {
		var chunkData [][]byte
		for j := 0; j < i+1; j++ {
			chunkData = append(chunkData, []byte(fmt.Sprintf("%d:%d", i, j)))
		}
		data, addr, err := buildTable(chunkData)
		require.NoError(t, err)
		fileID := addr.String()
		fileToData[fileID] = data
		err = st.WriteTableFile(ctx, fileID, i+1, bytes.NewReader(data), 0, nil)
		require.NoError(t, err)
	}
	return fileToData
}

func TestNBSAsTableFileStore(t *testing.T) {
	ctx := context.Background()

	numTableFiles := 128
	assert.Greater(t, defaultMaxTables, numTableFiles)
	st, _ := makeTestLocalStore(t, defaultMaxTables)
	fileToData := populateLocalStore(t, st, numTableFiles)

	_, sources, err := st.Sources(ctx)
	require.NoError(t, err)

	assert.Equal(t, numTableFiles, len(sources))

	for _, src := range sources {
		fileID := src.FileID()
		expected, ok := fileToData[fileID]
		require.True(t, ok)

		rd, err := src.Open(context.Background())
		require.NoError(t, err)

		data, err := ioutil.ReadAll(rd)
		require.NoError(t, err)

		err = rd.Close()
		require.NoError(t, err)

		assert.Equal(t, expected, data)
	}

	size, err := st.Size(ctx)
	require.NoError(t, err)
	require.Greater(t, size, uint64(0))
}

type tableFileSet map[string]TableFile

func (s tableFileSet) contains(fileName string) (ok bool) {
	_, ok = s[fileName]
	return ok
}

// findAbsent returns the table file names in |ftd| that don't exist in |s|
func (s tableFileSet) findAbsent(ftd fileToData) (absent []string) {
	for fileID := range ftd {
		if !s.contains(fileID) {
			absent = append(absent, fileID)
		}
	}
	return absent
}

func tableFileSetFromSources(sources []TableFile) (s tableFileSet) {
	s = make(tableFileSet, len(sources))
	for _, src := range sources {
		s[src.FileID()] = src
	}
	return s
}

func TestNBSPruneTableFiles(t *testing.T) {
	ctx := context.Background()

	// over populate table files
	numTableFiles := 64
	maxTableFiles := 16
	st, nomsDir := makeTestLocalStore(t, maxTableFiles)
	fileToData := populateLocalStore(t, st, numTableFiles)

	// add a chunk and flush to trigger a conjoin
	c := []byte("it's a boy!")
	ok := st.addChunk(ctx, computeAddr(c), c)
	require.True(t, ok)
	ok, err := st.Commit(ctx, st.upstream.root, st.upstream.root)
	require.True(t, ok)
	require.NoError(t, err)

	_, sources, err := st.Sources(ctx)
	require.NoError(t, err)
	assert.Greater(t, numTableFiles, len(sources))

	// find which input table files were conjoined
	tfSet := tableFileSetFromSources(sources)
	absent := tfSet.findAbsent(fileToData)
	// assert some input table files were conjoined
	assert.NotEmpty(t, absent)

	currTableFiles := func(dirName string) *set.StrSet {
		infos, err := ioutil.ReadDir(dirName)
		require.NoError(t, err)
		curr := set.NewStrSet(nil)
		for _, fi := range infos {
			if fi.Name() != manifestFileName && fi.Name() != lockFileName {
				curr.Add(fi.Name())
			}
		}
		return curr
	}

	preGC := currTableFiles(nomsDir)
	for _, tf := range sources {
		assert.True(t, preGC.Contains(tf.FileID()))
	}
	for _, fileName := range absent {
		assert.True(t, preGC.Contains(fileName))
	}

	err = st.PruneTableFiles(ctx)
	assert.NoError(t, err)

	postGC := currTableFiles(nomsDir)
	for _, tf := range sources {
		assert.True(t, preGC.Contains(tf.FileID()))
	}
	for _, fileName := range absent {
		assert.False(t, postGC.Contains(fileName))
	}
	infos, err := ioutil.ReadDir(nomsDir)
	require.NoError(t, err)

	// assert that we only have files for current sources,
	// the manifest, and the lock file
	assert.Equal(t, len(sources)+2, len(infos))

	size, err := st.Size(ctx)
	require.NoError(t, err)
	require.Greater(t, size, uint64(0))
}

func makeChunkSet(N, size int) (s map[hash.Hash]chunks.Chunk) {
	bb := make([]byte, size*N)
	rand.Seed(time.Now().UnixNano())
	rand.Read(bb)

	s = make(map[hash.Hash]chunks.Chunk, N)
	offset := 0
	for i := 0; i < N; i++ {
		c := chunks.NewChunk(bb[offset : offset+size])
		s[c.Hash()] = c
		offset += size
	}

	return
}

func TestNBSCopyGC(t *testing.T) {
	ctx := context.Background()
	st, _ := makeTestLocalStore(t, 8)

	keepers := makeChunkSet(64, 64)
	tossers := makeChunkSet(64, 64)

	for _, c := range keepers {
		err := st.Put(ctx, c)
		assert.NoError(t, err)
	}
	for h, c := range keepers {
		out, err := st.Get(ctx, h)
		assert.NoError(t, err)
		assert.Equal(t, c, out)
	}

	for h := range tossers {
		// assert mutually exclusive chunk sets
		c, ok := keepers[h]
		require.False(t, ok)
		assert.Equal(t, chunks.Chunk{}, c)
	}
	for _, c := range tossers {
		err := st.Put(ctx, c)
		assert.NoError(t, err)
	}
	for h, c := range tossers {
		out, err := st.Get(ctx, h)
		assert.NoError(t, err)
		assert.Equal(t, c, out)
	}

	r, err := st.Root(ctx)
	assert.NoError(t, err)

	errChan := make(chan error)
	keepChan := make(chan hash.Hash, 16)
	err = st.MarkAndSweepChunks(ctx, r, keepChan, errChan)
	require.NoError(t, err)

	for h := range keepers {
		keepChan <- h
	}
	close(keepChan)

	select {
	case err, ok := <-errChan:
		assert.False(t, ok)
		assert.Nil(t, err)
	}

	for h, c := range keepers {
		out, err := st.Get(ctx, h)
		require.NoError(t, err)
		assert.Equal(t, c, out)
	}
	for h := range tossers {
		out, err := st.Get(ctx, h)
		require.NoError(t, err)
		assert.Equal(t, chunks.EmptyChunk, out)
	}
}
