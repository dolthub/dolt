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
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
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

	// create a v5 manifest
	_, err = fileManifestV5{nomsDir}.Update(ctx, addr{}, manifestContents{}, &Stats{}, nil)
	require.NoError(t, err)

	st, err = newLocalStore(ctx, types.Format_Default.VersionString(), nomsDir, defaultMemTableSize, maxTableFiles)
	require.NoError(t, err)
	return st, nomsDir
}

type fileToData map[string][]byte

func populateLocalStore(t *testing.T, st *NomsBlockStore, numTableFiles int) fileToData {
	ctx := context.Background()
	fileToData := make(fileToData, numTableFiles)
	fileIDToNumChunks := make(map[string]int)
	for i := 0; i < numTableFiles; i++ {
		var chunkData [][]byte
		for j := 0; j < i+1; j++ {
			chunkData = append(chunkData, []byte(fmt.Sprintf("%d:%d", i, j)))
		}
		data, addr, err := buildTable(chunkData)
		require.NoError(t, err)
		fileID := addr.String()
		fileToData[fileID] = data
		fileIDToNumChunks[fileID] = i + 1
		err = st.WriteTableFile(ctx, fileID, i+1, bytes.NewReader(data), 0, nil)
		require.NoError(t, err)
	}
	err := st.AddTableFilesToManifest(ctx, fileIDToNumChunks)
	require.NoError(t, err)

	return fileToData
}

func TestNBSAsTableFileStore(t *testing.T) {
	ctx := context.Background()

	numTableFiles := 128
	assert.Greater(t, defaultMaxTables, numTableFiles)
	st, _ := makeTestLocalStore(t, defaultMaxTables)
	fileToData := populateLocalStore(t, st, numTableFiles)

	_, sources, _, err := st.Sources(ctx)
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

	_, sources, _, err := st.Sources(ctx)
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
	require.NoError(t, err)

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
	time.Sleep(10)
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
		require.NoError(t, err)
	}
	for h, c := range keepers {
		out, err := st.Get(ctx, h)
		require.NoError(t, err)
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
		require.NoError(t, err)
	}
	for h, c := range tossers {
		out, err := st.Get(ctx, h)
		require.NoError(t, err)
		assert.Equal(t, c, out)
	}

	r, err := st.Root(ctx)
	require.NoError(t, err)

	keepChan := make(chan []hash.Hash, 16)
	var msErr error
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		msErr = st.MarkAndSweepChunks(ctx, r, keepChan)
		wg.Done()
	}()
	for h := range keepers {
		keepChan <- []hash.Hash{h}
	}
	close(keepChan)
	wg.Wait()
	require.NoError(t, msErr)

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

func persistTableFileSources(t *testing.T, p tablePersister, numTableFiles int) (map[hash.Hash]uint32, []hash.Hash) {
	tableFileMap := make(map[hash.Hash]uint32, numTableFiles)
	mapIds := make([]hash.Hash, numTableFiles)

	for i := 0; i < numTableFiles; i++ {
		var chunkData [][]byte
		for j := 0; j < i+1; j++ {
			chunkData = append(chunkData, []byte(fmt.Sprintf("%d:%d", i, j)))
		}
		_, addr, err := buildTable(chunkData)
		require.NoError(t, err)
		fileIDHash, ok := hash.MaybeParse(addr.String())
		require.True(t, ok)
		tableFileMap[fileIDHash] = uint32(i + 1)
		mapIds[i] = fileIDHash
		_, err = p.Persist(context.Background(), createMemTable(chunkData), nil, &Stats{})
		require.NoError(t, err)
	}
	return tableFileMap, mapIds
}

func prepStore(ctx context.Context, t *testing.T, assert *assert.Assertions) (*fakeManifest, tablePersister, *NomsBlockStore, *Stats, chunks.Chunk) {
	fm, p, store := makeStoreWithFakes(t)
	h, err := store.Root(ctx)
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)

	rootChunk := chunks.NewChunk([]byte("root"))
	rootHash := rootChunk.Hash()
	err = store.Put(ctx, rootChunk)
	require.NoError(t, err)
	success, err := store.Commit(ctx, rootHash, hash.Hash{})
	require.NoError(t, err)
	if assert.True(success) {
		has, err := store.Has(ctx, rootHash)
		require.NoError(t, err)
		assert.True(has)
		h, err := store.Root(ctx)
		require.NoError(t, err)
		assert.Equal(rootHash, h)
	}

	stats := &Stats{}

	_, upstream, err := fm.ParseIfExists(ctx, stats, nil)
	require.NoError(t, err)
	// expect single spec for initial commit
	assert.Equal(1, upstream.NumTableSpecs())
	// Start with no appendixes
	assert.Equal(0, upstream.NumAppendixSpecs())
	return fm, p, store, stats, rootChunk
}

func TestNBSUpdateManifestWithAppendixOptions(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	_, p, store, _, _ := prepStore(ctx, t, assert)
	defer store.Close()

	// persist tablefiles to tablePersister
	appendixUpdates, appendixIds := persistTableFileSources(t, p, 4)

	tests := []struct {
		description                   string
		option                        ManifestAppendixOption
		appendixSpecIds               []hash.Hash
		expectedNumberOfSpecs         int
		expectedNumberOfAppendixSpecs int
		expectedError                 error
	}{
		{
			description:     "should error on unsupported appendix option",
			appendixSpecIds: appendixIds[:1],
			expectedError:   ErrUnsupportedManifestAppendixOption,
		},
		{
			description:                   "should append to appendix",
			option:                        ManifestAppendixOption_Append,
			appendixSpecIds:               appendixIds[:2],
			expectedNumberOfSpecs:         3,
			expectedNumberOfAppendixSpecs: 2,
		},
		{
			description:                   "should replace appendix",
			option:                        ManifestAppendixOption_Set,
			appendixSpecIds:               appendixIds[3:],
			expectedNumberOfSpecs:         2,
			expectedNumberOfAppendixSpecs: 1,
		},
		{
			description:                   "should set appendix to nil",
			option:                        ManifestAppendixOption_Set,
			appendixSpecIds:               []hash.Hash{},
			expectedNumberOfSpecs:         1,
			expectedNumberOfAppendixSpecs: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			updates := make(map[hash.Hash]uint32)
			for _, id := range test.appendixSpecIds {
				updates[id] = appendixUpdates[id]
			}

			if test.expectedError == nil {
				info, err := store.UpdateManifestWithAppendix(ctx, updates, test.option)
				require.NoError(t, err)
				assert.Equal(test.expectedNumberOfSpecs, info.NumTableSpecs())
				assert.Equal(test.expectedNumberOfAppendixSpecs, info.NumAppendixSpecs())
			} else {
				_, err := store.UpdateManifestWithAppendix(ctx, updates, test.option)
				assert.Equal(test.expectedError, err)
			}
		})
	}
}

func TestNBSUpdateManifestWithAppendix(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	fm, p, store, stats, _ := prepStore(ctx, t, assert)
	defer store.Close()

	_, upstream, err := fm.ParseIfExists(ctx, stats, nil)
	require.NoError(t, err)

	// persist tablefile to tablePersister
	appendixUpdates, appendixIds := persistTableFileSources(t, p, 1)

	// Ensure appendix (and specs) are updated
	appendixFileId := appendixIds[0]
	updates := map[hash.Hash]uint32{appendixFileId: appendixUpdates[appendixFileId]}
	newContents, err := store.UpdateManifestWithAppendix(ctx, updates, ManifestAppendixOption_Append)
	require.NoError(t, err)
	assert.Equal(upstream.NumTableSpecs()+1, newContents.NumTableSpecs())
	assert.Equal(1, newContents.NumAppendixSpecs())
	assert.Equal(newContents.GetTableSpecInfo(0), newContents.GetAppendixTableSpecInfo(0))
}

func TestNBSUpdateManifestRetainsAppendix(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	fm, p, store, stats, _ := prepStore(ctx, t, assert)
	defer store.Close()

	_, upstream, err := fm.ParseIfExists(ctx, stats, nil)
	require.NoError(t, err)

	// persist tablefile to tablePersister
	specUpdates, specIds := persistTableFileSources(t, p, 3)

	// Update the manifest
	firstSpecId := specIds[0]
	newContents, err := store.UpdateManifest(ctx, map[hash.Hash]uint32{firstSpecId: specUpdates[firstSpecId]})
	require.NoError(t, err)
	assert.Equal(1+upstream.NumTableSpecs(), newContents.NumTableSpecs())
	assert.Equal(0, upstream.NumAppendixSpecs())

	_, upstream, err = fm.ParseIfExists(ctx, stats, nil)
	require.NoError(t, err)

	// Update the appendix
	appendixSpecId := specIds[1]
	updates := map[hash.Hash]uint32{appendixSpecId: specUpdates[appendixSpecId]}
	newContents, err = store.UpdateManifestWithAppendix(ctx, updates, ManifestAppendixOption_Append)
	require.NoError(t, err)
	assert.Equal(1+upstream.NumTableSpecs(), newContents.NumTableSpecs())
	assert.Equal(1+upstream.NumAppendixSpecs(), newContents.NumAppendixSpecs())
	assert.Equal(newContents.GetAppendixTableSpecInfo(0), newContents.GetTableSpecInfo(0))

	_, upstream, err = fm.ParseIfExists(ctx, stats, nil)
	require.NoError(t, err)

	// Update the manifest again to show
	// it successfully retains the appendix
	// and the appendix specs are properly prepended
	// to the |manifestContents.specs|
	secondSpecId := specIds[2]
	newContents, err = store.UpdateManifest(ctx, map[hash.Hash]uint32{secondSpecId: specUpdates[secondSpecId]})
	require.NoError(t, err)
	assert.Equal(1+upstream.NumTableSpecs(), newContents.NumTableSpecs())
	assert.Equal(upstream.NumAppendixSpecs(), newContents.NumAppendixSpecs())
	assert.Equal(newContents.GetAppendixTableSpecInfo(0), newContents.GetTableSpecInfo(0))
}

func TestNBSCommitRetainsAppendix(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	fm, p, store, stats, rootChunk := prepStore(ctx, t, assert)
	defer store.Close()

	_, upstream, err := fm.ParseIfExists(ctx, stats, nil)
	require.NoError(t, err)

	// persist tablefile to tablePersister
	appendixUpdates, appendixIds := persistTableFileSources(t, p, 1)

	// Update the appendix
	appendixFileId := appendixIds[0]
	updates := map[hash.Hash]uint32{appendixFileId: appendixUpdates[appendixFileId]}
	newContents, err := store.UpdateManifestWithAppendix(ctx, updates, ManifestAppendixOption_Append)
	require.NoError(t, err)
	assert.Equal(1+upstream.NumTableSpecs(), newContents.NumTableSpecs())
	assert.Equal(1, newContents.NumAppendixSpecs())

	_, upstream, err = fm.ParseIfExists(ctx, stats, nil)
	require.NoError(t, err)

	// Make second Commit
	secondRootChunk := chunks.NewChunk([]byte("newer root"))
	secondRoot := secondRootChunk.Hash()
	err = store.Put(ctx, secondRootChunk)
	require.NoError(t, err)
	success, err := store.Commit(ctx, secondRoot, rootChunk.Hash())
	require.NoError(t, err)
	if assert.True(success) {
		h, err := store.Root(ctx)
		require.NoError(t, err)
		assert.Equal(secondRoot, h)
		has, err := store.Has(context.Background(), rootChunk.Hash())
		require.NoError(t, err)
		assert.True(has)
		has, err = store.Has(context.Background(), secondRoot)
		require.NoError(t, err)
		assert.True(has)
	}

	// Ensure commit did not blow away appendix
	_, newUpstream, err := fm.ParseIfExists(ctx, stats, nil)
	require.NoError(t, err)
	assert.Equal(1+upstream.NumTableSpecs(), newUpstream.NumTableSpecs())
	assert.Equal(upstream.NumAppendixSpecs(), newUpstream.NumAppendixSpecs())
	assert.Equal(upstream.GetAppendixTableSpecInfo(0), newUpstream.GetTableSpecInfo(0))
	assert.Equal(newUpstream.GetTableSpecInfo(0), newUpstream.GetAppendixTableSpecInfo(0))
}
