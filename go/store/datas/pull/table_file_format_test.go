// Copyright 2026 Dolthub, Inc.
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

package pull

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// toyTableFiles writes plain table files, counting what clone and pull ask of it.
type toyTableFiles struct {
	plainTableFiles
	mu           sync.Mutex
	writers      []bool
	journals     int
	fetchedCount int
	fetchedErr   error
}

func (f *toyTableFiles) newWriter(tmpDir string, archive bool) (nbs.GenericTableWriter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writers = append(f.writers, archive)
	return f.plainTableFiles.newWriter(tmpDir, archive)
}

func (f *toyTableFiles) visitJournal(ctx context.Context, r io.Reader, off int64, cb func(nbs.ToChunker) error) error {
	f.mu.Lock()
	f.journals++
	f.mu.Unlock()
	return f.plainTableFiles.visitJournal(ctx, r, off, cb)
}

func (f *toyTableFiles) fetched(nbs.ToChunker) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetchedCount++
	return f.fetchedErr
}

// withTableFiles makes clone and pull copy chunks with |f|, or fail with |err|, and records the stores they copy.
func withTableFiles(t *testing.T, f tableFileFormat, err error) *[]chunks.ChunkStore {
	prev := newTableFileFormat
	t.Cleanup(func() { newTableFileFormat = prev })
	var stores []chunks.ChunkStore
	newTableFileFormat = func(src, sink chunks.ChunkStore) (tableFileFormat, error) {
		stores = append(stores, src, sink)
		return f, err
	}
	return &stores
}

// testSource returns a journaling store holding a commit, and the commit's address.
func testSource(t *testing.T) (chunks.ChunkStore, hash.Hash) {
	ctx := context.Background()
	st, err := nbs.NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), nbs.NewUnlimitedMemQuotaProvider(), false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	ns := tree.NewNodeStore(st)
	db := datas.NewDatabase(st)
	m := createProllyMap(t, ctx, ns, []testTuple{{0, "a", "b"}, {1, "c", "d"}, {2, "e", "f"}})
	am := buildAddressMap(t, ctx, ns, map[string]prolly.Map{"t": m})
	ds, err := db.GetDataset(ctx, "ds")
	require.NoError(t, err)
	epoch := datas.CommitDateAt(time.UnixMilli(0))
	ds, err = db.Commit(ctx, ds, buildRootValue(am), datas.CommitOptions{Meta: &datas.CommitMeta{Author: datas.CommitIdent{Date: epoch}, Committer: datas.CommitIdent{Date: epoch}}})
	require.NoError(t, err)
	addr, ok := ds.MaybeHeadAddr()
	require.True(t, ok)
	return st, addr
}

func testSink(t *testing.T) chunks.ChunkStore {
	st, err := nbs.NewLocalStore(context.Background(), types.Format_DOLT.VersionString(), t.TempDir(), 1<<20, nbs.NewUnlimitedMemQuotaProvider(), false)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	return st
}

func testPull(t *testing.T, src, sink chunks.ChunkStore, root hash.Hash) (*Puller, error) {
	waf, err := types.WalkAddrsForChunkStore(src)
	require.NoError(t, err)
	statsCh := make(chan Stats)
	go func() {
		for range statsCh {
		}
	}()
	t.Cleanup(func() { close(statsCh) })
	return NewPuller(context.Background(), t.TempDir(), 1<<20, src, sink, waf, []hash.Hash{root}, statsCh)
}

func TestPullerUsesTableFileFormat(t *testing.T) {
	f := &toyTableFiles{}
	stores := withTableFiles(t, f, nil)
	src, root := testSource(t)
	sink := testSink(t)

	plr, err := testPull(t, src, sink, root)
	require.NoError(t, err)
	require.NoError(t, plr.Pull(context.Background()))
	assert.Equal(t, []chunks.ChunkStore{src, sink}, *stores)
	assert.NotEmpty(t, f.writers)
	assert.NotZero(t, f.fetchedCount)
	present, err := sink.Has(context.Background(), root)
	require.NoError(t, err)
	assert.True(t, present)

	errToy := errors.New("toy")
	withTableFiles(t, nil, errToy)
	_, err = testPull(t, src, testSink(t), root)
	assert.ErrorIs(t, err, errToy)

	withTableFiles(t, &toyTableFiles{fetchedErr: errToy}, nil)
	plr, err = testPull(t, src, testSink(t), root)
	require.NoError(t, err)
	assert.ErrorIs(t, plr.Pull(context.Background()), errToy)
}

func TestCloneUsesTableFileFormat(t *testing.T) {
	ctx := context.Background()
	src, root := testSource(t)
	getAddrs := func(c chunks.Chunk) chunks.InsertAddrsCb {
		return func(ctx context.Context, addrs hash.HashSet, _ chunks.PendingRefExists) error {
			return types.InsertAddrsFromNomsValue(c, types.Format_DOLT, addrs)
		}
	}

	f := &toyTableFiles{}
	stores := withTableFiles(t, f, nil)
	sink := testSink(t)
	require.NoError(t, Clone(ctx, src, sink, getAddrs, t.TempDir(), nil))
	assert.Equal(t, []chunks.ChunkStore{src, sink}, *stores)
	assert.Equal(t, []bool{true}, f.writers, "the journal is converted to an archive")
	assert.Equal(t, 1, f.journals)
	require.NoError(t, sink.Rebase(ctx))
	present, err := sink.Has(ctx, root)
	require.NoError(t, err)
	assert.True(t, present)

	errToy := errors.New("toy")
	withTableFiles(t, nil, errToy)
	assert.ErrorIs(t, Clone(ctx, src, testSink(t), getAddrs, t.TempDir(), nil), errToy)
}
