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
	"sync/atomic"
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
	writers, journals, fetches atomic.Int32
	fetchedErr                 error
}

func (f *toyTableFiles) newWriter(tmpDir string, archive bool) (nbs.GenericTableWriter, error) {
	f.writers.Add(1)
	return f.plainTableFiles.newWriter(tmpDir, archive)
}

func (f *toyTableFiles) visitJournal(ctx context.Context, r io.Reader, off int64, cb func(nbs.ToChunker) error) error {
	f.journals.Add(1)
	return f.plainTableFiles.visitJournal(ctx, r, off, cb)
}

func (f *toyTableFiles) fetched(nbs.ToChunker) error {
	f.fetches.Add(1)
	return f.fetchedErr
}

func TestTableFileFormat(t *testing.T) {
	ctx := context.Background()
	src, err := nbs.NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), nbs.NewUnlimitedMemQuotaProvider(), false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { src.Close() })
	ns := tree.NewNodeStore(src)
	db := datas.NewDatabase(src)
	m := createProllyMap(t, ctx, ns, []testTuple{{0, "a", "b"}, {1, "c", "d"}})
	ds, err := db.GetDataset(ctx, "ds")
	require.NoError(t, err)
	epoch := datas.CommitDateAt(time.UnixMilli(0))
	ds, err = db.Commit(ctx, ds, buildRootValue(buildAddressMap(t, ctx, ns, map[string]prolly.Map{"t": m})), datas.CommitOptions{Meta: &datas.CommitMeta{Author: datas.CommitIdent{Date: epoch}, Committer: datas.CommitIdent{Date: epoch}}})
	require.NoError(t, err)
	root, ok := ds.MaybeHeadAddr()
	require.True(t, ok)

	var stores []chunks.ChunkStore
	var format tableFileFormat
	var formatErr error
	prev := newTableFileFormat
	t.Cleanup(func() { newTableFileFormat = prev })
	newTableFileFormat = func(src, sink chunks.ChunkStore) (tableFileFormat, error) {
		stores = append(stores, src, sink)
		return format, formatErr
	}

	newSink := func(t *testing.T) chunks.ChunkStore {
		sink, err := nbs.NewLocalStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), 1<<20, nbs.NewUnlimitedMemQuotaProvider(), false)
		require.NoError(t, err)
		t.Cleanup(func() { sink.Close() })
		return sink
	}
	pull := func(sink chunks.ChunkStore) error {
		waf, err := types.WalkAddrsForChunkStore(src)
		require.NoError(t, err)
		statsCh := make(chan Stats)
		go func() {
			for range statsCh {
			}
		}()
		defer close(statsCh)
		plr, err := NewPuller(ctx, t.TempDir(), 1<<20, src, sink, waf, []hash.Hash{root}, statsCh)
		if err != nil {
			return err
		}
		return plr.Pull(ctx)
	}
	clone := func(sink chunks.ChunkStore) error {
		return Clone(ctx, src, sink, func(c chunks.Chunk) chunks.InsertAddrsCb {
			return func(ctx context.Context, addrs hash.HashSet, _ chunks.PendingRefExists) error {
				return types.InsertAddrsFromNomsValue(c, types.Format_DOLT, addrs)
			}
		}, t.TempDir(), nil)
	}

	for _, tc := range []struct {
		name    string
		copy    func(chunks.ChunkStore) error
		fetches bool
	}{
		{"Pull", pull, true},
		{"Clone", clone, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &toyTableFiles{}
			format, formatErr, stores = f, nil, nil
			sink := newSink(t)
			require.NoError(t, tc.copy(sink))
			assert.Equal(t, []chunks.ChunkStore{src, sink}, stores)
			assert.NotZero(t, f.writers.Load())
			if tc.fetches {
				assert.NotZero(t, f.fetches.Load())
			} else {
				assert.Equal(t, int32(1), f.journals.Load(), "the journal is converted")
			}
			require.NoError(t, sink.Rebase(ctx))
			present, err := sink.Has(ctx, root)
			require.NoError(t, err)
			assert.True(t, present)

			errToy := errors.New("toy")
			format, formatErr = nil, errToy
			assert.ErrorIs(t, tc.copy(newSink(t)), errToy)
			if tc.fetches {
				format, formatErr = &toyTableFiles{fetchedErr: errToy}, nil
				assert.ErrorIs(t, tc.copy(newSink(t)), errToy)
			}
		})
	}
}
