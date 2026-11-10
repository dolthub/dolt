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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

type recordingJournalCopier struct {
	archiveJournalCopier
	calls *[]string
}

func (c recordingJournalCopier) newWriter(tmpDir string) (*nbs.ArchiveStreamWriter, error) {
	*c.calls = append(*c.calls, "newWriter")
	return c.archiveJournalCopier.newWriter(tmpDir)
}

func (c recordingJournalCopier) visit(ctx context.Context, r io.Reader, off int64, cb func(nbs.ToChunker) error) error {
	*c.calls = append(*c.calls, "visit")
	return c.archiveJournalCopier.visit(ctx, r, off, cb)
}

func TestJournalCopierIsReached(t *testing.T) {
	ctx := context.Background()
	q := nbs.NewUnlimitedMemQuotaProvider()
	src, err := nbs.NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), q, false, nil)
	require.NoError(t, err)
	defer src.Close()
	sink, err := nbs.NewLocalStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), 1<<20, q, false)
	require.NoError(t, err)
	defer sink.Close()
	noAddrs := func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	}
	c := chunks.NewChunk([]byte("journaled"))
	require.NoError(t, src.Put(ctx, c, noAddrs))
	_, err = src.Commit(ctx, c.Hash(), hash.Hash{})
	require.NoError(t, err)

	saved := newJournalCopier
	t.Cleanup(func() { newJournalCopier = saved })
	var calls []string
	var stores []chunks.ChunkStore
	newJournalCopier = func(src, sink chunks.ChunkStore) (journalCopier, error) {
		stores = append(stores, src, sink)
		return recordingJournalCopier{calls: &calls}, nil
	}

	require.NoError(t, Clone(ctx, src, sink, noAddrs, t.TempDir(), nil))
	require.Equal(t, []chunks.ChunkStore{src, sink}, stores)
	require.Equal(t, []string{"newWriter", "visit"}, calls)
	has, err := sink.Has(ctx, c.Hash())
	require.NoError(t, err)
	require.True(t, has)
}
