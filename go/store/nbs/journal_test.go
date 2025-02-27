// Copyright 2022 Dolthub, Inc.
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
	"context"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/types"
)

func makeTestChunkJournal(t *testing.T) *ChunkJournal {
	cacheOnce.Do(makeGlobalCaches)
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	t.Cleanup(func() { file.RemoveAll(dir) })
	lock, err := AcquireManifestLock(dir)
	require.NoError(t, err)
	m, err := newJournalManifest(ctx, lock, dir)
	require.NoError(t, err)
	q := NewUnlimitedMemQuotaProvider()
	p := newFSTablePersister(dir, q)
	nbf := types.Format_Default.VersionString()
	j, err := newChunkJournal(ctx, nbf, dir, m, p.(*fsTablePersister))
	require.NoError(t, err)
	t.Cleanup(func() {
		j.Close()
		lock.Unlock()
	})
	return j
}

func TestChunkJournalBlockStoreSuite(t *testing.T) {
	cacheOnce.Do(makeGlobalCaches)
	fn := func(ctx context.Context, dir string) (*NomsBlockStore, error) {
		q := NewUnlimitedMemQuotaProvider()
		nbf := types.Format_Default.VersionString()
		return NewLocalJournalingStore(ctx, nbf, dir, q)
	}
	suite.Run(t, &BlockStoreSuite{
		factory:        fn,
		skipInterloper: true,
	})
}

func TestChunkJournalPersist(t *testing.T) {
	ctx := context.Background()
	j := makeTestChunkJournal(t)
	const iters = 64
	stats := &Stats{}
	haver := emptyChunkSource{}
	for i := 0; i < iters; i++ {
		memTbl, chunkMap := randomMemTable(16)
		source, _, err := j.Persist(ctx, memTbl, haver, nil, stats)
		assert.NoError(t, err)

		for h, ch := range chunkMap {
			ok, _, err := source.has(h, nil)
			assert.NoError(t, err)
			assert.True(t, ok)
			data, _, err := source.get(ctx, h, nil, stats)
			assert.NoError(t, err)
			assert.Equal(t, ch.Data(), data)
		}

		cs, err := j.Open(ctx, source.hash(), 16, stats)
		assert.NotNil(t, cs)
		assert.NoError(t, err)
	}
}

func TestReadRecordRanges(t *testing.T) {
	ctx := context.Background()
	j := makeTestChunkJournal(t)

	var buf []byte
	mt, data := randomMemTable(256)
	gets := make([]getRecord, 0, len(data))
	for h := range data {
		gets = append(gets, getRecord{a: &h, prefix: h.Prefix()})
	}

	jcs, _, err := j.Persist(ctx, mt, emptyChunkSource{}, nil, &Stats{})
	require.NoError(t, err)

	rdr, sz, err := jcs.(journalChunkSource).journal.snapshot(context.Background())
	require.NoError(t, err)
	defer rdr.Close()

	buf = make([]byte, sz)
	n, err := rdr.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, int(sz), n)

	ranges, _, err := jcs.getRecordRanges(ctx, gets, nil)
	require.NoError(t, err)

	for h, rng := range ranges {
		b, _, err := jcs.get(ctx, h, nil, &Stats{})
		assert.NoError(t, err)
		ch1 := chunks.NewChunkWithHash(h, b)
		assert.Equal(t, data[h], ch1)

		start, stop := rng.Offset, uint32(rng.Offset)+rng.Length
		cc2, err := NewCompressedChunk(h, buf[start:stop])
		assert.NoError(t, err)
		ch2, err := cc2.ToChunk()
		assert.NoError(t, err)
		assert.Equal(t, data[h], ch2)
	}
}

func randBuf(n int) (b []byte) {
	b = make([]byte, n)
	rand.Read(b)
	return
}
