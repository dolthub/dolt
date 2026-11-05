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

package nbs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/hash"
)

// While getManyCompressed callbacks are running, journal writes must not
// block, and Close must wait for the outstanding reads to finish.
func TestJournalGetManyCompressedCallbacksDoNotHoldLock(t *testing.T) {
	ctx := context.Background()
	j := newTestJournalWriter(t, newTestFilePath(t))

	flushed := randomCompressedChunks(32)
	for _, cc := range flushed {
		require.NoError(t, j.writeCompressedChunk(ctx, dherrors.FatalBehaviorError, cc))
	}
	require.NoError(t, j.flush(ctx, dherrors.FatalBehaviorError))
	buffered := randomCompressedChunks(8)
	for _, cc := range buffered {
		require.NoError(t, j.writeCompressedChunk(ctx, dherrors.FatalBehaviorError, cc))
	}

	expected := make(map[hash.Hash]CompressedChunk)
	hashes := hash.NewHashSet()
	for h, cc := range flushed {
		r, ok := j.ranges.get(h)
		require.True(t, ok)
		require.LessOrEqual(t, int64(r.Offset)+int64(r.Length), j.off)
		expected[h] = cc
		hashes.Insert(h)
	}
	for h, cc := range buffered {
		r, ok := j.ranges.get(h)
		require.True(t, ok)
		require.Greater(t, int64(r.Offset)+int64(r.Length), j.off)
		expected[h] = cc
		hashes.Insert(h)
	}

	var mu sync.Mutex
	got := make(map[hash.Hash]CompressedChunk)
	entered := make(chan struct{}, len(expected))
	release := make(chan struct{})
	found := func(ctx context.Context, tc ToChunker) {
		entered <- struct{}{}
		<-release
		mu.Lock()
		defer mu.Unlock()
		got[tc.Hash()] = tc.(CompressedChunk)
	}

	source := journalChunkSource{journal: j}
	readDone := make(chan error, 1)
	go func() {
		eg, ctx := errgroup.WithContext(ctx)
		eg.SetLimit(2)
		_, _, err := source.getManyCompressed(ctx, eg, toGetRecords(hashes), found, nil, &Stats{})
		readDone <- errors.Join(err, eg.Wait())
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no getManyCompressed callback started")
	}

	writeDone := make(chan error, 1)
	go func() {
		extra := randomCompressedChunks(1)
		for _, cc := range extra {
			writeDone <- j.writeCompressedChunk(ctx, dherrors.FatalBehaviorError, cc)
		}
	}()
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("journal write blocked behind getManyCompressed callbacks")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- j.Close()
	}()
	select {
	case <-closeDone:
		t.Fatal("Close returned while reads were outstanding")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("getManyCompressed did not finish")
	}
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish after reads drained")
	}

	require.Len(t, got, len(expected))
	for h, cc := range expected {
		assert.Equal(t, cc.FullCompressedChunk, got[h].FullCompressedChunk)
	}
}

func TestJournalGetManyCompressedAfterClose(t *testing.T) {
	ctx := context.Background()
	j := newTestJournalWriter(t, newTestFilePath(t))
	data := randomCompressedChunks(4)
	hashes := hash.NewHashSet()
	for h, cc := range data {
		require.NoError(t, j.writeCompressedChunk(ctx, dherrors.FatalBehaviorError, cc))
		hashes.Insert(h)
	}
	require.NoError(t, j.Close())

	source := journalChunkSource{journal: j}
	var eg errgroup.Group
	_, _, err := source.getManyCompressed(ctx, &eg, toGetRecords(hashes), func(context.Context, ToChunker) {}, nil, &Stats{})
	require.ErrorIs(t, err, errJournalWriterClosed)
	require.NoError(t, eg.Wait())
}
