// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

func TestPullChunkFetcher(t *testing.T) {
	t.Run("ImmediateCloseSend", func(t *testing.T) {
		f := NewPullChunkFetcher(emptyGetManyer{})
		assert.NoError(t, f.CloseSend())
		_, err := f.Recv(context.Background())
		assert.ErrorIs(t, err, io.EOF)
		assert.NoError(t, f.Close())
	})
	t.Run("CanceledGetCtx", func(t *testing.T) {
		gm := blockingGetManyer{make(chan struct{})}
		f := NewPullChunkFetcher(gm)
		ctx, c := context.WithCancel(context.Background())
		hs := make(hash.HashSet)
		var h hash.Hash
		hs.Insert(h)
		c()
		err := f.Get(ctx, hs)
		assert.Error(t, err)
		assert.NoError(t, f.Close())
	})
	t.Run("CanceledRecvCtx", func(t *testing.T) {
		ctx, c := context.WithCancel(context.Background())
		f := NewPullChunkFetcher(emptyGetManyer{})
		c()
		_, err := f.Recv(ctx)
		assert.Error(t, err)
		assert.NoError(t, f.Close())
	})
	t.Run("ReturnsDelieveredChunk", func(t *testing.T) {
		var gm deliveringGetManyer
		gm.C.FullCompressedChunk = make([]byte, 1024)
		f := NewPullChunkFetcher(gm)
		hs := make(hash.HashSet)
		hs.Insert(gm.C.H)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmp, err := f.Recv(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, cmp.Hash(), gm.C.H)

			cc, ok := cmp.(nbs.CompressedChunk)
			assert.True(t, ok)

			assert.Equal(t, cc.FullCompressedChunk, gm.C.FullCompressedChunk)
			_, err = f.Recv(context.Background())
			assert.ErrorIs(t, err, io.EOF)
			assert.NoError(t, f.Close())
		}()
		err := f.Get(context.Background(), hs)
		assert.NoError(t, err)
		assert.NoError(t, f.CloseSend())
		wg.Wait()
	})
	t.Run("ReturnsEmptyCompressedChunk", func(t *testing.T) {
		f := NewPullChunkFetcher(emptyGetManyer{})
		hs := make(hash.HashSet)
		var h hash.Hash
		hs.Insert(h)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmp, err := f.Recv(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, cmp.Hash(), h)

			cc, ok := cmp.(nbs.CompressedChunk)
			assert.True(t, ok)
			assert.Nil(t, cc.FullCompressedChunk)
			_, err = f.Recv(context.Background())
			assert.ErrorIs(t, err, io.EOF)
			assert.NoError(t, f.Close())
		}()
		err := f.Get(context.Background(), hs)
		assert.NoError(t, err)
		assert.NoError(t, f.CloseSend())
		wg.Wait()
	})
	t.Run("ErrorGetManyer", func(t *testing.T) {
		f := NewPullChunkFetcher(errorGetManyer{})
		hs := make(hash.HashSet)
		var h hash.Hash
		hs.Insert(h)
		var wg sync.WaitGroup
		wg.Add(2)
		var werr, rerr error
		go func() {
			defer wg.Done()
			_, rerr = f.Recv(context.Background())
			if rerr == io.EOF {
				rerr = nil
			}
		}()
		go func() {
			defer wg.Done()
			defer f.CloseSend()
			werr = f.Get(context.Background(), hs)
		}()
		wg.Wait()
		// Either the Recv or the Get should have seen
		// the error from GetManyComprsseed.
		assert.Error(t, errors.Join(rerr, werr))
		assert.NoError(t, f.Close())
	})
	t.Run("ClosedFetcherErrorsGet", func(t *testing.T) {
		f := NewPullChunkFetcher(emptyGetManyer{})
		assert.NoError(t, f.Close())
		hs := make(hash.HashSet)
		var h hash.Hash
		hs.Insert(h)
		assert.Error(t, f.Get(context.Background(), hs))
	})
}

type emptyGetManyer struct {
}

func (emptyGetManyer) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, nbs.ToChunker)) error {
	return nil
}

type deliveringGetManyer struct {
	C nbs.CompressedChunk
}

func (d deliveringGetManyer) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, nbs.ToChunker)) error {
	for _ = range hashes {
		found(ctx, d.C)
	}
	return nil
}

type blockingGetManyer struct {
	block chan struct{}
}

func (b blockingGetManyer) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, nbs.ToChunker)) error {
	select {
	case <-b.block:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

type errorGetManyer struct {
}

var getManyerErr = fmt.Errorf("always return an error")

func (errorGetManyer) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, nbs.ToChunker)) error {
	return getManyerErr
}

func TestPop(t *testing.T) {
	var backing [16]*int
	for i := range 16 {
		backing[i] = new(int)
		*backing[i] = i
	}
	// s is pointers of [0, 1, 2, ..., 16]
	s := backing[:]
	assert.Len(t, s, 16)
	// pop continuously and assert that we see
	// the sequence 0, 1, 2, ..., 16 and each
	// prior element at the end of the list is
	// |nil|.
	for i := range 16 {
		var p *int
		p, s = pop(s)
		assert.Len(t, s, 16-i-1)
		assert.Equal(t, i, *p)
		// One off the end of the new |s| is now nil.
		assert.Nil(t, backing[16-i-1], "i is %d", i)
	}
	// pop has a precondition of len > 0. This should panic.
	assert.Panics(t, func() {
		pop(backing[:0])
	})
}

func TestAppendAbsent(t *testing.T) {
	var absent []hash.HashSet
	var hashes [16]hash.Hash
	for i := range 16 {
		hashes[i][0] = byte(i)
	}
	// Initial set is the full batch.
	absent = appendAbsent(absent, hash.NewHashSet(hashes[:]...), 4)
	assert.Len(t, absent, 1)
	assert.Len(t, absent[0], 16)
	// Next set get batched up.
	absent = appendAbsent(absent, hash.NewHashSet(hashes[:]...), 4)
	assert.Len(t, absent, 5)
	for i := range 4 {
		assert.Len(t, absent[i+1], 4)
	}
}
