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
		f := NewPullChunkFetcher(context.Background(), emptyGetManyer{})
		assert.NoError(t, f.CloseSend())
		_, err := f.Recv(context.Background())
		assert.ErrorIs(t, err, io.EOF)
		assert.NoError(t, f.Close())
	})
	t.Run("CanceledGetCtx", func(t *testing.T) {
		ctx, c := context.WithCancel(context.Background())
		gm := blockingGetManyer{make(chan struct{})}
		f := NewPullChunkFetcher(context.Background(), gm)
		hs := make(hash.HashSet)
		var h hash.Hash
		hs.Insert(h)
		err := f.Get(ctx, hs)
		assert.NoError(t, err)
		c()
		err = f.Get(ctx, hs)
		assert.Error(t, err)
		close(gm.block)
		assert.NoError(t, f.Close())
	})
	t.Run("CanceledRecvCtx", func(t *testing.T) {
		ctx, c := context.WithCancel(context.Background())
		f := NewPullChunkFetcher(context.Background(), emptyGetManyer{})
		c()
		_, err := f.Recv(ctx)
		assert.Error(t, err)
		assert.NoError(t, f.Close())
	})
	t.Run("ReturnsDelieveredChunk", func(t *testing.T) {
		var gm deliveringGetManyer
		gm.C.FullCompressedChunk = make([]byte, 1024)
		f := NewPullChunkFetcher(context.Background(), gm)
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
		f := NewPullChunkFetcher(context.Background(), emptyGetManyer{})
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
		f := NewPullChunkFetcher(context.Background(), errorGetManyer{})
		hs := make(hash.HashSet)
		var h hash.Hash
		hs.Insert(h)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.Recv(context.Background())
			assert.Error(t, err)
			err = f.Close()
			assert.Error(t, err)
		}()
		err := f.Get(context.Background(), hs)
		assert.NoError(t, err)
		err = f.Get(context.Background(), hs)
		assert.Error(t, err)
		wg.Wait()
	})
	t.Run("ClosedFetcherErrorsGet", func(t *testing.T) {
		f := NewPullChunkFetcher(context.Background(), emptyGetManyer{})
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
	<-b.block
	return nil
}

type errorGetManyer struct {
}

var getManyerErr = fmt.Errorf("always return an error")

func (errorGetManyer) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, nbs.ToChunker)) error {
	return getManyerErr
}
