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
	"io"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

type GetManyer interface {
	GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, nbs.ToChunker)) error
}

type ChunkFetcherable interface {
	ChunkFetcher(ctx context.Context) nbs.ChunkFetcher
}

func GetChunkFetcher(ctx context.Context, cs GetManyer) nbs.ChunkFetcher {
	if fable, ok := cs.(ChunkFetcherable); ok {
		return fable.ChunkFetcher(ctx)
	}
	return NewPullChunkFetcher(ctx, cs)
}

// A PullChunkFetcher is a simple implementation of |ChunkFetcher| based on
// calling GetManyCompressed.
//
// It only has one outstanding GetManyCompressed call at a time.
type PullChunkFetcher struct {
	ctx context.Context
	eg  *errgroup.Group

	getter GetManyer

	batchCh chan hash.HashSet
	doneCh  chan struct{}
	resCh   chan nbs.ToChunker
}

func NewPullChunkFetcher(ctx context.Context, getter GetManyer) *PullChunkFetcher {
	eg, ctx := errgroup.WithContext(ctx)
	ret := &PullChunkFetcher{
		ctx:     ctx,
		eg:      eg,
		getter:  getter,
		batchCh: make(chan hash.HashSet),
		doneCh:  make(chan struct{}),
		resCh:   make(chan nbs.ToChunker),
	}
	ret.eg.Go(func() error {
		return ret.fetcherThread(ctx)
	})
	return ret
}

func (f *PullChunkFetcher) fetcherThread(ctx context.Context) error {
	for {
		select {
		case batch, ok := <-f.batchCh:
			if !ok {
				close(f.resCh)
				return nil
			}

			var mu sync.Mutex
			missing := batch.Copy()

			// Blocking get, no concurrency, only one fetcher.
			err := f.getter.GetManyCompressed(ctx, batch, func(ctx context.Context, chk nbs.ToChunker) {
				mu.Lock()
				missing.Remove(chk.Hash())
				mu.Unlock()
				select {
				case <-ctx.Done():
				case f.resCh <- chk:
				case <-f.doneCh:
				}
			})
			if err != nil {
				return err
			}

			for h := range missing {
				select {
				case <-ctx.Done():
					return context.Cause(ctx)
				case f.resCh <- nbs.CompressedChunk{H: h}:
				case <-f.doneCh:
					return nil
				}
			}
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-f.doneCh:
			return nil
		}
	}
}

func (f *PullChunkFetcher) Get(ctx context.Context, hashes hash.HashSet) error {
	select {
	case f.batchCh <- hashes:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-f.ctx.Done():
		return context.Cause(f.ctx)
	}
}

func (f *PullChunkFetcher) CloseSend() error {
	close(f.batchCh)
	return nil
}

func (f *PullChunkFetcher) Close() error {
	close(f.doneCh)
	return f.eg.Wait()
}

func (f *PullChunkFetcher) Recv(ctx context.Context) (nbs.ToChunker, error) {
	select {
	case res, ok := <-f.resCh:
		if !ok {
			return nbs.CompressedChunk{}, io.EOF
		}
		return res, nil
	case <-ctx.Done():
		return nbs.CompressedChunk{}, context.Cause(ctx)
	case <-f.ctx.Done():
		return nbs.CompressedChunk{}, context.Cause(f.ctx)
	}
}
