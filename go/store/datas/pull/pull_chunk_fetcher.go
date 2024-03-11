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
	GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, nbs.CompressedChunk)) error
}

// A ChunkFetcher is a batching, stateful, potentially concurrent interface to
// fetch lots of chunks from a ChunkStore. A caller is expected to call
// `Get()` and `Recv()` concurrently. Unless there is an error, for every
// single Hash passed to Get, a corresponding Recv() call will deliver the
// contents of the chunk. When a caller is done with a ChunkFetcher, they
// should call |CloseSend()|. After CloseSend, all requested hashes have been
// delivered through Recv(), Recv() will return `io.EOF`.
//
// A ChunkFetcher should be Closed() when it is no longer needed. In non-error
// cases, this will typically be after Recv() has delivererd io.EOF. If Close
// is called before Recv() delivers io.EOF, there is no guarantee that all
// requested chunks will be delivered through Recv().
//
// In contrast to interfaces to like GetManyCompressed on ChunkStore, if the
// chunk is not in the underlying database, then Recv() will return an
// nbs.CompressedChunk with its Hash set, but with empty contents.
//
// Other than an io.EOF from Recv(), any |error| returned from any method
// indicates an underlying problem wtih fetching requested chunks from the
// ChunkStore. A ChunkFetcher is single use and cannot be used effectively
// after an error is returned.
type ChunkFetcher interface {
	Get(ctx context.Context, hashes hash.HashSet) error

	CloseSend() error

	Recv(context.Context) (nbs.CompressedChunk, error)

	Close() error
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
	resCh   chan nbs.CompressedChunk
}

func NewPullChunkFetcher(ctx context.Context, getter GetManyer) *PullChunkFetcher {
	eg, ctx := errgroup.WithContext(ctx)
	ret := &PullChunkFetcher{
		ctx:     ctx,
		eg:      eg,
		getter:  getter,
		batchCh: make(chan hash.HashSet),
		doneCh:  make(chan struct{}),
		resCh:   make(chan nbs.CompressedChunk),
	}
	ret.eg.Go(func() error {
		return ret.fetcherThread(func() {
			close(ret.resCh)
		})
	})
	return ret
}

func (f *PullChunkFetcher) fetcherThread(finalize func()) error {
	for {
		select {
		case batch, ok := <-f.batchCh:
			if !ok {
				finalize()
				return nil
			}

			var mu sync.Mutex
			missing := batch.Copy()

			// Blocking get, no concurrency, only one fetcher.
			err := f.getter.GetManyCompressed(f.ctx, batch, func(ctx context.Context, chk nbs.CompressedChunk) {
				mu.Lock()
				missing.Remove(chk.H)
				mu.Unlock()
				select {
				case <-ctx.Done():
				case <-f.ctx.Done():
				case f.resCh <- chk:
				case <-f.doneCh:
				}
			})
			if err != nil {
				return err
			}

			for h := range missing {
				select {
				case <-f.ctx.Done():
					return context.Cause(f.ctx)
				case f.resCh <- nbs.CompressedChunk{H: h}:
				case <-f.doneCh:
					return nil
				}
			}
		case <-f.ctx.Done():
			return context.Cause(f.ctx)
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

func (f *PullChunkFetcher) Recv(ctx context.Context) (nbs.CompressedChunk, error) {
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
