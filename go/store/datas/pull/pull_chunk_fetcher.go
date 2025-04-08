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
	"io"
	"sync"

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
	return NewPullChunkFetcher(cs)
}

// A PullChunkFetcher is a simple implementation of |ChunkFetcher| based on
// calling GetManyCompressed.
//
// It only has one outstanding GetManyCompressed call at a time.
type PullChunkFetcher struct {
	getter   GetManyer
	resCh    chan nbs.ToChunker
	closedCh chan struct{}
}

func NewPullChunkFetcher(getter GetManyer) *PullChunkFetcher {
	ret := &PullChunkFetcher{
		getter:   getter,
		resCh:    make(chan nbs.ToChunker),
		closedCh: make(chan struct{}),
	}
	return ret
}

func (f *PullChunkFetcher) Get(ctx context.Context, hashes hash.HashSet) error {
	var mu sync.Mutex
	missing := hashes.Copy()
	// Blocking get, no concurrency, only one fetcher.
	err := f.getter.GetManyCompressed(ctx, hashes, func(ctx context.Context, chk nbs.ToChunker) {
		mu.Lock()
		missing.Remove(chk.Hash())
		mu.Unlock()
		select {
		case <-ctx.Done():
		case f.resCh <- chk:
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
		case <-f.closedCh:
			return errors.New("PullChunkFetcher: Get on closed Fetcher.")
		}
	}

	return nil
}

func (f *PullChunkFetcher) CloseSend() error {
	close(f.resCh)
	return nil
}

func (f *PullChunkFetcher) Close() error {
	close(f.closedCh)
	return nil
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
	case <-f.closedCh:
		return nbs.CompressedChunk{}, errors.New("PullChunkFetcher: Recv on closed Fetcher.")
	}
}
