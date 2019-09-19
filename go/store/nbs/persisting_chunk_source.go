// Copyright 2019 Liquidata, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/liquidata-inc/dolt/go/store/atomicerr"

	"github.com/liquidata-inc/dolt/go/store/chunks"
)

var ErrNoReader = errors.New("could not get reader")
var ErrNoChunkSource = errors.New("no chunk source")

func newPersistingChunkSource(ctx context.Context, mt *memTable, haver chunkReader, p tablePersister, rl chan struct{}, stats *Stats) *persistingChunkSource {
	t1 := time.Now()

	ccs := &persistingChunkSource{ae: atomicerr.New(), mt: mt}
	ccs.wg.Add(1)
	rl <- struct{}{}
	go func() {
		defer ccs.wg.Done()
		defer func() {
			<-rl
		}()

		cs, err := p.Persist(ctx, mt, haver, stats)

		if err != nil {
			ccs.ae.SetIfError(err)
			return
		}

		ccs.mu.Lock()
		defer ccs.mu.Unlock()
		ccs.cs = cs
		ccs.mt = nil

		cnt, err := cs.count()

		if err != nil {
			ccs.ae.SetIfError(err)
			return
		}

		if cnt > 0 {
			stats.PersistLatency.SampleTimeSince(t1)
		}
	}()

	return ccs
}

type persistingChunkSource struct {
	ae *atomicerr.AtomicError
	mu sync.RWMutex
	mt *memTable

	wg sync.WaitGroup
	cs chunkSource
}

func (ccs *persistingChunkSource) getReader() chunkReader {
	ccs.mu.RLock()
	defer ccs.mu.RUnlock()
	if ccs.mt != nil {
		return ccs.mt
	}
	return ccs.cs
}

func (ccs *persistingChunkSource) has(h addr) (bool, error) {
	cr := ccs.getReader()

	if cr == nil {
		return false, ErrNoReader
	}

	return cr.has(h)
}

func (ccs *persistingChunkSource) hasMany(addrs []hasRecord) (bool, error) {
	cr := ccs.getReader()

	if cr == nil {
		return false, ErrNoReader
	}
	return cr.hasMany(addrs)
}

func (ccs *persistingChunkSource) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	cr := ccs.getReader()

	if cr == nil {
		return nil, ErrNoReader
	}

	return cr.get(ctx, h, stats)
}

func (ccs *persistingChunkSource) getMany(ctx context.Context, reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup, ae *atomicerr.AtomicError, stats *Stats) bool {
	cr := ccs.getReader()

	if cr == nil {
		ae.SetIfError(ErrNoReader)
		return false
	}

	return cr.getMany(ctx, reqs, foundChunks, wg, ae, stats)
}

func (ccs *persistingChunkSource) getManyCompressed(ctx context.Context, reqs []getRecord, foundCmpChunks chan CompressedChunk, wg *sync.WaitGroup, ae *atomicerr.AtomicError, stats *Stats) bool {
	cr := ccs.getReader()

	if cr == nil {
		ae.SetIfErrAndCheck(ErrNoReader)
		return false
	}

	return cr.getManyCompressed(ctx, reqs, foundCmpChunks, wg, ae, stats)
}

func (ccs *persistingChunkSource) wait() error {
	ccs.wg.Wait()
	return ccs.ae.Get()
}

func (ccs *persistingChunkSource) count() (uint32, error) {
	err := ccs.wait()

	if err != nil {
		return 0, err
	}

	if ccs.cs == nil {
		return 0, ErrNoChunkSource
	}

	return ccs.cs.count()
}

func (ccs *persistingChunkSource) uncompressedLen() (uint64, error) {
	err := ccs.wait()

	if err != nil {
		return 0, err
	}

	if ccs.cs == nil {
		return 0, ErrNoChunkSource
	}

	return ccs.cs.uncompressedLen()
}

func (ccs *persistingChunkSource) hash() (addr, error) {
	err := ccs.wait()

	if err != nil {
		return addr{}, err
	}

	if ccs.cs == nil {
		return addr{}, ErrNoChunkSource
	}

	return ccs.cs.hash()
}

func (ccs *persistingChunkSource) index() (tableIndex, error) {
	err := ccs.wait()

	if err != nil {
		return tableIndex{}, err
	}

	if ccs.cs == nil {
		return tableIndex{}, ErrNoChunkSource
	}

	return ccs.cs.index()
}

func (ccs *persistingChunkSource) reader(ctx context.Context) (io.Reader, error) {
	err := ccs.wait()

	if err != nil {
		return nil, err
	}

	if ccs.cs == nil {
		return nil, ErrNoChunkSource
	}

	return ccs.cs.reader(ctx)
}

func (ccs *persistingChunkSource) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool, err error) {
	err = ccs.wait()

	if err != nil {
		return 0, false, err
	}

	if ccs.cs == nil {
		return 0, false, ErrNoChunkSource
	}

	return ccs.cs.calcReads(reqs, blockSize)
}

func (ccs *persistingChunkSource) extract(ctx context.Context, chunks chan<- extractRecord) error {
	err := ccs.wait()

	if err != nil {
		return err
	}

	if ccs.cs == nil {
		return ErrNoChunkSource
	}

	return ccs.cs.extract(ctx, chunks)
}

type emptyChunkSource struct{}

func (ecs emptyChunkSource) has(h addr) (bool, error) {
	return false, nil
}

func (ecs emptyChunkSource) hasMany(addrs []hasRecord) (bool, error) {
	return true, nil
}

func (ecs emptyChunkSource) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	return nil, nil
}

func (ecs emptyChunkSource) getMany(ctx context.Context, reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup, ae *atomicerr.AtomicError, stats *Stats) bool {
	return true
}

func (ecs emptyChunkSource) getManyCompressed(ctx context.Context, reqs []getRecord, foundCmpChunks chan CompressedChunk, wg *sync.WaitGroup, ae *atomicerr.AtomicError, stats *Stats) bool {
	return true
}

func (ecs emptyChunkSource) count() (uint32, error) {
	return 0, nil
}

func (ecs emptyChunkSource) uncompressedLen() (uint64, error) {
	return 0, nil
}

func (ecs emptyChunkSource) hash() (addr, error) {
	return addr{}, nil
}

func (ecs emptyChunkSource) index() (tableIndex, error) {
	return tableIndex{}, nil
}

func (ecs emptyChunkSource) reader(context.Context) (io.Reader, error) {
	return &bytes.Buffer{}, nil
}

func (ecs emptyChunkSource) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool, err error) {
	return 0, true, nil
}

func (ecs emptyChunkSource) extract(ctx context.Context, chunks chan<- extractRecord) error {
	return nil
}
