// Copyright 2019 Dolthub, Inc.
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
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

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

func (ecs emptyChunkSource) getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), stats *Stats) (bool, error) {
	return true, nil
}

func (ecs emptyChunkSource) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (bool, error) {
	return true, nil
}

func (ecs emptyChunkSource) count() (uint32, error) {
	return 0, nil
}

func (ecs emptyChunkSource) uncompressedLen() (uint64, error) {
	return 0, nil
}

func (ecs emptyChunkSource) hash() addr {
	return addr{}
}

func (ecs emptyChunkSource) index() (tableIndex, error) {
	return onHeapTableIndex{}, nil
}

func (ecs emptyChunkSource) reader(context.Context) (io.Reader, uint64, error) {
	return &bytes.Buffer{}, 0, nil
}

func (ecs emptyChunkSource) getRecordRanges(lookups []getRecord) (map[hash.Hash]Range, error) {
	return map[hash.Hash]Range{}, nil
}

func (ecs emptyChunkSource) currentSize() uint64 {
	return 0
}

func (ecs emptyChunkSource) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool, err error) {
	return 0, true, nil
}

func (ecs emptyChunkSource) close() error {
	return nil
}

func (ecs emptyChunkSource) clone() (chunkSource, error) {
	return ecs, nil
}
