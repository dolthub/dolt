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

package nbs

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type archiveChunkSource struct {
	file string
	aRdr archiveReader
}

var _ chunkSource = &archiveChunkSource{}

func newArchiveChunkSource(ctx context.Context, dir string, h hash.Hash, chunkCount uint32, q MemoryQuotaProvider) (archiveChunkSource, error) {
	archiveFile := filepath.Join(dir, h.String()+archiveFileSuffix)

	file, size, err := openReader(archiveFile)
	if err != nil {
		return archiveChunkSource{}, err
	}

	aRdr, err := newArchiveReader(file, size)
	if err != nil {
		return archiveChunkSource{}, err
	}
	return archiveChunkSource{archiveFile, aRdr}, nil
}

func openReader(file string) (io.ReaderAt, uint64, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, 0, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, uint64(stat.Size()), nil
}

func (acs archiveChunkSource) has(h hash.Hash, keeper keeperF) (bool, gcBehavior, error) {
	res := acs.aRdr.has(h)
	if res && keeper != nil && keeper(h) {
		return false, gcBehavior_Block, nil
	}
	return res, gcBehavior_Continue, nil
}

func (acs archiveChunkSource) hasMany(addrs []hasRecord, keeper keeperF) (bool, gcBehavior, error) {
	// single threaded first pass.
	foundAll := true
	for i, addr := range addrs {
		h := *addr.a
		if acs.aRdr.has(h) {
			if keeper != nil && keeper(h) {
				return false, gcBehavior_Block, nil
			}
			addrs[i].has = true
		} else {
			foundAll = false
		}
	}
	return !foundAll, gcBehavior_Continue, nil
}

func (acs archiveChunkSource) get(ctx context.Context, h hash.Hash, keeper keeperF, stats *Stats) ([]byte, gcBehavior, error) {
	res, err := acs.aRdr.get(h)
	if err != nil {
		return nil, gcBehavior_Continue, err
	}
	if res != nil && keeper != nil && keeper(h) {
		return nil, gcBehavior_Block, nil
	}
	return res, gcBehavior_Continue, nil
}

func (acs archiveChunkSource) getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	// single threaded first pass.
	foundAll := true
	for i, req := range reqs {
		h := *req.a
		data, err := acs.aRdr.get(h)
		if err != nil {
			return true, gcBehavior_Continue, err
		}
		if data == nil {
			foundAll = false
		} else {
			if keeper != nil && keeper(h) {
				return true, gcBehavior_Block, nil
			}
			chunk := chunks.NewChunk(data)
			found(ctx, &chunk)
			reqs[i].found = true
		}
	}
	return !foundAll, gcBehavior_Continue, nil
}

// iterate iterates over the archive chunks. The callback is called for each chunk in the archive. This is not optimized
// as currently is it only used for un-archiving, which should be uncommon.
func (acs archiveChunkSource) iterate(ctx context.Context, cb func(chunks.Chunk) error) error {
	return acs.aRdr.iterate(ctx, cb)
}

func (acs archiveChunkSource) count() (uint32, error) {
	return acs.aRdr.count(), nil
}

func (acs archiveChunkSource) close() error {
	return acs.aRdr.close()
}

func (acs archiveChunkSource) hash() hash.Hash {
	return acs.aRdr.footer.hash
}

func (acs archiveChunkSource) currentSize() uint64 {
	return acs.aRdr.footer.fileSize
}

func (acs archiveChunkSource) reader(ctx context.Context) (io.ReadCloser, uint64, error) {
	return nil, 0, errors.New("Archive chunk source does not support reader")
}
func (acs archiveChunkSource) uncompressedLen() (uint64, error) {
	return 0, errors.New("Archive chunk source does not support uncompressedLen")
}

func (acs archiveChunkSource) index() (tableIndex, error) {
	return nil, errors.New("Archive chunk source does not expose table file indexes")
}

func (acs archiveChunkSource) clone() (chunkSource, error) {
	newReader, _, err := openReader(acs.file)
	if err != nil {
		return nil, err
	}

	rdr := acs.aRdr.clone(newReader)

	return archiveChunkSource{acs.file, rdr}, nil
}

func (acs archiveChunkSource) getRecordRanges(_ context.Context, _ []getRecord, _ keeperF) (map[hash.Hash]Range, gcBehavior, error) {
	return nil, gcBehavior_Continue, errors.New("Archive chunk source does not support getRecordRanges")
}

func (acs archiveChunkSource) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	return acs.getMany(ctx, eg, reqs, func(ctx context.Context, chk *chunks.Chunk) {
		found(ctx, ChunkToCompressedChunk(*chk))
	}, keeper, stats)
}

func (acs archiveChunkSource) iterateAllChunks(ctx context.Context, cb func(chunks.Chunk), _ *Stats) error {
	addrCount := uint32(len(acs.aRdr.prefixes))
	for i := uint32(0); i < addrCount; i++ {
		var h hash.Hash
		suffix := acs.aRdr.getSuffixByID(i)

		// Reconstruct the hash from the prefix and suffix.
		binary.BigEndian.PutUint64(h[:uint64Size], acs.aRdr.prefixes[i])
		copy(h[uint64Size:], suffix[:])

		if ctx.Err() != nil {
			return ctx.Err()
		}

		data, err := acs.aRdr.get(h)
		if err != nil {
			return err
		}

		cb(chunks.NewChunkWithHash(h, data))
	}
	return nil
}
