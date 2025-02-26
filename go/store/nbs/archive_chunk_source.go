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
	"path/filepath"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type archiveChunkSource struct {
	file string // Only meaningful for local files.
	aRdr archiveReader
}

var _ chunkSource = &archiveChunkSource{}

func newArchiveChunkSource(ctx context.Context, dir string, h hash.Hash, chunkCount uint32, q MemoryQuotaProvider, stats *Stats) (archiveChunkSource, error) {
	archiveFile := filepath.Join(dir, h.String()+ArchiveFileSuffix)

	fra, err := newFileReaderAt(archiveFile)
	if err != nil {
		return archiveChunkSource{}, err
	}

	aRdr, err := newArchiveReader(ctx, fra, uint64(fra.sz), stats)
	if err != nil {
		return archiveChunkSource{}, err
	}
	return archiveChunkSource{archiveFile, aRdr}, nil
}

func newAWSArchiveChunkSource(ctx context.Context,
	s3 *s3ObjectReader,
	al awsLimits,
	name string,
	chunkCount uint32,
	q MemoryQuotaProvider,
	stats *Stats) (cs chunkSource, err error) {

	footer := make([]byte, archiveFooterSize)
	// sz is what we are really after here, but we'll use the bytes to construct the footer to avoid another call.
	_, sz, err := s3.readS3ObjectFromEnd(ctx, name, footer, stats)
	if err != nil {
		return emptyChunkSource{}, err
	}

	aRdr, err := newArchiveReaderFromFooter(ctx, &s3TableReaderAt{s3, name}, sz, footer, stats)
	if err != nil {
		return archiveChunkSource{}, err
	}
	return archiveChunkSource{"", aRdr}, nil
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
	res, err := acs.aRdr.get(ctx, h, stats)
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
		data, err := acs.aRdr.get(ctx, h, stats)
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
func (acs archiveChunkSource) iterate(ctx context.Context, cb func(chunks.Chunk) error, stats *Stats) error {
	return acs.aRdr.iterate(ctx, cb, stats)
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

func (acs archiveChunkSource) name() string {
	return acs.hash().String() + ArchiveFileSuffix
}

func (acs archiveChunkSource) currentSize() uint64 {
	return acs.aRdr.footer.fileSize
}

// reader returns a reader for the entire archive file.
func (acs archiveChunkSource) reader(ctx context.Context) (io.ReadCloser, uint64, error) {
	rd, err := acs.aRdr.reader.Reader(ctx)
	if err != nil {
		return nil, 0, err
	}
	return rd, acs.currentSize(), nil
}
func (acs archiveChunkSource) uncompressedLen() (uint64, error) {
	return 0, errors.New("Archive chunk source does not support uncompressedLen")
}

func (acs archiveChunkSource) index() (tableIndex, error) {
	return nil, errors.New("Archive chunk source does not expose table file indexes")
}

func (acs archiveChunkSource) clone() (chunkSource, error) {
	reader, err := acs.aRdr.clone()
	if err != nil {
		return nil, err
	}
	return archiveChunkSource{acs.file, reader}, nil
}

func (acs archiveChunkSource) getRecordRanges(_ context.Context, requests []getRecord, keeper keeperF) (map[hash.Hash]Range, gcBehavior, error) {
	result := make(map[hash.Hash]Range, len(requests))
	for _, req := range requests {
		hAddr := *req.a
		idx := acs.aRdr.search(hAddr)
		if idx < 0 {
			// Chunk not found.
			continue
		}
		if keeper != nil && keeper(hAddr) {
			return nil, gcBehavior_Block, nil
		}

		dictId, dataId := acs.aRdr.getChunkRef(idx)
		dataSpan := acs.aRdr.getByteSpanByID(dataId)
		dictSpan := acs.aRdr.getByteSpanByID(dictId)

		rng := Range{
			Offset:     dataSpan.offset,
			Length:     uint32(dataSpan.length),
			DictOffset: dictSpan.offset,
			DictLength: uint32(dictSpan.length),
		}

		result[hAddr] = rng
	}
	return result, gcBehavior_Continue, nil
}

func (acs archiveChunkSource) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, ToChunker), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	foundAll := true
	for i, req := range reqs {
		h := *req.a
		toChk, err := acs.aRdr.getAsToChunker(ctx, h, stats)
		if err != nil {
			return true, gcBehavior_Continue, err
		}
		if toChk == nil || toChk.IsEmpty() {
			foundAll = false
		} else {
			if keeper != nil && keeper(h) {
				return true, gcBehavior_Block, nil
			}
			found(ctx, toChk)
			reqs[i].found = true
		}
	}
	return !foundAll, gcBehavior_Continue, nil
}

func (acs archiveChunkSource) iterateAllChunks(ctx context.Context, cb func(chunks.Chunk), stats *Stats) error {
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

		data, err := acs.aRdr.get(ctx, h, stats)
		if err != nil {
			return err
		}

		cb(chunks.NewChunkWithHash(h, data))
	}
	return nil
}
