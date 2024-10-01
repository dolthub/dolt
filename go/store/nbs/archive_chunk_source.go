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
	"sync"

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

func (acs archiveChunkSource) has(h hash.Hash) (bool, error) {
	return acs.aRdr.has(h), nil
}

func (acs archiveChunkSource) hasMany(addrs []hasRecord) (bool, error) {
	// single threaded first pass.
	foundAll := true
	for i, addr := range addrs {
		if acs.aRdr.has(*(addr.a)) {
			addrs[i].has = true
		} else {
			foundAll = false
		}
	}
	return foundAll, nil
}

func (acs archiveChunkSource) get(ctx context.Context, h hash.Hash, stats *Stats) ([]byte, error) {
	// ctx, stats ? NM4.
	return acs.aRdr.get(h)
}

func (acs archiveChunkSource) getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), stats *Stats) (bool, error) {
	// single threaded first pass.
	foundAll := true
	for i, req := range reqs {
		data, err := acs.aRdr.get(*req.a)
		if err != nil || data == nil {
			foundAll = false
		} else {
			chunk := chunks.NewChunk(data)
			found(ctx, &chunk)
			reqs[i].found = true
		}
	}
	return !foundAll, nil
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

func (acs archiveChunkSource) getRecordRanges(_ context.Context, _ []getRecord) (map[hash.Hash]Range, error) {
	return nil, errors.New("Archive chunk source does not support getRecordRanges")
}

func (acs archiveChunkSource) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (bool, error) {
	return false, errors.New("Archive chunk source does not support getManyCompressed")
}

func (acs archiveChunkSource) getAllChunkHashes(ctx context.Context, out chan<- hash.Hash, wg *sync.WaitGroup) int {
	addrCount := uint32(len(acs.aRdr.prefixes))

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint32(0); i < addrCount; i++ {
			var h hash.Hash
			suffix := acs.aRdr.getSuffixByID(i)

			// Reconstruct the hash from the prefix and suffix.
			binary.BigEndian.PutUint64(h[:uint64Size], acs.aRdr.prefixes[i])
			copy(h[hash.ByteLen-hash.SuffixLen:], suffix[:])

			select {
			case out <- h: // Send hash
			case <-ctx.Done(): // Stop if context is cancelled
				return
			}
		}
	}()
	return int(addrCount)
}
