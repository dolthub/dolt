// Copyright 2026 Dolthub, Inc.
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
	"errors"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/dynassert"
	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

// shouldSpool reports whether |bs| needs its table files spooled to a local temp file.
func shouldSpool(bs blobstore.Blobstore) bool {
	s, ok := bs.(interface{ RangeReadsWholeBlob() bool })
	return ok && s.RangeReadsWholeBlob()
}

// spoolingTableReaderAt serves random ReadAt over a blob that cannot do cheap ranged
// reads. The whole blob is spooled once into a local temp file at construction. Every
// read is served from that file, whose lifetime is bound to the open chunk source.
type spoolingTableReaderAt struct {
	f   *os.File
	sz  int64
	cnt *int32 // clone() increments and Close() decrements it. The temp file is removed at zero.
}

// newSpoolingTableReaderAt streams the whole blob |key| from |bs| into a temp file.
func newSpoolingTableReaderAt(ctx context.Context, bs blobstore.Blobstore, key string) (*spoolingTableReaderAt, error) {
	rc, _, _, err := bs.Get(ctx, key, blobstore.AllRange)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	f, err := tempfiles.MovableTempFileProvider.NewFile("", "nbs-spool-")
	if err != nil {
		return nil, err
	}
	sz, err := io.Copy(f, rc)
	if err != nil {
		f.Close()
		_ = file.Remove(f.Name())
		return nil, err
	}
	cnt := int32(1)
	return &spoolingTableReaderAt{f: f, sz: sz, cnt: &cnt}, nil
}

func (s *spoolingTableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	t1 := time.Now()
	defer func() {
		stats.FileBytesPerRead.Sample(uint64(len(p)))
		stats.FileReadLatency.SampleTimeSince(t1)
	}()
	return s.f.ReadAt(p, off)
}

// Reader returns an independent reader over the whole spooled file. It holds its own
// reference, so the reader stays valid until the caller closes it, even after the owning
// chunk source is closed. [os.File.ReadAt] does not move the file offset, so the reader
// is safe alongside concurrent ReadAt calls.
func (s *spoolingTableReaderAt) Reader(ctx context.Context) (io.ReadCloser, error) {
	src := s.ref()
	return &spoolFileReader{Reader: io.NewSectionReader(src.f, 0, src.sz), src: src}, nil
}

// spoolFileReader streams a spooled file while holding a reference to it, so the temp
// file is not removed until the reader is closed.
type spoolFileReader struct {
	io.Reader
	src *spoolingTableReaderAt
}

func (r *spoolFileReader) Close() error {
	return r.src.Close()
}

// ref increments the reference count and returns a handle that shares the spooled file.
func (s *spoolingTableReaderAt) ref() *spoolingTableReaderAt {
	dynassert.Assert(atomic.AddInt32(s.cnt, 1) > 1, "attempt to reference a closed spoolingTableReaderAt")
	c := *s
	return &c
}

func (s *spoolingTableReaderAt) clone() (tableReaderAt, error) {
	return s.ref(), nil
}

func (s *spoolingTableReaderAt) Close() error {
	cnt := atomic.AddInt32(s.cnt, -1)
	dynassert.Assert(cnt >= 0, "invalid cnt on spoolingTableReaderAt")
	if cnt != 0 {
		return nil
	}
	name := s.f.Name()
	return errors.Join(s.f.Close(), file.Remove(name))
}

// newSpooledBSTableChunkSource opens a table file by spooling it whole to a local temp
// file once, then reading its index and serving chunk reads from that file.
func newSpooledBSTableChunkSource(ctx context.Context, bs blobstore.Blobstore, name hash.Hash, chunkCount uint32, q MemoryQuotaProvider, stats *Stats) (chunkSource, error) {
	ra, err := newSpoolingTableReaderAt(ctx, bs, name.String())
	if err != nil {
		return nil, err
	}

	index, err := loadTableIndex(ctx, stats, chunkCount, q, func(p []byte) error {
		_, err := ra.f.ReadAt(p, ra.sz-int64(len(p)))
		return err
	})
	if err != nil {
		_ = ra.Close()
		return nil, err
	}

	if chunkCount != index.chunkCount() {
		_ = index.Close()
		_ = ra.Close()
		return nil, errors.New("unexpected chunk count")
	}

	tr, err := newTableReader(ctx, index, ra, s3BlockSize)
	if err != nil {
		_ = index.Close()
		_ = ra.Close()
		return nil, err
	}
	return &chunkSourceAdapter{tr, name}, nil
}

// newSpooledBSArchiveChunkSource is the archive counterpart of newSpooledBSTableChunkSource.
// It spools the file whole, reads the footer, and serves chunk reads from the spooled file.
func newSpooledBSArchiveChunkSource(ctx context.Context, bs blobstore.Blobstore, name hash.Hash, q MemoryQuotaProvider, stats *Stats) (chunkSource, error) {
	ra, err := newSpoolingTableReaderAt(ctx, bs, name.String()+ArchiveFileSuffix)
	if err != nil {
		return nil, err
	}

	aRdr, err := newArchiveReader(ctx, ra, name, uint64(ra.sz), q, stats)
	if err != nil {
		_ = ra.Close()
		return nil, err
	}
	return &archiveChunkSource{aRdr: aRdr, refs: noopRefCounter{}}, nil
}
