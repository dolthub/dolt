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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/dynassert"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

var errNoSpoolDir = errors.New("cannot spool table file: blobstore has no spool directory")

// shouldSpool reports whether |bs| needs its table files spooled to a local file.
func shouldSpool(bs blobstore.Blobstore) bool {
	s, ok := bs.(interface{ RangeReadsWholeBlob() bool })
	return ok && s.RangeReadsWholeBlob()
}

// spoolDir returns the directory for the spool files of |bs|, or nil if it does not have one.
func spoolDir(bs blobstore.Blobstore) *tempfiles.LockedDir {
	if s, ok := bs.(interface{ SpoolDir() *tempfiles.LockedDir }); ok {
		return s.SpoolDir()
	}
	return nil
}

// spoolingTableReaderAt serves random ReadAt over a blob that cannot do cheap ranged reads.
// The whole blob is spooled into a local file by the first read that needs its chunks, and
// every later read is served from that file. The file's lifetime is bound to the open chunk
// source. Clones share this struct, whose fields are immutable or safe for concurrent use.
type spoolingTableReaderAt struct {
	bs  blobstore.Blobstore
	key string
	sz  int64
	// pinned reads the blob, if the blobstore can read it after |key| is removed.
	pinned func(context.Context) (io.ReadCloser, error)
	// f is the spool file, or nil if there is none yet.
	f atomic.Pointer[os.File]
	// spooling is a semaphore held while a file is written, so a blob is spooled only once.
	spooling chan struct{}
	cnt      atomic.Int32 // clone() increments and Close() decrements it. The spool file is removed at zero.
}

// newSpoolingTableReaderAt returns a reader of the |sz| byte blob |key| in |bs|. Nothing is
// spooled until a read needs it.
func newSpoolingTableReaderAt(bs blobstore.Blobstore, key string, sz int64) *spoolingTableReaderAt {
	s := &spoolingTableReaderAt{bs: bs, key: key, sz: sz, spooling: make(chan struct{}, 1)}
	s.cnt.Store(1)
	return s
}

// newPinnedSpoolingTableReaderAt is newSpoolingTableReaderAt for a table file being opened. With
// nothing spooled yet, a read holding an older table set can reach this table after a conjoin
// has removed |key|. The chunks are still needed, so the reader pins the blob as it is now.
func newPinnedSpoolingTableReaderAt(bs blobstore.Blobstore, key string, sz int64) (*spoolingTableReaderAt, error) {
	s := newSpoolingTableReaderAt(bs, key, sz)
	if p, ok := bs.(interface {
		PinBlob(key string) (func(context.Context) (io.ReadCloser, error), error)
	}); ok {
		pinned, err := p.PinBlob(key)
		if err != nil {
			return nil, err
		}
		s.pinned = pinned
	}
	return s, nil
}

// openBlob streams the whole blob.
func (s *spoolingTableReaderAt) openBlob(ctx context.Context) (io.ReadCloser, error) {
	if s.pinned != nil {
		return s.pinned(ctx)
	}
	rc, _, _, err := s.bs.Get(ctx, s.key, blobstore.AllRange)
	return rc, err
}

// newPersistedSpoolingTableReaderAt returns a reader of the blob |key| in |bs|, whose
// contents are |data|. They are spooled right away, avoiding a read of the blob.
func newPersistedSpoolingTableReaderAt(bs blobstore.Blobstore, key string, data []byte) (*spoolingTableReaderAt, error) {
	f, sz, err := createSpoolFile(spoolDir(bs), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	s := newSpoolingTableReaderAt(bs, key, sz)
	s.f.Store(f)
	return s, nil
}

// createSpoolFile copies |r| into a file in |dir|. Spools never go to the temp directory, so
// |dir| is required.
func createSpoolFile(dir *tempfiles.LockedDir, r io.Reader) (*os.File, int64, error) {
	if dir == nil {
		return nil, 0, errNoSpoolDir
	}
	f, err := dir.NewFile("nbs-spool-")
	if err != nil {
		return nil, 0, err
	}
	sz, err := io.Copy(f, r)
	if err != nil {
		return nil, 0, errors.Join(err, dir.RemoveFile(f))
	}
	return f, sz, nil
}

// spool returns the spool file, making it from the blob if there is none.
func (s *spoolingTableReaderAt) spool(ctx context.Context) (*os.File, error) {
	if f := s.f.Load(); f != nil {
		return f, nil
	}

	select {
	case s.spooling <- struct{}{}:
		defer func() { <-s.spooling }()
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	if f := s.f.Load(); f != nil {
		return f, nil
	}

	rc, err := s.openBlob(ctx)
	if err != nil {
		return nil, err
	}
	f, sz, err := createSpoolFile(spoolDir(s.bs), rc)
	// Closing is what reports that a git process streaming the blob failed.
	err = errors.Join(err, rc.Close())
	if err == nil && sz != s.sz {
		err = fmt.Errorf("spooling %s: expected %d bytes, got %d", s.key, s.sz, sz)
	}
	if err != nil {
		if f != nil {
			err = errors.Join(err, spoolDir(s.bs).RemoveFile(f))
		}
		return nil, err
	}
	s.f.Store(f)
	return f, nil
}

func (s *spoolingTableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	t1 := time.Now()
	defer func() {
		stats.FileBytesPerRead.Sample(uint64(len(p)))
		stats.FileReadLatency.SampleTimeSince(t1)
	}()
	f, err := s.spool(ctx)
	if err != nil {
		return 0, err
	}
	return f.ReadAt(p, off)
}

// Reader returns an independent reader over the whole blob. A sequential read does not need
// a spool, so with none it streams the blob. Otherwise it reads the spool and holds its own
// reference, so the reader stays valid until the caller closes it, even after the owning
// chunk source is closed. [os.File.ReadAt] does not move the file offset, so the reader is
// safe alongside concurrent ReadAt calls.
func (s *spoolingTableReaderAt) Reader(ctx context.Context) (io.ReadCloser, error) {
	f := s.f.Load()
	if f == nil {
		return s.openBlob(ctx)
	}
	return &spoolFileReader{Reader: io.NewSectionReader(f, 0, s.sz), src: s.ref()}, nil
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
	dynassert.Assert(s.cnt.Add(1) > 1, "attempt to reference a closed spoolingTableReaderAt")
	return s
}

func (s *spoolingTableReaderAt) clone() (tableReaderAt, error) {
	return s.ref(), nil
}

func (s *spoolingTableReaderAt) Close() error {
	cnt := s.cnt.Add(-1)
	dynassert.Assert(cnt >= 0, "invalid cnt on spoolingTableReaderAt")
	if cnt != 0 {
		return nil
	}
	if f := s.f.Load(); f != nil {
		return spoolDir(s.bs).RemoveFile(f)
	}
	return nil
}

// archiveIndexReaderAt serves the reads of opening an archive from its index, already in
// memory, so that opening it does not spool it.
type archiveIndexReaderAt struct {
	buf []byte
	off int64
}

func (r *archiveIndexReaderAt) ReadAtWithStats(_ context.Context, p []byte, off int64, _ *Stats) (int, error) {
	if off < r.off {
		return 0, fmt.Errorf("read at %d is before the archive index at %d", off, r.off)
	}
	return bytes.NewReader(r.buf).ReadAt(p, off-r.off)
}

func (r *archiveIndexReaderAt) Reader(context.Context) (io.ReadCloser, error) {
	return nil, errors.New("runtime error: archiveIndexReaderAt only serves index reads")
}

func (r *archiveIndexReaderAt) clone() (tableReaderAt, error) {
	return nil, errors.New("runtime error: archiveIndexReaderAt only serves index reads")
}

func (r *archiveIndexReaderAt) Close() error {
	return nil
}

// readBlobRange reads the |n| bytes of |br| from the blob |key|, and returns them with the
// size of the whole blob.
func readBlobRange(ctx context.Context, bs blobstore.Blobstore, key string, br blobstore.BlobRange, n uint64) ([]byte, uint64, error) {
	rc, sz, _, err := bs.Get(ctx, key, br)
	if err != nil {
		return nil, 0, err
	}
	buf := make([]byte, n)
	_, err = io.ReadFull(rc, buf)
	if err = errors.Join(err, rc.Close()); err != nil {
		return nil, 0, err
	}
	return buf, sz, nil
}

// newSpooledBSArchiveChunkSource opens an archive by reading its footer and index, and
// spools it when its chunks are first read.
func newSpooledBSArchiveChunkSource(ctx context.Context, bs blobstore.Blobstore, name hash.Hash, q MemoryQuotaProvider, opts openOpts, stats *Stats) (chunkSource, error) {
	key := name.String() + ArchiveFileSuffix
	footerBytes, sz, err := readBlobRange(ctx, bs, key, blobstore.NewBlobRange(-int64(archiveFooterSize), 0), archiveFooterSize)
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, fmt.Errorf("%s: blobstore did not report the size of the archive", key)
	}
	footer, err := buildArchiveFooter(name, sz, footerBytes)
	if err != nil {
		return nil, err
	}

	index := footer.totalIndexSpan()
	var indexBytes []byte
	if index.length > 0 {
		indexBytes, _, err = readBlobRange(ctx, bs, key, blobstore.NewBlobRange(int64(index.offset), int64(index.length)), index.length)
		if err != nil {
			return nil, err
		}
	}

	aRdr, err := buildArchiveReader(ctx, &archiveIndexReaderAt{buf: indexBytes, off: int64(index.offset)}, footer, q, opts, stats)
	if err != nil {
		return nil, err
	}
	aRdr.reader, err = newPinnedSpoolingTableReaderAt(bs, key, int64(sz))
	if err != nil {
		return nil, errors.Join(err, aRdr.close())
	}
	return &archiveChunkSource{aRdr: aRdr, refs: noopRefCounter{}, blockSize: fileBlockSize}, nil
}
