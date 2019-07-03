// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"io"
	"math"
	"path/filepath"
	"strconv"
	"time"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

type mmapTableReader struct {
	tableReader
	fc *fdCache
	h  addr
}

const (
	fileBlockSize = 1 << 12
)

var (
	maxInt = int64(math.MaxInt64)
)

func init() {
	if strconv.IntSize == 32 {
		maxInt = math.MaxInt32
	}
}

func newMmapTableReader(dir string, h addr, chunkCount uint32, indexCache *indexCache, fc *fdCache) (chunkSource, error) {
	path := filepath.Join(dir, h.String())

	var index tableIndex
	found := false
	if indexCache != nil {
		indexCache.lockEntry(h)
		defer indexCache.unlockEntry(h)
		index, found = indexCache.get(h)
	}

	if !found {
		f, err := fc.RefFile(path)

		if err != nil {
			return nil, err
		}

		defer func() {
			err := fc.UnrefFile(path)
			d.PanicIfError(err)
		}()

		fi, err := f.Stat()

		if err != nil {
			return nil, err
		}

		if fi.Size() < 0 {
			// Size returns the number of bytes for regular files and is system dependant for others (Some of which can be negative).
			return nil, fmt.Errorf("%s has invalid size: %d", path, fi.Size())
		}

		// index. Mmap won't take an offset that's not page-aligned, so find the nearest page boundary preceding the index.
		indexOffset := fi.Size() - int64(footerSize) - int64(indexSize(chunkCount))
		aligned := indexOffset / mmapAlignment * mmapAlignment // Thanks, integer arithmetic!

		if fi.Size()-aligned > maxInt {
			return nil, fmt.Errorf("%s - size: %d alignment: %d> maxInt: %d", path, fi.Size(), aligned, maxInt)
		}

		mm, err := mmap.MapRegion(f, int(fi.Size()-aligned), mmap.RDONLY, 0, aligned)

		if err != nil {
			return nil, err
		}

		buff := []byte(mm)
		index, err = parseTableIndex(buff[indexOffset-aligned:])

		if err != nil {
			return nil, err
		}

		if indexCache != nil {
			indexCache.put(h, index)
		}

		err = mm.Unmap()

		if err != nil {
			return nil, err
		}
	}

	if chunkCount != index.chunkCount {
		return nil, errors.New("unexpected chunk count")
	}

	return &mmapTableReader{
		newTableReader(index, &cacheReaderAt{path, fc}, fileBlockSize),
		fc,
		h,
	}, nil
}

func (mmtr *mmapTableReader) hash() (addr, error) {
	return mmtr.h, nil
}

type cacheReaderAt struct {
	path string
	fc   *fdCache
}

func (cra *cacheReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	var r io.ReaderAt
	t1 := time.Now()

	if r, err = cra.fc.RefFile(cra.path); err != nil {
		return
	}
	defer func() {
		stats.FileBytesPerRead.Sample(uint64(len(p)))
		stats.FileReadLatency.SampleTimeSince(t1)
	}()

	defer func() {
		err := cra.fc.UnrefFile(cra.path)
		d.PanicIfError(err)
	}()

	return r.ReadAt(p, off)
}
