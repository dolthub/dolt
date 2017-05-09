// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"

	"github.com/attic-labs/noms/go/d"
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
	pageSize = int64(os.Getpagesize())
	maxInt   = int64(math.MaxInt64)
)

func init() {
	if strconv.IntSize == 32 {
		maxInt = math.MaxInt32
	}
}

func newMmapTableReader(dir string, h addr, chunkCount uint32, indexCache *indexCache, fc *fdCache) chunkSource {
	path := filepath.Join(dir, h.String())
	f, err := fc.RefFile(path)
	d.PanicIfError(err)
	defer fc.UnrefFile(path)

	fi, err := f.Stat()
	d.PanicIfError(err)
	d.PanicIfTrue(fi.Size() < 0)

	var index tableIndex
	found := false
	if indexCache != nil {
		index, found = indexCache.get(h)
	}

	if !found {
		// index. Mmap won't take an offset that's not page-aligned, so find the nearest page boundary preceding the index.
		indexOffset := fi.Size() - int64(footerSize) - int64(indexSize(chunkCount))
		aligned := indexOffset / pageSize * pageSize // Thanks, integer arithmetic!
		d.PanicIfTrue(fi.Size()-aligned > maxInt)
		buff, err := unix.Mmap(int(f.Fd()), aligned, int(fi.Size()-aligned), unix.PROT_READ, unix.MAP_SHARED)
		d.PanicIfError(err)
		index = parseTableIndex(buff[indexOffset-aligned:])

		if indexCache != nil {
			indexCache.put(h, index)
		}
		err = unix.Munmap(buff)
		d.PanicIfError(err)
	}

	source := &mmapTableReader{
		newTableReader(index, &cacheReaderAt{path, fc}, fileBlockSize),
		fc,
		h,
	}

	d.PanicIfFalse(chunkCount == source.count())
	return source
}

func (mmtr *mmapTableReader) hash() addr {
	return mmtr.h
}

type cacheReaderAt struct {
	path string
	fc   *fdCache
}

func (cra *cacheReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	var r io.ReaderAt
	if r, err = cra.fc.RefFile(cra.path); err != nil {
		return
	}
	defer cra.fc.UnrefFile(cra.path)
	return r.ReadAt(p, off)
}
