// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"math"
	"os"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"

	"github.com/attic-labs/noms/go/d"
)

type mmapTableReader struct {
	tableReader
	f    *os.File
	buff []byte
	h    addr
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

func newMmapTableReader(dir string, h addr, chunkCount uint32, indexCache *indexCache) chunkSource {
	success := false
	f, err := os.Open(filepath.Join(dir, h.String()))
	d.PanicIfError(err)
	defer func() {
		if !success {
			d.PanicIfError(f.Close())
		}
	}()

	fi, err := f.Stat()
	d.PanicIfError(err)
	d.PanicIfTrue(fi.Size() < 0)

	var index tableIndex
	found := false
	if indexCache != nil {
		index, found = indexCache.get(h)
	}

	var buff []byte
	if !found {
		// index. Mmap won't take an offset that's not page-aligned, so find the nearest page boundary preceding the index.
		indexOffset := fi.Size() - int64(footerSize) - int64(indexSize(chunkCount))
		aligned := indexOffset / pageSize * pageSize // Thanks, integer arithmetic!
		d.PanicIfTrue(fi.Size()-aligned > maxInt)
		var err error
		buff, err = unix.Mmap(int(f.Fd()), aligned, int(fi.Size()-aligned), unix.PROT_READ, unix.MAP_SHARED)
		d.PanicIfError(err)
		index = parseTableIndex(buff[indexOffset-aligned:])

		if indexCache != nil {
			indexCache.put(h, index)
		}
	}
	success = true

	source := &mmapTableReader{newTableReader(index, f, fileBlockSize), f, buff, h}

	d.PanicIfFalse(chunkCount == source.count())
	return source
}

func (mmtr *mmapTableReader) close() (err error) {
	err = mmtr.f.Close()
	if mmtr.buff != nil {
		err = unix.Munmap(mmtr.buff)
	}
	return
}

func (mmtr *mmapTableReader) hash() addr {
	return mmtr.h
}
