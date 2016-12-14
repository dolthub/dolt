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

const fileReadAmpThresh = uint64(2)

var (
	pageSize = int64(os.Getpagesize())
	maxInt   = int64(math.MaxInt64)
)

func init() {
	if strconv.IntSize == 32 {
		maxInt = math.MaxInt32
	}
}

func newMmapTableReader(dir string, h addr, chunkCount uint32) chunkSource {
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

	// index. Mmap won't take an offset that's not page-aligned, so find the nearest page boundary preceding the index.
	indexOffset := fi.Size() - int64(footerSize) - int64(indexSize(chunkCount))
	aligned := indexOffset / pageSize * pageSize // Thanks, integer arithmetic!
	d.PanicIfTrue(fi.Size()-aligned > maxInt)
	buff, err := unix.Mmap(int(f.Fd()), aligned, int(fi.Size()-aligned), unix.PROT_READ, unix.MAP_SHARED)
	d.PanicIfError(err)
	success = true

	index := parseTableIndex(buff[indexOffset-aligned:])
	source := &mmapTableReader{newTableReader(index, f, fileReadAmpThresh), f, buff, h}

	d.PanicIfFalse(chunkCount == source.count())
	return source
}

func (mmtr *mmapTableReader) close() error {
	mmtr.f.Close()
	return unix.Munmap(mmtr.buff)
}

func (mmtr *mmapTableReader) hash() addr {
	return mmtr.h
}
