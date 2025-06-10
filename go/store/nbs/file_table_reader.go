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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dolthub/dolt/go/store/hash"
)

var ErrTableFileNotFound = errors.New("table file not found")

type fileTableReader struct {
	tableReader
	h hash.Hash
}

const (
	fileBlockSize = 1 << 12
)

func tableFileExists(ctx context.Context, dir string, h hash.Hash) (bool, error) {
	path := filepath.Join(dir, h.String())
	_, err := os.Stat(path)

	if os.IsNotExist(err) {
		return false, nil
	}

	return err == nil, err
}

func archiveFileExists(ctx context.Context, dir string, name string) (bool, error) {
	if !strings.HasSuffix(name, ArchiveFileSuffix) {
		_, ok := hash.MaybeParse(name)
		if !ok {
			return false, errors.New(fmt.Sprintf("invalid archive file name: %s", name))
		}

		name = fmt.Sprintf("%s%s", name, ArchiveFileSuffix)
	}

	path := filepath.Join(dir, name)
	_, err := os.Stat(path)

	if os.IsNotExist(err) {
		return false, nil
	}

	return err == nil, err
}

func newFileTableReader(ctx context.Context, dir string, h hash.Hash, chunkCount uint32, q MemoryQuotaProvider, stats *Stats) (cs chunkSource, err error) {
	// we either have a table file or an archive file
	tfExists, err := tableFileExists(ctx, dir, h)
	if err != nil {
		return nil, err
	} else if tfExists {
		return nomsFileTableReader(ctx, filepath.Join(dir, h.String()), h, chunkCount, q)
	}

	afExists, err := archiveFileExists(ctx, dir, h.String())
	if err != nil {
		return nil, err
	} else if afExists {
		return newArchiveChunkSource(ctx, dir, h, chunkCount, q, stats)
	}
	return nil, fmt.Errorf("error opening table file: %w: %s/%s", ErrTableFileNotFound, dir, h.String())
}

func newFileReaderAt(path string) (*fileReaderAt, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() < 0 {
		// Size returns the number of bytes for regular files and is system dependent for others (Some of which can be negative).
		return nil, fmt.Errorf("%s has invalid size: %d", path, fi.Size())
	}
	cnt := new(int32)
	*cnt = 1
	return &fileReaderAt{f, path, fi.Size(), cnt}, nil
}

func nomsFileTableReader(ctx context.Context, path string, h hash.Hash, chunkCount uint32, q MemoryQuotaProvider) (cs chunkSource, err error) {
	fra, err := newFileReaderAt(path)
	if err != nil {
		return nil, err
	}

	idxSz := int64(indexSize(chunkCount) + footerSize)
	indexOffset := fra.sz - idxSz
	r := io.NewSectionReader(fra.f, indexOffset, idxSz)
	if int64(int(idxSz)) != idxSz {
		err = fmt.Errorf("table file %s is too large to read on this platform. index size %d > max int.", path, idxSz)
		return
	}

	var b []byte
	b, err = q.AcquireQuotaBytes(ctx, int(idxSz))
	if err != nil {
		fra.Close()
		return
	}

	_, err = io.ReadFull(r, b)
	if err != nil {
		q.ReleaseQuotaBytes(len(b))
		fra.Close()
		return
	}

	index, err := parseTableIndex(ctx, b, q)
	if err != nil {
		q.ReleaseQuotaBytes(len(b))
		fra.Close()
		return
	}

	if chunkCount != index.chunkCount() {
		index.Close()
		fra.Close()
		return nil, errors.New("unexpected chunk count")
	}

	tr, err := newTableReader(index, fra, fileBlockSize)
	if err != nil {
		index.Close()
		fra.Close()
		return nil, err
	}
	return &fileTableReader{
		tr,
		h,
	}, nil
}

func (ftr *fileTableReader) hash() hash.Hash {
	return ftr.h
}

func (ftr *fileTableReader) suffix() string {
	return ""
}

func (ftr *fileTableReader) Close() error {
	return ftr.tableReader.close()
}

func (ftr *fileTableReader) clone() (chunkSource, error) {
	tr, err := ftr.tableReader.clone()
	if err != nil {
		return &fileTableReader{}, err
	}
	return &fileTableReader{tr, ftr.h}, nil
}

type fileReaderAt struct {
	f    *os.File
	path string
	sz   int64
	// refcnt, clone() increments and Close() decrements. The *os.File is closed when it reaches 0.
	cnt  *int32
}

func (fra *fileReaderAt) clone() (tableReaderAt, error) {
	if atomic.AddInt32(fra.cnt, 1) == 1 {
		panic("attempt to clone a closed fileReaderAt")
	}
	return &fileReaderAt{
		fra.f,
		fra.path,
		fra.sz,
		fra.cnt,
	}, nil
}

func (fra *fileReaderAt) Close() error {
	cnt := atomic.AddInt32(fra.cnt, -1)
	if cnt == 0 {
		return fra.f.Close()
	} else if cnt < 0 {
		panic("invalid cnt on fileReaderAt")
	} else {
		return nil
	}
}

func (fra *fileReaderAt) Reader(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(fra.path)
}

func (fra *fileReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	t1 := time.Now()
	defer func() {
		stats.FileBytesPerRead.Sample(uint64(len(p)))
		stats.FileReadLatency.SampleTimeSince(t1)
	}()
	return fra.f.ReadAt(p, off)
}

func newTableFileMetadata(path string, chunkCount uint32) (*TableFileMetadata, error) {
	fra, err := newFileReaderAt(path)
	if err != nil {
		return nil, err
	}

	idxSz := int64(indexSize(chunkCount) + footerSize)
	indexOffset := fra.sz - idxSz

	return &TableFileMetadata{
		snappyChunkCount: int(chunkCount),
		snappyBytes:      uint64(indexOffset),
	}, nil
}
