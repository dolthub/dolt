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
	"time"

	"github.com/dolthub/dolt/go/store/hash"
)

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

func archiveFileExists(ctx context.Context, dir string, h hash.Hash) (bool, error) {
	darc := fmt.Sprintf("%s%s", h.String(), archiveFileSuffix)

	path := filepath.Join(dir, darc)
	_, err := os.Stat(path)

	if os.IsNotExist(err) {
		return false, nil
	}

	return err == nil, err
}

func newFileTableReader(ctx context.Context, dir string, h hash.Hash, chunkCount uint32, q MemoryQuotaProvider) (cs chunkSource, err error) {
	// we either have a table file or an archive file
	tfExists, err := tableFileExists(ctx, dir, h)
	if err != nil {
		return nil, err
	} else if tfExists {
		return nomsFileTableReader(ctx, filepath.Join(dir, h.String()), h, chunkCount, q)
	}

	afExists, err := archiveFileExists(ctx, dir, h)
	if err != nil {
		return nil, err
	} else if afExists {
		return newArchiveChunkSource(ctx, dir, h, chunkCount, q)
	}
	return nil, errors.New(fmt.Sprintf("table file %s/%s not found", dir, h.String()))
}

func nomsFileTableReader(ctx context.Context, path string, h hash.Hash, chunkCount uint32, q MemoryQuotaProvider) (cs chunkSource, err error) {
	var f *os.File
	index, sz, err := func() (ti onHeapTableIndex, sz int64, err error) {
		// Be careful with how |f| is used below. |RefFile| returns a cached
		// os.File pointer so the code needs to use f in a concurrency-safe
		// manner. Moving the file offset is BAD.
		f, err = os.Open(path)
		if err != nil {
			return
		}

		// Since we can't move the file offset, get the size of the file and use
		// ReadAt to load the index instead.
		var fi os.FileInfo
		fi, err = f.Stat()

		if err != nil {
			return
		}

		if fi.Size() < 0 {
			// Size returns the number of bytes for regular files and is system dependent for others (Some of which can be negative).
			err = fmt.Errorf("%s has invalid size: %d", path, fi.Size())
			return
		}

		idxSz := int64(indexSize(chunkCount) + footerSize)
		sz = fi.Size()
		indexOffset := sz - idxSz
		r := io.NewSectionReader(f, indexOffset, idxSz)

		if int64(int(idxSz)) != idxSz {
			err = fmt.Errorf("table file %s is too large to read on this platform. index size %d > max int.", path, idxSz)
			return
		}

		var b []byte
		b, err = q.AcquireQuotaBytes(ctx, int(idxSz))
		if err != nil {
			return
		}

		_, err = io.ReadFull(r, b)
		if err != nil {
			q.ReleaseQuotaBytes(len(b))
			return
		}

		ti, err = parseTableIndex(ctx, b, q)
		if err != nil {
			q.ReleaseQuotaBytes(len(b))
			return
		}

		return
	}()
	if err != nil {
		if f != nil {
			f.Close()
		}
		return nil, err
	}

	if chunkCount != index.chunkCount() {
		index.Close()
		f.Close()
		return nil, errors.New("unexpected chunk count")
	}

	tr, err := newTableReader(index, &fileReaderAt{f, path, sz}, fileBlockSize)
	if err != nil {
		index.Close()
		f.Close()
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

func (ftr *fileTableReader) name() string {
	return ftr.h.String()
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
}

func (fra *fileReaderAt) clone() (tableReaderAt, error) {
	f, err := os.Open(fra.path)
	if err != nil {
		return nil, err
	}
	return &fileReaderAt{
		f,
		fra.path,
		fra.sz,
	}, nil
}

func (fra *fileReaderAt) Close() error {
	return fra.f.Close()
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
