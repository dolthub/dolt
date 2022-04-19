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
)

type fileTableReader struct {
	tableReader
	fc *fdCache
	h  addr
}

const (
	fileBlockSize = 1 << 12
)

func newFileTableReader(dir string, h addr, chunkCount uint32, q MemoryQuotaProvider, fc *fdCache) (cs chunkSource, err error) {
	path := filepath.Join(dir, h.String())

	index, err := func() (ti onHeapTableIndex, err error) {

		// Be careful with how |f| is used below. |RefFile| returns a cached
		// os.File pointer so the code needs to use f in a concurrency-safe
		// manner. Moving the file offset is BAD.
		var f *os.File
		f, err = fc.RefFile(path)
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
			// Size returns the number of bytes for regular files and is system dependant for others (Some of which can be negative).
			err = fmt.Errorf("%s has invalid size: %d", path, fi.Size())
			return
		}

		indexSize := int64(indexSize(chunkCount) + footerSize)
		indexOffset := fi.Size() - indexSize
		r := io.NewSectionReader(f, indexOffset, indexSize)
		b := make([]byte, indexSize)

		_, err = io.ReadFull(r, b)
		if err != nil {
			return
		}

		defer func() {
			unrefErr := fc.UnrefFile(path)

			if unrefErr != nil {
				err = unrefErr
			}
		}()

		ti, err = parseTableIndex(b, q)
		if err != nil {
			return
		}

		return
	}()
	if err != nil {
		return nil, err
	}

	if chunkCount != index.chunkCount {
		return nil, errors.New("unexpected chunk count")
	}

	tr, err := newTableReader(index, &cacheReaderAt{path, fc}, fileBlockSize)
	if err != nil {
		return nil, err
	}
	return &fileTableReader{
		tr,
		fc,
		h,
	}, nil
}

func (mmtr *fileTableReader) hash() (addr, error) {
	return mmtr.h, nil
}

func (mmtr *fileTableReader) Close() error {
	return mmtr.tableReader.Close()
}

func (mmtr *fileTableReader) Clone() (chunkSource, error) {
	tr, err := mmtr.tableReader.Clone()
	if err != nil {
		return &fileTableReader{}, err
	}
	return &fileTableReader{tr, mmtr.fc, mmtr.h}, nil
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
		unrefErr := cra.fc.UnrefFile(cra.path)

		if err == nil {
			err = unrefErr
		}
	}()

	return r.ReadAt(p, off)
}
