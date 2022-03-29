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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

const tempTablePrefix = "nbs_table_"

func newFSTablePersister(dir string, fc *fdCache, q MemoryQuotaProvider) tablePersister {
	d.PanicIfTrue(fc == nil)
	return &fsTablePersister{dir, fc, q}
}

type fsTablePersister struct {
	dir string
	fc  *fdCache
	q   MemoryQuotaProvider
}

func (ftp *fsTablePersister) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	return newMmapTableReader(ftp.dir, name, chunkCount, ftp.q, ftp.fc)
}

func (ftp *fsTablePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	name, data, chunkCount, err := mt.write(haver, stats)

	if err != nil {
		return emptyChunkSource{}, err
	}

	return ftp.persistTable(ctx, name, data, chunkCount, stats)
}

func (ftp *fsTablePersister) persistTable(ctx context.Context, name addr, data []byte, chunkCount uint32, stats *Stats) (cs chunkSource, err error) {
	if chunkCount == 0 {
		return emptyChunkSource{}, nil
	}

	tempName, err := func() (tempName string, ferr error) {
		var temp *os.File
		temp, ferr = tempfiles.MovableTempFileProvider.NewFile(ftp.dir, tempTablePrefix)

		if ferr != nil {
			return "", ferr
		}

		defer func() {
			closeErr := temp.Close()

			if ferr == nil {
				ferr = closeErr
			}
		}()

		_, ferr = io.Copy(temp, bytes.NewReader(data))
		if ferr != nil {
			return "", ferr
		}

		return temp.Name(), nil
	}()

	if err != nil {
		return nil, err
	}

	newName := filepath.Join(ftp.dir, name.String())
	err = ftp.fc.ShrinkCache()

	if err != nil {
		return nil, err
	}

	err = file.Rename(tempName, newName)

	if err != nil {
		return nil, err
	}

	return ftp.Open(ctx, name, chunkCount, stats)
}

func (ftp *fsTablePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	plan, err := planConjoin(sources, stats)

	if err != nil {
		return emptyChunkSource{}, err
	}

	if plan.chunkCount == 0 {
		return emptyChunkSource{}, nil
	}

	name := nameFromSuffixes(plan.suffixes())
	tempName, err := func() (tempName string, ferr error) {
		var temp *os.File
		temp, ferr = tempfiles.MovableTempFileProvider.NewFile(ftp.dir, tempTablePrefix)

		if ferr != nil {
			return "", ferr
		}

		defer func() {
			closeErr := temp.Close()

			if ferr == nil {
				ferr = closeErr
			}
		}()

		for _, sws := range plan.sources.sws {
			var r io.Reader
			r, ferr = sws.source.reader(ctx)

			if ferr != nil {
				return "", ferr
			}

			n, ferr := io.CopyN(temp, r, int64(sws.dataLen))

			if ferr != nil {
				return "", ferr
			}

			if uint64(n) != sws.dataLen {
				return "", errors.New("failed to copy all data")
			}
		}

		_, ferr = temp.Write(plan.mergedIndex)

		if ferr != nil {
			return "", ferr
		}

		return temp.Name(), nil
	}()

	if err != nil {
		return nil, err
	}

	err = file.Rename(tempName, filepath.Join(ftp.dir, name.String()))

	if err != nil {
		return nil, err
	}

	return ftp.Open(ctx, name, plan.chunkCount, stats)
}

func (ftp *fsTablePersister) PruneTableFiles(ctx context.Context, contents manifestContents) error {
	ss := contents.getSpecSet()

	fileInfos, err := os.ReadDir(ftp.dir)

	if err != nil {
		return err
	}

	err = ftp.fc.ShrinkCache()

	if err != nil {
		return err
	}

	ea := make(gcErrAccum)
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}

		filePath := path.Join(ftp.dir, info.Name())

		if strings.HasPrefix(info.Name(), tempTablePrefix) {
			err = file.Remove(filePath)
			if err != nil {
				ea.add(filePath, err)
			}
			continue
		}

		if len(info.Name()) != 32 {
			continue // not a table file
		}

		addy, err := parseAddr(info.Name())
		if err != nil {
			continue // not a table file
		}

		if _, ok := ss[addy]; ok {
			continue // file is referenced in the manifest
		}

		err = file.Remove(filePath)
		if err != nil {
			ea.add(filePath, err)
		}
	}

	if !ea.isEmpty() {
		return ea
	}

	return nil
}
