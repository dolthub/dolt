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
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

const tempTablePrefix = "nbs_table_"

func newFSTablePersister(dir string, q MemoryQuotaProvider) tablePersister {
	return &fsTablePersister{dir, q, sync.Mutex{}, nil, make(map[string]struct{})}
}

type fsTablePersister struct {
	dir string
	q   MemoryQuotaProvider

	// Protects the following two maps.
	removeMu sync.Mutex
	// While we are running PruneTableFiles, any newly created table files are
	// added to this map. The file delete loop will never delete anything which
	// appears in this map. Files should be added to this map before they are
	// written.
	toKeep map[string]struct{}
	// Any temp files we are currently writing are always present in this map.
	// The logic should be taken before we generate the new temp file, and the
	// new temp file should be added to this map. Care should be taken to always
	// remove the entry from this map when we are done processing the temp file
	// or else this map will grow without bound.
	curTmps map[string]struct{}
}

var _ tablePersister = &fsTablePersister{}
var _ tableFilePersister = &fsTablePersister{}

func (ftp *fsTablePersister) Open(ctx context.Context, name hash.Hash, chunkCount uint32, stats *Stats) (chunkSource, error) {
	return newFileTableReader(ctx, ftp.dir, name, chunkCount, ftp.q, stats)
}

func (ftp *fsTablePersister) Exists(ctx context.Context, name string, chunkCount uint32, stats *Stats) (bool, error) {
	ftp.removeMu.Lock()
	defer ftp.removeMu.Unlock()
	if ftp.toKeep != nil {
		ftp.toKeep[filepath.Join(ftp.dir, name)] = struct{}{}
	}

	if h, ok := hash.MaybeParse(name); ok {
		exists, err := tableFileExists(ctx, ftp.dir, h)
		if exists || err != nil {
			return exists, err
		}

	}

	return archiveFileExists(ctx, ftp.dir, name)
}

func (ftp *fsTablePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (chunkSource, gcBehavior, error) {
	t1 := time.Now()
	defer stats.PersistLatency.SampleTimeSince(t1)

	name, data, chunkCount, gcb, err := mt.write(haver, keeper, stats)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}
	if gcb != gcBehavior_Continue {
		return emptyChunkSource{}, gcb, nil
	}

	src, err := ftp.persistTable(ctx, name, data, chunkCount, stats)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}
	return src, gcBehavior_Continue, nil
}

func (ftp *fsTablePersister) Path() string {
	return ftp.dir
}

func (ftp *fsTablePersister) CopyTableFile(ctx context.Context, r io.Reader, fileId string, fileSz uint64, chunkCount uint32) error {
	tn, f, err := func() (n string, cleanup func(), err error) {
		ftp.removeMu.Lock()
		var temp *os.File
		temp, err = tempfiles.MovableTempFileProvider.NewFile(ftp.dir, tempTablePrefix)
		if err != nil {
			ftp.removeMu.Unlock()
			return "", func() {}, err
		}
		ftp.curTmps[filepath.Clean(temp.Name())] = struct{}{}
		ftp.removeMu.Unlock()

		cleanup = func() {
			ftp.removeMu.Lock()
			delete(ftp.curTmps, filepath.Clean(temp.Name()))
			ftp.removeMu.Unlock()
		}

		defer func() {
			cerr := temp.Close()
			if cerr != nil {
				err = errors.Join(err, fmt.Errorf("error Closing temp in CopyTableFile: %w", cerr))
			}
		}()

		_, err = io.Copy(temp, r)
		if err != nil {
			return "", cleanup, err
		}

		err = temp.Sync()
		if err != nil {
			return "", cleanup, err
		}

		return temp.Name(), cleanup, nil
	}()
	defer f()
	if err != nil {
		return err
	}

	path := filepath.Join(ftp.dir, fileId)
	ftp.removeMu.Lock()
	if ftp.toKeep != nil {
		ftp.toKeep[filepath.Clean(path)] = struct{}{}
	}
	defer ftp.removeMu.Unlock()
	return file.Rename(tn, path)
}

func (ftp *fsTablePersister) TryMoveCmpChunkTableWriter(ctx context.Context, filename string, w *CmpChunkTableWriter) error {
	path := filepath.Join(ftp.dir, filename)
	ftp.removeMu.Lock()
	if ftp.toKeep != nil {
		ftp.toKeep[filepath.Clean(path)] = struct{}{}
	}
	defer ftp.removeMu.Unlock()
	return w.FlushToFile(path)
}

func (ftp *fsTablePersister) persistTable(ctx context.Context, name hash.Hash, data []byte, chunkCount uint32, stats *Stats) (cs chunkSource, err error) {
	if chunkCount == 0 {
		return emptyChunkSource{}, nil
	}

	tempName, f, err := func() (tempName string, cleanup func(), ferr error) {
		ftp.removeMu.Lock()
		var temp *os.File
		temp, ferr = tempfiles.MovableTempFileProvider.NewFile(ftp.dir, tempTablePrefix)
		if ferr != nil {
			ftp.removeMu.Unlock()
			return "", func() {}, ferr
		}
		ftp.curTmps[filepath.Clean(temp.Name())] = struct{}{}
		ftp.removeMu.Unlock()

		cleanup = func() {
			ftp.removeMu.Lock()
			delete(ftp.curTmps, filepath.Clean(temp.Name()))
			ftp.removeMu.Unlock()
		}

		defer func() {
			cerr := temp.Close()
			if cerr != nil {
				ferr = errors.Join(ferr, fmt.Errorf("error Closing temp in persistTable: %w", cerr))
			}
		}()

		_, ferr = io.Copy(temp, bytes.NewReader(data))
		if ferr != nil {
			return "", cleanup, ferr
		}

		ferr = temp.Sync()
		if ferr != nil {
			return "", cleanup, ferr
		}

		return temp.Name(), cleanup, nil
	}()
	defer f()
	if err != nil {
		return nil, err
	}

	newName := filepath.Join(ftp.dir, name.String())
	ftp.removeMu.Lock()
	if ftp.toKeep != nil {
		ftp.toKeep[filepath.Clean(newName)] = struct{}{}
	}
	err = file.Rename(tempName, newName)
	ftp.removeMu.Unlock()
	if err != nil {
		return nil, err
	}

	return ftp.Open(ctx, name, chunkCount, stats)
}

func (ftp *fsTablePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, cleanupFunc, error) {
	plan, err := planRangeCopyConjoin(sources, stats)
	if err != nil {
		return emptyChunkSource{}, nil, err
	}

	if plan.chunkCount == 0 {
		return emptyChunkSource{}, func() {}, nil
	}

	name := nameFromSuffixes(plan.suffixes())
	tempName, f, err := func() (tempName string, cleanup func(), ferr error) {
		ftp.removeMu.Lock()
		var temp *os.File
		temp, ferr = tempfiles.MovableTempFileProvider.NewFile(ftp.dir, tempTablePrefix)
		if ferr != nil {
			ftp.removeMu.Unlock()
			return "", func() {}, ferr
		}
		ftp.curTmps[filepath.Clean(temp.Name())] = struct{}{}
		ftp.removeMu.Unlock()

		cleanup = func() {
			ftp.removeMu.Lock()
			delete(ftp.curTmps, filepath.Clean(temp.Name()))
			ftp.removeMu.Unlock()
		}

		defer func() {
			closeErr := temp.Close()
			if ferr == nil {
				ferr = closeErr
			}
		}()

		for _, sws := range plan.sources.sws {
			var r io.ReadCloser
			r, _, ferr = sws.source.reader(ctx)
			if ferr != nil {
				return "", cleanup, ferr
			}

			n, ferr := io.CopyN(temp, r, int64(sws.dataLen))
			if ferr != nil {
				r.Close()
				return "", cleanup, ferr
			}

			if uint64(n) != sws.dataLen {
				r.Close()
				return "", cleanup, errors.New("failed to copy all data")
			}

			err := r.Close()
			if err != nil {
				return "", cleanup, err
			}
		}

		_, ferr = temp.Write(plan.mergedIndex)

		if ferr != nil {
			return "", cleanup, ferr
		}

		ferr = temp.Sync()
		if ferr != nil {
			return "", cleanup, ferr
		}

		return temp.Name(), cleanup, nil
	}()
	defer f()
	if err != nil {
		return nil, nil, err
	}

	path := filepath.Join(ftp.dir, name.String())
	ftp.removeMu.Lock()
	if ftp.toKeep != nil {
		ftp.toKeep[filepath.Clean(path)] = struct{}{}
	}
	err = file.Rename(tempName, path)
	if err != nil {
		return nil, nil, err
	}
	ftp.removeMu.Unlock()

	cs, err := ftp.Open(ctx, name, plan.chunkCount, stats)
	if err != nil {
		return nil, nil, err
	}
	return cs, func() {
		for _, s := range sources {
			file.Remove(filepath.Join(ftp.dir, s.hash().String()))
		}
	}, nil
}

func (ftp *fsTablePersister) PruneTableFiles(ctx context.Context, keeper func() []hash.Hash, mtime time.Time) error {
	ftp.removeMu.Lock()
	if ftp.toKeep != nil {
		ftp.removeMu.Unlock()
		return errors.New("shallow gc already in progress")
	}
	ftp.toKeep = make(map[string]struct{})
	ftp.removeMu.Unlock()

	defer func() {
		ftp.removeMu.Lock()
		ftp.toKeep = nil
		ftp.removeMu.Unlock()
	}()

	toKeep := make(map[string]struct{})
	for _, k := range keeper() {
		toKeep[filepath.Clean(filepath.Join(ftp.dir, k.String()))] = struct{}{}
	}

	ftp.removeMu.Lock()
	for f := range toKeep {
		ftp.toKeep[f] = struct{}{}
	}
	ftp.removeMu.Unlock()

	fileInfos, err := os.ReadDir(ftp.dir)
	if err != nil {
		return err
	}

	ea := make(gcErrAccum)

	unfilteredTableFiles := make([]string, 0)
	unfilteredTempFiles := make([]string, 0)

	isTableFileName := func(name string) bool {
		if strings.HasSuffix(name, ArchiveFileSuffix) {
			name = strings.TrimSuffix(name, ArchiveFileSuffix)
		}
		if len(name) != 32 {
			return false
		}
		_, ok := hash.MaybeParse(name)
		return ok
	}

	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}

		filePath := path.Join(ftp.dir, info.Name())

		if strings.HasPrefix(info.Name(), tempTablePrefix) {
			unfilteredTempFiles = append(unfilteredTempFiles, filePath)
			continue
		}

		if !isTableFileName(info.Name()) {
			continue
		}

		i, err := info.Info()
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				ea.add(filePath, err)
			}
			continue
		}

		ctime := i.ModTime()
		if ctime.After(mtime) {
			continue // file has been updated more recently than our cutoff time
		}

		unfilteredTableFiles = append(unfilteredTableFiles, filePath)
	}

	for _, p := range unfilteredTempFiles {
		ftp.removeMu.Lock()
		if _, ok := ftp.curTmps[filepath.Clean(p)]; !ok {
			err := file.Remove(p)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				ea.add(p, fmt.Errorf("error file.Remove unfilteredTempFiles: %w", err))
			}
		}
		ftp.removeMu.Unlock()
	}

	for _, p := range unfilteredTableFiles {
		ftp.removeMu.Lock()
		_, exists := ftp.toKeep[filepath.Clean(p)]
		_, archiveExists := ftp.toKeep[filepath.Clean(p + ArchiveFileSuffix)]
		_, trimmedExists := ftp.toKeep[filepath.Clean(strings.TrimSuffix(p, ArchiveFileSuffix))]
		if !exists && !archiveExists && !trimmedExists {
			err := file.Remove(p)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				ea.add(p, fmt.Errorf("error file.Remove unfilteredTableFiles: %w", err))
			}
		}
		ftp.removeMu.Unlock()
	}

	if !ea.isEmpty() {
		return ea
	}

	return nil
}

func (ftp *fsTablePersister) Close() error {
	return nil
}

func (ftp *fsTablePersister) AccessMode() chunks.ExclusiveAccessMode {
	return chunks.ExclusiveAccessMode_Shared
}
