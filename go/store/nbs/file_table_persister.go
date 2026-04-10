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

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

const tempTablePrefix = "nbs_table_"

func newFSTablePersister(dir string, q MemoryQuotaProvider, mmapArchiveIndexes bool) tablePersister {
	return &fsTablePersister{
		q:                  q,
		protected:          make(map[hash.Hash]int32),
		dir:                dir,
		mmapArchiveIndexes: mmapArchiveIndexes,
	}
}

type fsTablePersister struct {
	q                  MemoryQuotaProvider
	dir                string
	mmapArchiveIndexes bool
	// protected is a ref-counted set of file hashes that must not be
	// pruned. Refs are added by Open (and clone/addRef) and by
	// file-landing methods (CopyTableFile, writeAndProtect, Exists). Refs
	// are removed by close/decRef and by closing the handle returned
	// from file-landing methods.
	protected map[hash.Hash]int32
	// mu protects the protected map from concurrent access.
	mu sync.Mutex
	// pruneMu serializes file operations with PruneTableFiles.
	// File-landing methods and Open take the read lock.
	// PruneTableFiles takes the write lock.
	pruneMu sync.RWMutex

	// test hook: called in ConjoinAll after Rename but before Open.
	_testFtpConjoinAfterRenameHook func()
}

func (ftp *fsTablePersister) addProtected(h hash.Hash) {
	ftp.mu.Lock()
	defer ftp.mu.Unlock()
	ftp.protected[h]++
}

func (ftp *fsTablePersister) removeProtected(h hash.Hash) {
	ftp.mu.Lock()
	defer ftp.mu.Unlock()
	if ftp.protected[h] <= 1 {
		delete(ftp.protected, h)
	} else {
		ftp.protected[h]--
	}
}

// pendingHandle is returned by file-landing methods. Closing it removes a
// ref from the protected set, allowing the file to be pruned once all refs
// are gone. Safe to double-close.
type pendingHandle struct {
	once sync.Once
	ftp  *fsTablePersister
	h    hash.Hash
}

func (ph *pendingHandle) Close() error {
	ph.once.Do(func() {
		ph.ftp.pruneMu.RLock()
		defer ph.ftp.pruneMu.RUnlock()
		ph.ftp.removeProtected(ph.h)
	})
	return nil
}

func (ftp *fsTablePersister) addPending(h hash.Hash) *pendingHandle {
	ftp.addProtected(h)
	return &pendingHandle{ftp: ftp, h: h}
}

var _ tablePersister = &fsTablePersister{}
var _ tableFilePersister = &fsTablePersister{}
var _ movingTableFilePersister = &fsTablePersister{}

type refCounter interface {
	decRef()
	addRef()
}

type noopRefCounter struct{}

func (noopRefCounter) decRef() {}
func (noopRefCounter) addRef() {}

type fsTablePersisterRefCounter struct {
	ftp  *fsTablePersister
	name hash.Hash
}

func (ftplc *fsTablePersisterRefCounter) decRef() {
	ftplc.ftp.pruneMu.RLock()
	defer ftplc.ftp.pruneMu.RUnlock()
	ftplc.ftp.removeProtected(ftplc.name)
}

func (ftplc *fsTablePersisterRefCounter) addRef() {
	ftplc.ftp.pruneMu.RLock()
	defer ftplc.ftp.pruneMu.RUnlock()
	ftplc.ftp.addProtected(ftplc.name)
}

func (ftp *fsTablePersister) Open(ctx context.Context, name hash.Hash, chunkCount uint32, stats *Stats) (chunkSource, error) {
	ftp.pruneMu.RLock()
	defer ftp.pruneMu.RUnlock()
	rc := fsTablePersisterRefCounter{ftp, name}
	cs, err := newFileTableReader(ctx, ftp.dir, name, chunkCount, ftp.q, ftp.mmapArchiveIndexes, &rc, stats)
	if err != nil {
		return nil, err
	}
	ftp.addProtected(name)
	return cs, nil
}

func (ftp *fsTablePersister) Exists(ctx context.Context, name string, chunkCount uint32, stats *Stats) (bool, io.Closer, error) {
	ftp.pruneMu.RLock()
	defer ftp.pruneMu.RUnlock()

	if h, ok := hash.MaybeParse(name); ok {
		exists, err := tableFileExists(ctx, ftp.dir, h)
		if err != nil {
			return false, nil, err
		}
		if exists {
			return true, ftp.addPending(h), nil
		}
	}

	exists, err := archiveFileExists(ctx, ftp.dir, name)
	if err != nil {
		return false, nil, err
	}
	if exists {
		h, ok := fileNameToAddr(name)
		if !ok {
			return false, nil, fmt.Errorf("invalid file name: %s", name)
		}
		return true, ftp.addPending(h), nil
	}
	return false, nil, nil
}

func (ftp *fsTablePersister) Persist(ctx context.Context, behavior dherrors.FatalBehavior, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (chunkSource, gcBehavior, error) {
	t1 := time.Now()
	defer stats.PersistLatency.SampleTimeSince(t1)

	name, data, _, chunkCount, gcb, err := mt.write(haver, keeper, stats)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}
	if gcb != gcBehavior_Continue {
		return emptyChunkSource{}, gcb, nil
	}

	src, err := ftp.persistTable(ctx, behavior, name, data, chunkCount, stats)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}
	return src, gcBehavior_Continue, nil
}

func (ftp *fsTablePersister) Path() string {
	return ftp.dir
}

func (ftp *fsTablePersister) CopyTableFile(_ context.Context, r io.Reader, fileId string, _ uint64, _ uint64) (io.Closer, error) {
	return ftp.writeAndProtect(fileId, func(temp *os.File) error {
		if _, err := io.Copy(temp, r); err != nil {
			return err
		}
		return temp.Sync()
	})
}

func (ftp *fsTablePersister) TryMoveCmpChunkTableWriter(ctx context.Context, filename string, w GenericTableWriter) (io.Closer, error) {
	addr, ok := fileNameToAddr(filename)
	if !ok {
		return nil, fmt.Errorf("invalid filename for TryMoveCmpChunkTableWriter: %s", filename)
	}

	ftp.pruneMu.RLock()
	defer ftp.pruneMu.RUnlock()
	if err := w.FlushToFile(filepath.Join(ftp.dir, filename)); err != nil {
		return nil, err
	}
	return ftp.addPending(addr), nil
}

// writeAndProtect creates a temp file in ftp.dir, calls writeFn to populate it,
// renames it to its final name, and adds it to the pending set. The returned
// handle must be closed after the file has been Open'd or is no longer needed.
// The pruneMu read lock is held for the entire operation.
func (ftp *fsTablePersister) writeAndProtect(finalName string, writeFn func(temp *os.File) error) (*pendingHandle, error) {
	addr, ok := fileNameToAddr(finalName)
	if !ok {
		return nil, fmt.Errorf("invalid filename: %s", finalName)
	}

	ftp.pruneMu.RLock()
	defer ftp.pruneMu.RUnlock()

	temp, err := tempfiles.MovableTempFileProvider.NewFile(ftp.dir, tempTablePrefix)
	if err != nil {
		return nil, err
	}

	if err = writeFn(temp); err != nil {
		_ = temp.Close()
		return nil, err
	}
	if err = temp.Close(); err != nil {
		return nil, err
	}
	if err = file.Rename(temp.Name(), filepath.Join(ftp.dir, finalName)); err != nil {
		return nil, err
	}
	return ftp.addPending(addr), nil
}

func (ftp *fsTablePersister) persistTable(ctx context.Context, behavior dherrors.FatalBehavior, name hash.Hash, data []byte, chunkCount uint32, stats *Stats) (cs chunkSource, err error) {
	if chunkCount == 0 {
		return emptyChunkSource{}, nil
	}

	ph, err := ftp.writeAndProtect(name.String(), func(temp *os.File) error {
		if _, err := io.Copy(temp, bytes.NewReader(data)); err != nil {
			return err
		}
		return temp.Sync()
	})
	if err != nil {
		return nil, err
	}
	defer ph.Close()

	return ftp.Open(ctx, name, chunkCount, stats)
}

func (ftp *fsTablePersister) ConjoinAll(ctx context.Context, behavior dherrors.FatalBehavior, sources chunkSources, stats *Stats) (chunkSource, cleanupFunc, error) {
	plan, err := planRangeCopyConjoin(ctx, sources, ftp.q, stats)
	if err != nil {
		return emptyChunkSource{}, nil, err
	}
	defer plan.closer()

	if plan.chunkCount == 0 {
		return emptyChunkSource{}, func() {}, nil
	}

	ph, err := ftp.writeAndProtect(plan.name.String()+plan.suffix, func(temp *os.File) error {
		for _, sws := range plan.sources.sws {
			r, _, err := sws.source.reader(ctx, behavior)
			if err != nil {
				return err
			}

			n, err := io.CopyN(temp, r, int64(sws.dataLen))
			if err != nil {
				r.Close()
				return err
			}

			if uint64(n) != sws.dataLen {
				r.Close()
				return errors.New("failed to copy all data")
			}

			if err := r.Close(); err != nil {
				return err
			}
		}

		if _, err := temp.Write(plan.mergedIndex); err != nil {
			return err
		}
		return temp.Sync()
	})
	if err != nil {
		return nil, nil, err
	}
	if ftp._testFtpConjoinAfterRenameHook != nil {
		ftp._testFtpConjoinAfterRenameHook()
	}
	defer ph.Close()

	cs, err := ftp.Open(ctx, plan.name, plan.chunkCount, stats)
	if err != nil {
		return nil, nil, err
	}
	return cs, func() {
		ftp.pruneMu.Lock()
		defer ftp.pruneMu.Unlock()
		for _, s := range sources {
			h := s.hash()
			if ftp.protected[h] > 0 {
				continue
			}
			file.Remove(filepath.Join(ftp.dir, h.String()+s.suffix()))
		}
	}, nil
}

func (ftp *fsTablePersister) PruneTableFiles(ctx context.Context) error {
	ftp.pruneMu.Lock()
	defer ftp.pruneMu.Unlock()

	fileInfos, err := os.ReadDir(ftp.dir)
	if err != nil {
		return err
	}

	var errs []error

	parseTableFileHash := func(name string) (hash.Hash, bool) {
		name = strings.TrimSuffix(name, ArchiveFileSuffix)
		if len(name) != 32 {
			return hash.Hash{}, false
		}
		return hash.MaybeParse(name)
	}

	// pruneMu write lock guarantees no concurrent file-landing or Open,
	// so openFiles and pending cannot be modified while we iterate.
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}

		name := info.Name()
		filePath := path.Join(ftp.dir, name)

		if strings.HasPrefix(name, tempTablePrefix) {
			// Write lock guarantees no temp files are in flight.
			if err := file.Remove(filePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, fmt.Errorf("error removing temp file %s: %w", filePath, err))
			}
			continue
		}

		h, ok := parseTableFileHash(name)
		if !ok {
			continue
		}

		if ftp.protected[h] > 0 {
			continue
		}

		if err := file.Remove(filePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("error removing table file %s: %w", filePath, err))
		}
	}

	return errors.Join(errs...)
}

func (ftp *fsTablePersister) Close() error {
	return nil
}

func (ftp *fsTablePersister) AccessMode() chunks.ExclusiveAccessMode {
	return chunks.ExclusiveAccessMode_Shared
}
