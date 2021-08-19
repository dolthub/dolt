// Copyright 2021 Dolthub, Inc.
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

package edits

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/types"
)

var _ types.EditAccumulator = (*DiskBackedEditAcc)(nil)

// DiskBackedEditAcc is an EditAccumulator implementation that flushes the edits to disk at regular intervals
type DiskBackedEditAcc struct {
	ctx        context.Context
	nbf        *types.NomsBinFormat
	vrw        types.ValueReadWriter
	directory  string
	newEditAcc func() types.EditAccumulator

	backing types.EditAccumulator

	accumulated   int64
	flushInterval int64

	ae *atomicerr.AtomicError
	wg *sync.WaitGroup

	files      chan string
	flushCount int
}

// NewDiskBackedEditAcc returns a new DiskBackedEditAccumulator instance
func NewDiskBackedEditAcc(ctx context.Context, nbf *types.NomsBinFormat, vrw types.ValueReadWriter, flushInterval int64, directory string, newEditAcc func() types.EditAccumulator) *DiskBackedEditAcc {
	return &DiskBackedEditAcc{
		ctx:           ctx,
		nbf:           nbf,
		vrw:           vrw,
		directory:     directory,
		newEditAcc:    newEditAcc,
		backing:       newEditAcc(),
		flushInterval: flushInterval,
		ae:            atomicerr.New(),
		wg:            &sync.WaitGroup{},
		files:         make(chan string, 32),
	}
}

// AddEdit adds an edit. Not thread safe
func (dbea *DiskBackedEditAcc) AddEdit(key types.LesserValuable, val types.Valuable) {
	dbea.backing.AddEdit(key, val)
	dbea.accumulated++

	if dbea.accumulated%dbea.flushInterval == 0 {
		// flush interval reached.  kick off a background routine to process everything
		dbea.flushCount++
		dbea.flushToDisk()
		dbea.backing = dbea.newEditAcc()
	}
}

// FinishedEditing should be called when all edits have been added to get an EditProvider which provides the
// edits in sorted order. Adding more edits after calling FinishedEditing is an error.
func (dbea *DiskBackedEditAcc) FinishedEditing() (types.EditProvider, error) {
	if err := dbea.ae.Get(); err != nil {
		return nil, err
	}

	// If we never flushed to disk then there is no need.  Just return the data from the backing edit accumulator
	if dbea.flushCount == 0 {
		return dbea.backing.FinishedEditing()
	}

	// If there are no background errors, flush any data we haven't flushed yet before processing
	if !dbea.ae.IsSet() {
		sinceLastFlush := dbea.accumulated%dbea.flushInterval
		if sinceLastFlush > 0 {
			dbea.flushCount++
			dbea.flushToDisk()
			dbea.backing = nil
		}
	}

	// spawn a routine to watch the flush routines and close the result channel once they have all closed
	go func() {
		dbea.wg.Wait()
		close(dbea.files)
	}()

	// stream all the results off the result channel even if an error has occurred
	var files []string
	for flushedFile := range dbea.files {
		files = append(files, flushedFile)
	}

	if err := dbea.ae.Get(); err != nil {
		tryDeleteFiles(files)
		return nil, err
	}

	return EditProviderForFiles(dbea.ctx, dbea.nbf, dbea.vrw, files, dbea.accumulated, true)
}

// Close ensures that the accumulator is closed. Repeat calls are allowed. Not guaranteed to be thread-safe, thus
// requires external synchronization.
func (dbea *DiskBackedEditAcc) Close() {
	if dbea.backing != nil {
		dbea.backing.Close()
		dbea.backing = nil
	}
}

func (dbea *DiskBackedEditAcc) flushToDisk() {
	// spawn a go routine to flush in the background
	dbea.wg.Add(1)
	go func(acc types.EditAccumulator, ae *atomicerr.AtomicError) {
		defer dbea.wg.Done()

		if ae.IsSet() {
			return
		}

		itr, err := acc.FinishedEditing()
		if ae.SetIfErrAndCheck(err) {
			return
		}

		path, wr, err := dbea.openTupleWriter()
		if ae.SetIfErrAndCheck(err) {
			return
		}

		err = flushKVPs(wr, itr)
		ae.SetIfError(err)
		err = dbea.closeTupleWriter(path, wr, ae.Get())

		if err != nil {
			ae.SetIfError(err)
			return
		}

		dbea.files <- path
	}(dbea.backing, dbea.ae)
}

func (dbea *DiskBackedEditAcc) openTupleWriter() (string, types.TupleWriteCloser, error) {
	absPath := filepath.Join(dbea.directory, uuid.New().String())
	f, err := os.OpenFile(absPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)

	if err != nil {
		return "", nil, err
	}

	return absPath, types.NewTupleWriter(f), nil
}

func (dbea *DiskBackedEditAcc) closeTupleWriter(absPath string, wr types.TupleWriteCloser, err error) error {
	closeErr := wr.Close(dbea.ctx)

	if err != nil || closeErr != nil {
		if err == nil {
			err = closeErr
		}

		// an error occurred writing. Best effort deletion
		_ = os.Remove(absPath)
		return err
	}

	return nil
}

func flushKVPs(wr types.TupleWriter, itr types.EditProvider) error {
	// iterate over all kvps writing the key followed by the value
	for {
		kvp, err := itr.Next()

		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		k := kvp.Key.(types.Tuple)
		v := kvp.Val.(types.Tuple)
		err = wr.WriteTuples(k, v)
		if err != nil {
			return err
		}
	}
}

// best effort deletion ignores errors
func tryDeleteFiles(paths []string) {
	for _, path := range paths {
		_ = os.Remove(path)
	}
}
