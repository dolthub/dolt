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

package events

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/fslock"
	"github.com/fatih/color"
	"google.golang.org/protobuf/proto"

	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var (
	// ErrEventsDataDir occurs when events are trying to be  flushed, but the events data directory
	// does not yet exist
	ErrEventsDataDir = errors.New("unable to flush, events data directory does not exist")

	// ErrFileLocked occurs if the current file or dir is locked for processing
	ErrFileLocked = errors.New("file is currently locked")

	// errInvalidFile occurs if the filename fails the CheckingFunc
	errInvalidFile = errors.New("unable to flush, invalid file")
)

// flushCB is the signature of the callback used to process event files
type flushCB func(ctx context.Context, path string) error

// Flusher flushes events to a destination
type Flusher interface {
	Flush(ctx context.Context) error
}

// lockAndFlush locks the given lockPath and passes the flushCB to the filesys' Iter method
func lockAndFlush(ctx context.Context, fs filesys.Filesys, dirPath string, lockPath string, fcb flushCB) error {
	fsLock := filesys.CreateFilesysLock(fs, lockPath)

	isUnlocked, err := fsLock.TryLock()
	defer func() error {
		err := fsLock.Unlock()
		if err != nil {
			return err
		}
		return nil
	}()

	if err != nil {
		if err == fslock.ErrLocked {
			return ErrFileLocked
		}
		return err
	}

	if isUnlocked && err == nil {
		err := fs.Iter(dirPath, false, func(path string, size int64, isDir bool) (stop bool) {
			if err := fcb(ctx, path); err != nil {
				// log.Print(err)
				return false
			}

			return false
		})

		if err != nil {
			return err
		}

		return nil
	}

	return nil
}

// GrpcEventFlusher parses dolt event logs sends the events to the events server
type GrpcEventFlusher struct {
	em  *GrpcEmitter
	fbp *FileBackedProc
}

// NewGrpcEventFlusher creates a new GrpcEventFlusher
func NewGrpcEventFlusher(fs filesys.Filesys, userHomeDir string, doltDir string, grpcEmitter *GrpcEmitter) *GrpcEventFlusher {
	fbp := NewFileBackedProc(fs, userHomeDir, doltDir, MD5FileNamer, CheckFilenameMD5)

	if exists := fbp.EventsDirExists(); !exists {
		panic(ErrEventsDataDir)
	}

	return &GrpcEventFlusher{em: grpcEmitter, fbp: fbp}
}

// flush has the function signature of the flushCb type
// and sends events data to the events server
func (egf *GrpcEventFlusher) flush(ctx context.Context, path string) error {
	fs := egf.fbp.GetFileSys()

	data, err := fs.ReadFile(path)
	if err != nil {
		return err
	}

	isFileValid, err := egf.fbp.CheckingFunc(data, path)

	if isFileValid && err == nil {
		req := &eventsapi.LogEventsRequest{}

		if err := proto.Unmarshal(data, req); err != nil {
			return err
		}

		if err := egf.em.SendLogEventsRequest(ctx, req); err != nil {
			return err
		}

		if err := fs.DeleteFile(path); err != nil {
			return err
		}

		return nil
	}

	return errInvalidFile
}

// Flush satisfies the Flusher interface and calls this Flusher's flush method on each events file
func (egf *GrpcEventFlusher) Flush(ctx context.Context) error {
	fs := egf.fbp.GetFileSys()

	evtsDir := egf.fbp.GetEventsDirPath()

	err := lockAndFlush(ctx, fs, evtsDir, egf.fbp.LockPath, egf.flush)
	if err != nil {
		return err
	}

	return nil
}

// IOFlusher parses event files and writes them to stdout
type IOFlusher struct {
	fbp *FileBackedProc
}

// NewIOFlusher creates a new IOFlusher
func NewIOFlusher(fs filesys.Filesys, userHomeDir string, doltDir string) *IOFlusher {
	fbp := NewFileBackedProc(fs, userHomeDir, doltDir, MD5FileNamer, CheckFilenameMD5)

	if exists := fbp.EventsDirExists(); !exists {
		panic(ErrEventsDataDir)
	}

	return &IOFlusher{fbp: fbp}
}

// flush has the function signature of the flushCb type
// and writes data to stdout
func (iof *IOFlusher) flush(ctx context.Context, path string) error {
	fs := iof.fbp.GetFileSys()

	data, err := fs.ReadFile(path)
	if err != nil {
		return err
	}

	req := &eventsapi.LogEventsRequest{}

	if err := proto.Unmarshal(data, req); err != nil {
		return err
	}

	// needed for bats test
	fmt.Fprintf(color.Output, "%+v\n", req)

	if err := fs.DeleteFile(path); err != nil {
		return err
	}

	return nil
}

// Flush satisfies the Flusher interface and calls this Flusher's flush method on each events file
func (iof *IOFlusher) Flush(ctx context.Context) error {
	fs := iof.fbp.GetFileSys()

	evtsDir := iof.fbp.GetEventsDirPath()

	err := lockAndFlush(ctx, fs, evtsDir, iof.fbp.LockPath, iof.flush)
	if err != nil {
		return err
	}

	return nil
}
