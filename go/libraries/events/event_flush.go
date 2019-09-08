// Copyright 2019 Liquidata, Inc.
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
	"log"
	"strconv"
	"time"

	"github.com/fatih/color"
	"github.com/golang/protobuf/proto"
	"github.com/juju/fslock"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

var (
	// ErrEventsDataDir occurs when events are trying to be  flushed, but the events data directory
	// does not yet exist
	ErrEventsDataDir = errors.New("unable to flush, events data directory does not exist")

	ErrFileLocked  = errors.New("file is currently locked")
	errInvalidFile = errors.New("unable to flush, invalid file")
)

type Flusher interface {
	Flush(ctx context.Context) error
}

// GrpcEventFlusher parses dolt event logs sends the events to the events server
type GrpcEventFlusher struct {
	em       *GrpcEmitter
	fbp      *FileBackedProc
	LockPath string
}

// flushCB is the signature of the callback used on each file of the given path
type flushCB func(ctx context.Context, path string) error

// getGRPCEmitter gets the connection to the events grpc service
func getGRPCEmitter(dEnv *env.DoltEnv) *GrpcEmitter {
	host := dEnv.Config.GetStringOrDefault(env.MetricsHost, env.DefaultMetricsHost)
	portStr := dEnv.Config.GetStringOrDefault(env.MetricsPort, env.DefaultMetricsPort)
	insecureStr := dEnv.Config.GetStringOrDefault(env.MetricsInsecure, "false")

	port, err := strconv.ParseUint(*portStr, 10, 16)

	if err != nil {
		log.Println(color.YellowString("The config value of '%s' is '%s' which is not a valid port.", env.MetricsPort, *portStr))
		return nil
	}

	insecure, err := strconv.ParseBool(*insecureStr)

	if err != nil {
		log.Println(color.YellowString("The config value of '%s' is '%s' which is not a valid true/false value", env.MetricsInsecure, *insecureStr))
	}

	hostAndPort := fmt.Sprintf("%s:%d", *host, port)
	conn, _ := dEnv.GrpcConnWithCreds(hostAndPort, insecure, nil)

	return NewGrpcEmitter(conn)
}

// lockAndFlush locks the given lock path and passes the flushCB to the filesys' Iter method
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

	if !isUnlocked && err != nil {
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

	if err != nil {
		return err
	}

	return nil
}

// NewGrpcEventFlusher creates a new GrpcEventFlusher
func NewGrpcEventFlusher(fs filesys.Filesys, userHomeDir string, doltDir string, dEnv *env.DoltEnv) *GrpcEventFlusher {
	fbp := NewFileBackedProc(fs, userHomeDir, doltDir, MD5FileNamer, CheckFilenameMD5)

	if exists := fbp.EventsDirExists(); !exists {
		panic(ErrEventsDataDir)
	}

	return &GrpcEventFlusher{em: getGRPCEmitter(dEnv), fbp: fbp, LockPath: fbp.GetEventsDirPath()}
}

// flush has the function signature of the flushCb type
func (egf *GrpcEventFlusher) flush(ctx context.Context, path string) error {
	fs := egf.fbp.GetFileSys()

	data, err := fs.ReadFile(path)
	if err != nil {
		return err
	}

	isFileValid, err := egf.fbp.CheckingFunc(data, path)

	if isFileValid && err == nil {
		ctx, cnclFn := context.WithDeadline(ctx, time.Now().Add(time.Minute))
		defer cnclFn()

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

// Flush sends event logs to the events server
func (egf *GrpcEventFlusher) Flush(ctx context.Context) error {
	fs := egf.fbp.GetFileSys()

	evtsDir := egf.fbp.GetEventsDirPath()

	err := lockAndFlush(ctx, fs, evtsDir, egf.LockPath, egf.flush)
	if err != nil {
		return err
	}

	return nil
}

type IOFlusher struct {
	fbp      *FileBackedProc
	LockPath string
}

func NewIOFlusher(fs filesys.Filesys, userHomeDir string, doltDir string, dEnv *env.DoltEnv) *IOFlusher {
	fbp := NewFileBackedProc(fs, userHomeDir, doltDir, MD5FileNamer, CheckFilenameMD5)

	if exists := fbp.EventsDirExists(); !exists {
		panic(ErrEventsDataDir)
	}
	return &IOFlusher{fbp: fbp, LockPath: fbp.GetEventsDirPath()}
}

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

	// is this the correct output format?
	fmt.Fprintf(color.Output, "%+v\n", req)

	// do  we  want to delete the file in  this format?
	if err := fs.DeleteFile(path); err != nil {
		return err
	}

	return nil
}

func (iof *IOFlusher) Flush(ctx context.Context) error {
	fs := iof.fbp.GetFileSys()

	evtsDir := iof.fbp.GetEventsDirPath()

	err := lockAndFlush(ctx, fs, evtsDir, iof.LockPath, iof.flush)
	if err != nil {
		return err
	}

	return nil
}
