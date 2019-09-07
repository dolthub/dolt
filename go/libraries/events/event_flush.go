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

// NewGrpcEventFlusher creates a new GrpcEventFlusher
func NewGrpcEventFlusher(fs filesys.Filesys, userHomeDir string, doltDir string, dEnv *env.DoltEnv) *GrpcEventFlusher {
	fbp := NewFileBackedProc(fs, userHomeDir, doltDir, MD5FileNamer, CheckFilenameMD5)

	if exists := fbp.EventsDirExists(); !exists {
		panic(ErrEventsDataDir)
	}

	return &GrpcEventFlusher{em: getGRPCEmitter(dEnv), fbp: fbp, LockPath: fbp.GetEventsDirPath()}
}

// checkAndFlush is the function that checks that the file are correct
// then it calls the call back on the
type flushFunc func(ctx context.Context, req eventsapi.LogEventsRequest) error

func checkAndFlush(ctx context.Context, path string, flusher Flusher, flush flushFunc) error {
	fs := flusher.fbp.GetFileSys()

	data, err := fs.ReadFile(path)
	if err != nil {
		return err
	}

	isFileValid, err := flusher.fbp.CheckingFunc(data, path)

	if isFileValid && err == nil {
		ctx, cnclFn := context.WithDeadline(ctx, time.Now().Add(time.Minute))
		defer cnclFn()

		req := &eventsapi.LogEventsRequest{}

		if err := proto.Unmarshal(data, req); err != nil {
			return err
		}

		// flush takes context and a proto request
		err := flush(ctx, req)
		if err != nil {
			return err
		}
		// this would be in the flush method on the different flusher
		// if err := flusher.em.SendLogEventsRequest(ctx, req); err != nil {
		// 	return err
		// }

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

	fsLock := filesys.CreateFilesysLock(fs, egf.LockPath)

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
		err := fs.Iter(egf.fbp.GetEventsDirPath(), false, func(path string, size int64, isDir bool) (stop bool) {
			if err := checkAndFlush(ctx, path, egf.flush); err != nil {
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

// func (iof *IOFlusher) flush(ctx context.Context, path string) error {
// 	// do stuff
// }

// func (iof *IOFlusher) Flush(ctx context.Context) error {
// 	// do stuff
// }
