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
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

// EventGrpcFlush parses dolt event logs sends the events to the events server
type EventGrpcFlush struct {
	em       *GrpcEmitter
	fbp      *FileBackedProc
	LockPath string
}

// ErrEventsDataDir occurs when events are trying to be  flushed, but the events data directory
// does not yet exist
var ErrEventsDataDir = errors.New("unable to flush, events data directory does not exist")

var errInvalidFile = errors.New("unable to flush, invalid file")

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

// NewEventGrpcFlush creates a new EventGrpcFlush
func NewEventGrpcFlush(fs filesys.Filesys, userHomeDir string, doltDir string, dEnv *env.DoltEnv) *EventGrpcFlush {
	fbp := NewFileBackedProc(fs, userHomeDir, doltDir, MD5FileNamer, CheckFilenameMD5)

	if exists := fbp.EventsDirExists(); !exists {
		panic(ErrEventsDataDir)
	}

	return &EventGrpcFlush{em: getGRPCEmitter(dEnv), fbp: fbp, LockPath: fbp.GetEventsDirPath()}
}

// flush sends the events requests from the files to the grpc server
func (egf *EventGrpcFlush) flush(ctx context.Context, path string) error {
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

// FlushEvents sends event logs to the events server
func (egf *EventGrpcFlush) FlushEvents(ctx context.Context) error {
	fs := egf.fbp.GetFileSys()

	err := fs.Iter(egf.fbp.GetEventsDirPath(), false, func(path string, size int64, isDir bool) (stop bool) {
		if err := egf.flush(ctx, path); err != nil {
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
