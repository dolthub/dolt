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
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
)

// Emitter is an interface used for processing a batch of events
type Emitter interface {
	// LogEvents takes a batch of events and processes them
	LogEvents(version string, evts []*eventsapi.ClientEvent) error
}

// NullEmitter is an emitter that drops events
type NullEmitter struct{}

// LogEvents takes a batch of events and processes them.  In this case it just drops them
func (ne NullEmitter) LogEvents(version string, evts []*eventsapi.ClientEvent) error {
	return nil
}

// WriterEmitter is an emitter that writes the text encoding of the events to it's writer
type WriterEmitter struct {
	// Wr the writer to log events to
	Wr io.Writer
}

// LogEvents takes a batch of events and processes them.  In this case the text encoding of the events is written to
// the writer
func (we WriterEmitter) LogEvents(version string, evts []*eventsapi.ClientEvent) error {
	for i, evt := range evts {
		header := fmt.Sprintf("event%03d: <\n", i)

		err := iohelp.WriteAll(we.Wr, []byte(header))

		if err != nil {
			return err
		}

		str := proto.MarshalTextString(evt)
		tokens := strings.Split(strings.TrimSpace(str), "\n")
		str = "\t" + strings.Join(tokens, "\n\t") + "\n>\n"

		err = iohelp.WriteAll(we.Wr, []byte(str))

		if err != nil {
			return err
		}
	}

	return nil
}

// GrpcEmitter sends events to a GRPC service implementing the eventsapi
type GrpcEmitter struct {
	client eventsapi.ClientEventsServiceClient
}

func NewGrpcEmitter(conn *grpc.ClientConn) *GrpcEmitter {
	client := eventsapi.NewClientEventsServiceClient(conn)
	return &GrpcEmitter{client}
}

func (em *GrpcEmitter) LogEvents(version string, evts []*eventsapi.ClientEvent) error {
	ctx, cnclFn := context.WithDeadline(context.Background(), time.Now().Add(time.Second+500*time.Millisecond))
	defer cnclFn()

	var plat eventsapi.Platform
	switch strings.ToLower(runtime.GOOS) {
	case "darwin":
		plat = eventsapi.Platform_DARWIN
	case "linux":
		plat = eventsapi.Platform_LINUX
	case "windows":
		plat = eventsapi.Platform_WINDOWS
	}

	req := eventsapi.LogEventsRequest{
		MachineId: getMachineID(),
		Version:   version,
		Platform:  plat,
		Events:    evts,
	}

	_, err := em.client.LogEvents(ctx, &req)

	return err
}

// SendLogEventsRequest sends a request using the grpc client
func (em *GrpcEmitter) SendLogEventsRequest(ctx context.Context, req *eventsapi.LogEventsRequest) error {
	_, err := em.client.LogEvents(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

// FileEmitter saves event requests to files
type FileEmitter struct {
	fbp *FileBackedProc
}

// NewFileEmitter creates a new file emitter
func NewFileEmitter() *FileEmitter {
	fs := filesys.LocalFS

	root, err := env.GetCurrentUserHomeDir()
	if err != nil {
		panic(err)
	}

	dolt := dbfactory.DoltDir

	return &FileEmitter{fbp: NewFileBackedProc(fs, root, dolt, MD5FileNamer, CheckFilenameMD5)}
}

// LogEvents implements the Emitter interface and writes events requests to files
func (fe *FileEmitter) LogEvents(version string, evts []*eventsapi.ClientEvent) error {
	if err := fe.fbp.WriteEvents(version, evts); err != nil {
		return err
	}

	return nil
}

// AreMetricsDisabled returns true if the dolt config has the metrics.disabled property
// set to true, otherwise returns false
func AreMetricsDisabled(dEnv *env.DoltEnv) (bool, error) {
	metricsDisabled := dEnv.Config.GetStringOrDefault(env.MetricsDisabled, "false")

	return strconv.ParseBool(*metricsDisabled)
}
