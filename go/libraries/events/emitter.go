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
	"fmt"
	"io"
	"runtime"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

// Application is the application ID used for all events emitted by this application. Other applications (not dolt)
// should set this once at initialization.
var Application = eventsapi.AppID_APP_DOLT

// EmitterTypeEnvVar is the environment variable DOLT_EVENTS_EMITTER, which you can set to one of the values below
// to change how event emission occurs. Currently only used for sql-server heartbeat events.
const EmitterTypeEnvVar = "DOLT_EVENTS_EMITTER"

// Types of emitters. These strings are accepted by the --output-format flag for the send-metrics command.
const (
	EmitterTypeNull   = "null"   // no output
	EmitterTypeStdout = "stdout" // output to stdout, used in testing
	EmitterTypeGrpc   = "grpc"   // output to a grpc server, the default for send-metrics
	EmitterTypeFile   = "file"   // output to a file, used to log events during normal execution
	EmitterTypeLogger = "logger" // output to a logger, used in testing
)

const DefaultMetricsHost = "eventsapi.dolthub.com"
const DefaultMetricsPort = "443"

// Emitter is an interface used for processing a batch of events
type Emitter interface {
	// LogEvents emits a batch of events
	LogEvents(ctx context.Context, version string, evts []*eventsapi.ClientEvent) error
	// LogEventsRequest emits a batch of events wrapped in a request object, with other metadata
	LogEventsRequest(ctx context.Context, req *eventsapi.LogEventsRequest) error
}

// NullEmitter is an emitter that drops events
type NullEmitter struct{}

// LogEvents takes a batch of events and processes them.  In this case it just drops them
func (ne NullEmitter) LogEvents(ctx context.Context, version string, evts []*eventsapi.ClientEvent) error {
	return nil
}

func (ne NullEmitter) LogEventsRequest(ctx context.Context, req *eventsapi.LogEventsRequest) error {
	return nil
}

// WriterEmitter is an emitter that writes the text encoding of the events to it's writer
type WriterEmitter struct {
	// Wr the writer to log events to
	Wr io.Writer
}

// LogEvents takes a batch of events and processes them.  In this case the text encoding of the events is written to
// the writer
func (we WriterEmitter) LogEvents(ctx context.Context, version string, evts []*eventsapi.ClientEvent) error {
	for i, evt := range evts {
		header := fmt.Sprintf("event%03d: <\n", i)

		err := iohelp.WriteAll(we.Wr, []byte(header))

		if err != nil {
			return err
		}

		bs, err := prototext.Marshal(evt)
		if err != nil {
			return err
		}
		str := string(bs)
		tokens := strings.Split(strings.TrimSpace(str), "\n")
		str = "\t" + strings.Join(tokens, "\n\t") + "\n>\n"

		err = iohelp.WriteAll(we.Wr, []byte(str))

		if err != nil {
			return err
		}
	}

	return nil
}

func (we WriterEmitter) LogEventsRequest(ctx context.Context, req *eventsapi.LogEventsRequest) error {
	_, err := fmt.Fprintf(color.Output, "%+v\n", req)
	return err
}

// GrpcEmitter sends events to a GRPC service implementing the eventsapi
type GrpcEmitter struct {
	client eventsapi.ClientEventsServiceClient
}

// NewGrpcEmitter creates a new GrpcEmitter
func NewGrpcEmitter(conn *grpc.ClientConn) *GrpcEmitter {
	client := eventsapi.NewClientEventsServiceClient(conn)
	return &GrpcEmitter{client}
}

func (em *GrpcEmitter) LogEvents(ctx context.Context, version string, evts []*eventsapi.ClientEvent) error {
	ctx, cnclFn := context.WithDeadline(ctx, time.Now().Add(time.Second+500*time.Millisecond))
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

	req := &eventsapi.LogEventsRequest{
		MachineId: getMachineID(),
		Version:   version,
		Platform:  plat,
		Events:    evts,
		App:       Application,
	}

	return em.sendLogEventsRequest(ctx, req)
}

func (em *GrpcEmitter) LogEventsRequest(ctx context.Context, req *eventsapi.LogEventsRequest) error {
	return em.sendLogEventsRequest(ctx, req)
}

// SendLogEventsRequest sends a request using the grpc client
func (em *GrpcEmitter) sendLogEventsRequest(ctx context.Context, req *eventsapi.LogEventsRequest) error {
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
func NewFileEmitter(userHomeDir string, doltDir string) *FileEmitter {
	fs := filesys.LocalFS

	return &FileEmitter{fbp: NewFileBackedProc(fs, userHomeDir, doltDir, MD5FileNamer, CheckFilenameMD5)}
}

// LogEvents implements the Emitter interface and writes events requests to files
func (fe *FileEmitter) LogEvents(ctx context.Context, version string, evts []*eventsapi.ClientEvent) error {
	if err := fe.fbp.WriteEvents(version, evts); err != nil {
		return err
	}

	return nil
}

func (fe *FileEmitter) LogEventsRequest(ctx context.Context, req *eventsapi.LogEventsRequest) error {
	// TODO: we are losing some information here, like the machine id
	if err := fe.fbp.WriteEvents(req.Version, req.Events); err != nil {
		return err
	}

	return nil
}

type LoggerEmitter struct {
	logLevel logrus.Level
}

func (l LoggerEmitter) LogEvents(ctx context.Context, version string, evts []*eventsapi.ClientEvent) error {
	sb := &strings.Builder{}
	wr := WriterEmitter{Wr: sb}
	err := wr.LogEvents(ctx, version, evts)
	if err != nil {
		return err
	}

	eventString := sb.String()
	return l.logEventString(eventString)
}

func (l LoggerEmitter) LogEventsRequest(ctx context.Context, req *eventsapi.LogEventsRequest) error {
	sb := &strings.Builder{}
	wr := WriterEmitter{Wr: sb}
	err := wr.LogEventsRequest(ctx, req)
	if err != nil {
		return err
	}

	eventString := sb.String()
	return l.logEventString(eventString)
}

func (l LoggerEmitter) logEventString(eventString string) error {
	switch l.logLevel {
	case logrus.DebugLevel:
		logrus.Debug(eventString)
	case logrus.ErrorLevel:
		logrus.Error(eventString)
	case logrus.FatalLevel:
		logrus.Fatal(eventString)
	case logrus.InfoLevel:
		logrus.Info(eventString)
	case logrus.PanicLevel:
		logrus.Panic(eventString)
	case logrus.TraceLevel:
		logrus.Trace(eventString)
	case logrus.WarnLevel:
		logrus.Warn(eventString)
	default:
		return fmt.Errorf("unknown log level %v", l.logLevel)
	}
	return nil
}

func NewLoggerEmitter(level logrus.Level) *LoggerEmitter {
	return &LoggerEmitter{
		logLevel: level,
	}
}
