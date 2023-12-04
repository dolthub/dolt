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
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/fatih/color"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"

	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

var Application = eventsapi.AppID_APP_DOLT

// EmitterTypeEnvVar is the environment variable DOLT_EVENTS_EMITTER, which you can set to one of the values below 
// to change how event emission occurs. This is useful for testing and in some environments.
const EmitterTypeEnvVar = "DOLT_EVENTS_EMITTER"
const (
	EmitterTypeNull   = "null"
	EmitterTypeStdout = "stdout"
	EmitterTypeGrpc   = "grpc"
	EmitterTypeFile   = "file"
)

const DefaultMetricsHost = "eventsapi.dolthub.com"
const DefaultMetricsPort = "443"

// Emitter is an interface used for processing a batch of events
type Emitter interface {
	// LogEvents emits a batch of events
	LogEvents(version string, evts []*eventsapi.ClientEvent) error
	// LogEventsRequest emits a batch of events wrapped in a request object, with other metadata
	LogEventsRequest(ctx context.Context, req *eventsapi.LogEventsRequest) error
}

// EmitterConfigProvider is an interface used to get the configuration to create an emitter
type EmitterConfigProvider interface {
	GetGRPCDialParams(config grpcendpoint.Config) (dbfactory.GRPCRemoteConfig, error)
	GetConfig() config.ReadableConfig
	GetUserHomeDir() (string, error)
}

// NullEmitter is an emitter that drops events
type NullEmitter struct{}

// LogEvents takes a batch of events and processes them.  In this case it just drops them
func (ne NullEmitter) LogEvents(version string, evts []*eventsapi.ClientEvent) error {
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
func (we WriterEmitter) LogEvents(version string, evts []*eventsapi.ClientEvent) error {
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
		App:       Application,
	}

	_, err := em.client.LogEvents(ctx, &req)

	return err
}

func (em *GrpcEmitter) LogEventsRequest(ctx context.Context, req *eventsapi.LogEventsRequest) error {
	return em.SendLogEventsRequest(ctx, req)
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
func NewFileEmitter(userHomeDir string, doltDir string) *FileEmitter {
	fs := filesys.LocalFS

	return &FileEmitter{fbp: NewFileBackedProc(fs, userHomeDir, doltDir, MD5FileNamer, CheckFilenameMD5)}
}

// LogEvents implements the Emitter interface and writes events requests to files
func (fe *FileEmitter) LogEvents(version string, evts []*eventsapi.ClientEvent) error {
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

// NewEmitter returns an emitter for the given configuration provider, of the type named. If an empty name is provided, 
// defaults to a file-based emitter.
func NewEmitter(emitterType string, pro EmitterConfigProvider) (Emitter, error) {
	switch emitterType {
	case EmitterTypeNull:
		return NullEmitter{}, nil
	case EmitterTypeStdout:
		return WriterEmitter{Wr: os.Stdout}, nil
	case EmitterTypeGrpc:
		return GRPCEmitterForConfig(pro), nil
	case EmitterTypeFile:
		homeDir, err := pro.GetUserHomeDir()
		if err != nil {
			return nil, err
		}
		return NewFileEmitter(homeDir, dbfactory.DoltDir), nil
	default:
		return nil, fmt.Errorf("unknown emitter type: %s", emitterType)
	}
}

// GRPCEmitterForConfig returns an event emitter for the given environment, or nil if the environment cannot
// provide one
func GRPCEmitterForConfig(pro EmitterConfigProvider) *GrpcEmitter {
	cfg, err := GRPCEventRemoteConfig(pro)
	if err != nil {
		return nil
	}

	conn, err := grpc.Dial(cfg.Endpoint, cfg.DialOptions...)
	if err != nil {
		return nil
	}
	return NewGrpcEmitter(conn)
}

// GRPCEventRemoteConfig returns a GRPCRemoteConfig for the given configuration provider
func GRPCEventRemoteConfig(pro EmitterConfigProvider) (dbfactory.GRPCRemoteConfig, error) {
	host := pro.GetConfig().GetStringOrDefault(config.MetricsHost, DefaultMetricsHost)
	portStr := pro.GetConfig().GetStringOrDefault(config.MetricsPort, DefaultMetricsPort)
	insecureStr := pro.GetConfig().GetStringOrDefault(config.MetricsInsecure, "false")

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return dbfactory.GRPCRemoteConfig{}, nil
	}

	insecure, _ := strconv.ParseBool(insecureStr)

	hostAndPort := fmt.Sprintf("%s:%d", host, port)
	cfg, err := pro.GetGRPCDialParams(grpcendpoint.Config{
		Endpoint: hostAndPort,
		Insecure: insecure,
	})
	if err != nil {
		return dbfactory.GRPCRemoteConfig{}, nil
	}
	
	return cfg, nil
}
