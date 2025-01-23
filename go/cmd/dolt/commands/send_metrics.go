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

package commands

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

// SendMetricsCommand is the command used for sending metrics
const (
	SendMetricsCommand   = "send-metrics"
	EventsOutputFormat   = "output-format"
	sendMetricsShortDesc = "Send usage metrics to the events server (default), or log them in another way"
)

type SendMetricsCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd SendMetricsCmd) Name() string {
	return SendMetricsCommand
}

// Description returns a description of the command
func (cmd SendMetricsCmd) Description() string {
	return "Send events logs to server."
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd SendMetricsCmd) RequiresRepo() bool {
	return false
}

// Hidden should return true if this command should be hidden from the help text
func (cmd SendMetricsCmd) Hidden() bool {
	return true
}

func (cmd SendMetricsCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd SendMetricsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString(
		EventsOutputFormat,
		"r",
		"output-format",
		"Format of the events output. Valid values are null, stdout, grpc, file, logger. Defaults to grpc.",
	)
	return ap
}

// Exec is the implementation of the command that flushes the events to the grpc service
func (cmd SendMetricsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	if dEnv.DoltDB(ctx) != nil { // see go/cmd/dolt/dolt.go:interceptSendMetrics()
		cli.PrintErrln("expected DoltEnv without doltDB")
		return 1
	}

	ap := cmd.ArgParser()

	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{ShortDesc: sendMetricsShortDesc}, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	metricsDisabled := dEnv.Config.GetStringOrDefault(config.MetricsDisabled, "false")

	disabled, err := strconv.ParseBool(metricsDisabled)
	if err != nil {
		return 1
	}

	if disabled {
		cli.Println(color.CyanString("Sending metrics is currently disabled\n"))
		return 0
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	userHomeDir, err := dEnv.GetUserHomeDir()
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	output := apr.GetValueOrDefault(EventsOutputFormat, events.EmitterTypeGrpc)
	err = FlushLoggedEvents(ctx, dEnv, userHomeDir, output)

	if err != nil {
		cli.PrintErrf("Error flushing events: %s\n", err.Error())

		if err == events.ErrFileLocked {
			return 2
		}

		return 1
	}

	return 0
}

// FlushLoggedEvents flushes any logged events in the directory given to an appropriate event emitter
func FlushLoggedEvents(ctx context.Context, dEnv *env.DoltEnv, userHomeDir string, outputType string) error {
	emitter, closer, err := NewEmitter(outputType, dEnv)
	if err != nil {
		return err
	}
	defer closer()
	flusher := events.NewFileFlusher(dEnv.FS, userHomeDir, dbfactory.DoltDir, emitter)
	return flusher.Flush(ctx)
}

// NewEmitter returns an emitter for the given configuration provider, of the type named. If an empty name is provided,
// defaults to a file-based emitter.
func NewEmitter(emitterType string, pro EmitterConfigProvider) (events.Emitter, func() error, error) {
	switch emitterType {
	case events.EmitterTypeNull:
		return events.NullEmitter{}, func() error { return nil }, nil
	case events.EmitterTypeStdout:
		return events.WriterEmitter{Wr: os.Stdout}, func() error { return nil }, nil
	case events.EmitterTypeGrpc:
		return GRPCEmitterForConfig(pro)
	case events.EmitterTypeFile:
		homeDir, err := pro.GetUserHomeDir()
		if err != nil {
			return nil, nil, err
		}
		return events.NewFileEmitter(homeDir, dbfactory.DoltDir), func() error { return nil }, nil
	case events.EmitterTypeLogger:
		return events.NewLoggerEmitter(logrus.DebugLevel), func() error { return nil }, nil
	default:
		return nil, nil, fmt.Errorf("unknown emitter type: %s", emitterType)
	}
}

// GRPCEmitterForConfig returns an event emitter for the given environment, or nil if the environment cannot
// provide one
func GRPCEmitterForConfig(pro EmitterConfigProvider) (*events.GrpcEmitter, func() error, error) {
	cfg, err := GRPCEventRemoteConfig(pro)
	if err != nil {
		return nil, nil, err
	}

	conn, err := grpc.Dial(cfg.Endpoint, cfg.DialOptions...)
	if err != nil {
		return nil, nil, err
	}

	return events.NewGrpcEmitter(conn), conn.Close, nil
}

// GRPCEventRemoteConfig returns a GRPCRemoteConfig for the given configuration provider
func GRPCEventRemoteConfig(pro EmitterConfigProvider) (dbfactory.GRPCRemoteConfig, error) {
	host := pro.GetConfig().GetStringOrDefault(config.MetricsHost, events.DefaultMetricsHost)
	portStr := pro.GetConfig().GetStringOrDefault(config.MetricsPort, events.DefaultMetricsPort)
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

// EmitterConfigProvider is an interface used to get the configuration to create an emitter
type EmitterConfigProvider interface {
	GetGRPCDialParams(config grpcendpoint.Config) (dbfactory.GRPCRemoteConfig, error)
	GetConfig() config.ReadableConfig
	GetUserHomeDir() (string, error)
}
