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
	"io"
	"log"
	"strconv"
	"time"

	"github.com/fatih/color"
	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

// SendMetricsCommand is the command used for sending metrics
const (
	SendMetricsCommand   = "send-metrics"
	outputFlag           = "output"
	sendMetricsShortDesc = "Send metrics to the events server or print them to stdout"
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

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd SendMetricsCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	return nil
}

func (cmd SendMetricsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(outputFlag, "o", "Flush events to stdout.")
	return ap
}

// Exec is the implementation of the command that flushes the events to the grpc service
// Exec executes the command
func (cmd SendMetricsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()

	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cli.CommandDocumentationContent{ShortDesc: sendMetricsShortDesc}, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	metricsDisabled := dEnv.Config.GetStringOrDefault(env.MetricsDisabled, "false")

	disabled, err := strconv.ParseBool(metricsDisabled)
	if err != nil {
		// log.Print(err)
		return 1
	}

	if disabled {
		cli.Println(color.CyanString("Sending metrics is currently disabled\n"))
		return 0
	}

	if !disabled {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		root, err := dEnv.GetUserHomeDir()
		if err != nil {
			// log.Print(err)
			return 1
		}

		dolt := dbfactory.DoltDir

		var flusher events.Flusher

		if apr.Contains(outputFlag) {
			flusher = events.NewIOFlusher(dEnv.FS, root, dolt)
		} else {
			grpcEmitter := getGRPCEmitter(dEnv)

			flusher = events.NewGrpcEventFlusher(dEnv.FS, root, dolt, grpcEmitter)
		}

		err = flusher.Flush(ctx)

		if err != nil {
			if err == events.ErrFileLocked {
				return 2
			}

			return 1
		}

		return 0
	}

	return 1
}

// getGRPCEmitter gets the connection to the events grpc service
func getGRPCEmitter(dEnv *env.DoltEnv) *events.GrpcEmitter {
	host := dEnv.Config.GetStringOrDefault(env.MetricsHost, env.DefaultMetricsHost)
	portStr := dEnv.Config.GetStringOrDefault(env.MetricsPort, env.DefaultMetricsPort)
	insecureStr := dEnv.Config.GetStringOrDefault(env.MetricsInsecure, "false")

	port, err := strconv.ParseUint(portStr, 10, 16)

	if err != nil {
		log.Println(color.YellowString("The config value of '%s' is '%s' which is not a valid port.", env.MetricsPort, portStr))
		return nil
	}

	insecure, err := strconv.ParseBool(insecureStr)

	if err != nil {
		log.Println(color.YellowString("The config value of '%s' is '%s' which is not a valid true/false value", env.MetricsInsecure, insecureStr))
	}

	hostAndPort := fmt.Sprintf("%s:%d", host, port)
	endpoint, opts, err := dEnv.GetGRPCDialParams(grpcendpoint.Config{
		Endpoint: hostAndPort,
		Insecure: insecure,
	})
	if err != nil {
		return nil
	}
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return nil
	}
	return events.NewGrpcEmitter(conn)
}
