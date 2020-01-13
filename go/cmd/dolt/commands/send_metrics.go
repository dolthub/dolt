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

package commands

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/events"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

// SendMetricsCommand is the command used for sending metrics
const (
	SendMetricsCommand  = "send-metrics"
	outputFlag          = "output"
	sendMetricsShortDec = "Send metrics to the events server or print them to stdout"
)

type SendMetricsCmd struct{}

func (cmd SendMetricsCmd) Name() string {
	return SendMetricsCommand
}

func (cmd SendMetricsCmd) Description() string {
	return "Send events logs to server."
}

// Exec is the implementation of the command that flushes the events to the grpc service
func (cmd SendMetricsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(outputFlag, "o", "Flush events to stdout.")

	help, _ := cli.HelpAndUsagePrinters(commandStr, sendMetricsShortDec, "", []string{}, ap)
	apr := cli.ParseArgs(ap, args, help)

	metricsDisabled := dEnv.Config.GetStringOrDefault(env.MetricsDisabled, "false")

	disabled, err := strconv.ParseBool(*metricsDisabled)
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

	return events.NewGrpcEmitter(conn)
}
