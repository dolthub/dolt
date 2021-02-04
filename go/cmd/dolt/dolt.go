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

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/profile"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/transport"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/cnfcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/credcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/indexcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/schcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/tblcmds"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

const (
	Version = "0.22.13"
)

var dumpDocsCommand = &commands.DumpDocsCmd{}
var doltCommand = cli.NewSubCommandHandler("dolt", "it's git for data", []cli.Command{
	commands.InitCmd{},
	commands.StatusCmd{},
	commands.AddCmd{},
	commands.ResetCmd{},
	commands.CommitCmd{},
	commands.SqlCmd{VersionStr: Version},
	sqlserver.SqlServerCmd{VersionStr: Version},
	sqlserver.SqlClientCmd{},
	commands.LogCmd{},
	commands.DiffCmd{},
	commands.BlameCmd{},
	commands.MergeCmd{},
	commands.BranchCmd{},
	commands.TagCmd{},
	commands.CheckoutCmd{},
	commands.RemoteCmd{},
	commands.PushCmd{},
	commands.PullCmd{},
	commands.FetchCmd{},
	commands.CloneCmd{},
	credcmds.Commands,
	commands.LoginCmd{},
	commands.VersionCmd{VersionStr: Version},
	commands.ConfigCmd{},
	commands.LsCmd{},
	schcmds.Commands,
	tblcmds.Commands,
	cnfcmds.Commands,
	commands.SendMetricsCmd{},
	dumpDocsCommand,
	commands.MigrateCmd{},
	indexcmds.Commands,
	commands.ReadTablesCmd{},
	commands.GarbageCollectionCmd{},
	commands.FilterBranchCmd{},
	commands.VerifyConstraintsCmd{},
})

func init() {
	dumpDocsCommand.DoltCommand = doltCommand
	dfunctions.VersionString = Version
}

const chdirFlag = "--chdir"
const jaegerFlag = "--jaeger"
const profFlag = "--prof"
const csMetricsFlag = "--csmetrics"
const stdInFlag = "--stdin"
const cpuProf = "cpu"
const memProf = "mem"
const blockingProf = "blocking"
const traceProf = "trace"

const keylessFeatureFlag = "--keyless"

func main() {
	os.Exit(runMain())
}

func runMain() int {
	args := os.Args[1:]

	csMetrics := false
	if len(args) > 0 {
		var doneDebugFlags bool
		for !doneDebugFlags {
			switch args[0] {
			case profFlag:
				switch args[1] {
				case cpuProf:
					fmt.Println("cpu profiling enabled.")
					defer profile.Start(profile.CPUProfile).Stop()
				case memProf:
					fmt.Println("mem profiling enabled.")
					defer profile.Start(profile.MemProfile).Stop()
				case blockingProf:
					fmt.Println("block profiling enabled")
					defer profile.Start(profile.BlockProfile).Stop()
				case traceProf:
					fmt.Println("trace profiling enabled")
					defer profile.Start(profile.TraceProfile).Stop()
				default:
					panic("Unexpected prof flag: " + args[1])
				}
				args = args[2:]

			// Enable a global jaeger tracer for this run of Dolt,
			// emitting traces to a collector running at
			// localhost:14268. To visualize these traces, run:
			// docker run -d --name jaeger \
			//    -e COLLECTOR_ZIPKIN_HTTP_PORT=9411 \
			//    -p 5775:5775/udp \
			//    -p 6831:6831/udp \
			//    -p 6832:6832/udp \
			//    -p 5778:5778 \
			//    -p 16686:16686 \
			//    -p 14268:14268 \
			//    -p 14250:14250 \
			//    -p 9411:9411 \
			//    jaegertracing/all-in-one:1.21
			// and browse to http://localhost:16686
			case jaegerFlag:
				fmt.Println("running with jaeger tracing reporting to localhost")
				transport := transport.NewHTTPTransport("http://localhost:14268/api/traces?format=jaeger.thrift", transport.HTTPBatchSize(128000))
				reporter := jaeger.NewRemoteReporter(transport)
				tracer, closer := jaeger.NewTracer("dolt", jaeger.NewConstSampler(true), reporter)
				opentracing.SetGlobalTracer(tracer)
				defer closer.Close()
				args = args[1:]

			// Currently goland doesn't support running with a different working directory when using go modules.
			// This is a hack that allows a different working directory to be set after the application starts using
			// chdir=<DIR>.  The syntax is not flexible and must match exactly this.
			case chdirFlag:
				err := os.Chdir(args[1])

				if err != nil {
					panic(err)
				}

				args = args[2:]

			case stdInFlag:
				stdInFile := args[1]
				fmt.Println("Using file contents as stdin:", stdInFile)

				f, err := os.Open(stdInFile)

				if err != nil {
					panic(err)
				}

				os.Stdin = f
				args = args[2:]

			case csMetricsFlag:
				csMetrics = true
				args = args[1:]

			case keylessFeatureFlag:
				schema.FeatureFlagKeylessSchema = true
				args = args[1:]

			default:
				doneDebugFlags = true
			}
		}
	}

	restoreIO := cli.InitIO()
	defer restoreIO()

	warnIfMaxFilesTooLow()

	ctx := context.Background()
	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, Version)

	if dEnv.DBLoadError == nil && commandNeedsMigrationCheck(args) {
		if commands.MigrationNeeded(ctx, dEnv, args) {
			return 1
		}
	}

	root, err := env.GetCurrentUserHomeDir()

	if err != nil {
		return 1
	}

	emitter := events.NewFileEmitter(root, dbfactory.DoltDir)

	defer func() {
		ces := events.GlobalCollector.Close()
		// events.WriterEmitter{cli.CliOut}.LogEvents(Version, ces)

		metricsDisabled := dEnv.Config.GetStringOrDefault(env.MetricsDisabled, "false")

		disabled, err := strconv.ParseBool(*metricsDisabled)
		if err != nil {
			// log.Print(err)
			return
		}

		if disabled {
			return
		}

		// write events
		_ = emitter.LogEvents(Version, ces)

		// flush events
		if err := processEventsDir(args, dEnv); err != nil {
			// log.Print(err)
		}
	}()

	if dEnv.CfgLoadErr != nil {
		cli.PrintErrln(color.RedString("Failed to load the global config. %v", dEnv.CfgLoadErr))
		return 1
	}

	err = reconfigIfTempFileMoveFails(dEnv)

	if err != nil {
		cli.PrintErrln(color.RedString("Failed to setup the temporary directory. %v`", err))
		return 1
	}

	defer tempfiles.MovableTempFileProvider.Clean()

	res := doltCommand.Exec(ctx, "dolt", args, dEnv)

	if csMetrics && dEnv.DoltDB != nil {
		metricsSummary := dEnv.DoltDB.CSMetricsSummary()
		cli.PrintErrln(metricsSummary)
	}

	return res
}

// These subcommands will cannot be performed if a migration is needed
func commandNeedsMigrationCheck(args []string) bool {
	if len(args) == 0 {
		return false
	}

	// special case for -h, --help
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return false
		}
	}

	subCommandStr := strings.ToLower(strings.TrimSpace(args[0]))
	for _, cmd := range []cli.Command{
		commands.ResetCmd{},
		commands.CommitCmd{},
		commands.SqlCmd{},
		sqlserver.SqlServerCmd{},
		sqlserver.SqlClientCmd{},
		commands.DiffCmd{},
		commands.MergeCmd{},
		commands.BranchCmd{},
		commands.CheckoutCmd{},
		commands.RemoteCmd{},
		commands.PushCmd{},
		commands.PullCmd{},
		commands.FetchCmd{},
		commands.CloneCmd{},
		schcmds.ImportCmd{},
		tblcmds.ImportCmd{},
		tblcmds.RmCmd{},
		tblcmds.MvCmd{},
		tblcmds.CpCmd{},
	} {
		if subCommandStr == strings.ToLower(cmd.Name()) {
			return true
		}
	}
	return false
}

// processEventsDir runs the dolt send-metrics command in a new process
func processEventsDir(args []string, dEnv *env.DoltEnv) error {
	if len(args) > 0 {
		ignoreCommands := map[string]struct{}{
			commands.SendMetricsCommand: {},
			"init":                      {},
			"config":                    {},
		}

		_, ok := ignoreCommands[args[0]]

		if ok {
			return nil
		}

		cmd := exec.Command("dolt", commands.SendMetricsCommand)

		if err := cmd.Start(); err != nil {
			// log.Print(err)
			return err
		}

		return nil
	}

	return nil
}
