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
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/fatih/color"
	"github.com/pkg/profile"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/admin"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/cnfcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/credcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/cvcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/docscmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/indexcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/schcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/tblcmds"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

const (
	Version = "0.52.3"
)

var dumpDocsCommand = &commands.DumpDocsCmd{}
var dumpZshCommand = &commands.GenZshCompCmd{}
var doltCommand = cli.NewSubCommandHandler("dolt", "it's git for data", []cli.Command{
	commands.InitCmd{},
	commands.StatusCmd{},
	commands.AddCmd{},
	commands.DiffCmd{},
	commands.ResetCmd{},
	commands.CleanCmd{},
	commands.CommitCmd{},
	commands.SqlCmd{VersionStr: Version},
	admin.Commands,
	sqlserver.SqlServerCmd{VersionStr: Version},
	sqlserver.SqlClientCmd{VersionStr: Version},
	commands.LogCmd{},
	commands.BranchCmd{},
	commands.CheckoutCmd{},
	commands.MergeCmd{},
	cnfcmds.Commands,
	commands.CherryPickCmd{},
	commands.RevertCmd{},
	commands.CloneCmd{},
	commands.FetchCmd{},
	commands.PullCmd{},
	commands.PushCmd{},
	commands.ConfigCmd{},
	commands.RemoteCmd{},
	commands.BackupCmd{},
	commands.LoginCmd{},
	credcmds.Commands,
	commands.LsCmd{},
	schcmds.Commands,
	tblcmds.Commands,
	commands.TagCmd{},
	commands.BlameCmd{},
	cvcmds.Commands,
	commands.SendMetricsCmd{},
	commands.MigrateCmd{},
	indexcmds.Commands,
	commands.ReadTablesCmd{},
	commands.GarbageCollectionCmd{},
	commands.FilterBranchCmd{},
	commands.MergeBaseCmd{},
	commands.RootsCmd{},
	commands.VersionCmd{VersionStr: Version},
	commands.DumpCmd{},
	commands.InspectCmd{},
	dumpDocsCommand,
	dumpZshCommand,
	docscmds.Commands,
})

func init() {
	dumpDocsCommand.DoltCommand = doltCommand
	dumpZshCommand.DoltCommand = doltCommand
	dfunctions.VersionString = Version
}

const pprofServerFlag = "--pprof-server"
const chdirFlag = "--chdir"
const jaegerFlag = "--jaeger"
const profFlag = "--prof"
const csMetricsFlag = "--csmetrics"
const stdInFlag = "--stdin"
const stdOutFlag = "--stdout"
const stdErrFlag = "--stderr"
const stdOutAndErrFlag = "--out-and-err"
const ignoreLocksFlag = "--ignore-lock-file"

const cpuProf = "cpu"
const memProf = "mem"
const blockingProf = "blocking"
const traceProf = "trace"

const featureVersionFlag = "--feature-version"

func main() {
	os.Exit(runMain())
}

func runMain() int {
	args := os.Args[1:]

	csMetrics := false
	ignoreLockFile := false
	if len(args) > 0 {
		var doneDebugFlags bool
		for !doneDebugFlags && len(args) > 0 {
			switch args[0] {
			case profFlag:
				switch args[1] {
				case cpuProf:
					cli.Println("cpu profiling enabled.")
					defer profile.Start(profile.CPUProfile, profile.NoShutdownHook).Stop()
				case memProf:
					cli.Println("mem profiling enabled.")
					defer profile.Start(profile.MemProfile, profile.NoShutdownHook).Stop()
				case blockingProf:
					cli.Println("block profiling enabled")
					defer profile.Start(profile.BlockProfile, profile.NoShutdownHook).Stop()
				case traceProf:
					cli.Println("trace profiling enabled")
					defer profile.Start(profile.TraceProfile, profile.NoShutdownHook).Stop()
				default:
					panic("Unexpected prof flag: " + args[1])
				}
				args = args[2:]

			case pprofServerFlag:
				// serve the pprof endpoints setup in the init function run when "net/http/pprof" is imported
				go func() {
					cyanStar := color.CyanString("*")
					cli.Println(cyanStar, "Starting pprof server on port 6060.")
					cli.Println(cyanStar, "Go to", color.CyanString("http://localhost:6060/debug/pprof"), "in a browser to see supported endpoints.")
					cli.Println(cyanStar)
					cli.Println(cyanStar, "Known endpoints are:")
					cli.Println(cyanStar, "  /allocs: A sampling of all past memory allocations")
					cli.Println(cyanStar, "  /block: Stack traces that led to blocking on synchronization primitives")
					cli.Println(cyanStar, "  /cmdline: The command line invocation of the current program")
					cli.Println(cyanStar, "  /goroutine: Stack traces of all current goroutines")
					cli.Println(cyanStar, "  /heap: A sampling of memory allocations of live objects. You can specify the gc GET parameter to run GC before taking the heap sample.")
					cli.Println(cyanStar, "  /mutex: Stack traces of holders of contended mutexes")
					cli.Println(cyanStar, "  /profile: CPU profile. You can specify the duration in the seconds GET parameter. After you get the profile file, use the go tool pprof command to investigate the profile.")
					cli.Println(cyanStar, "  /threadcreate: Stack traces that led to the creation of new OS threads")
					cli.Println(cyanStar, "  /trace: A trace of execution of the current program. You can specify the duration in the seconds GET parameter. After you get the trace file, use the go tool trace command to investigate the trace.")
					cli.Println()

					err := http.ListenAndServe("localhost:6060", nil)

					if err != nil {
						cli.Println(color.YellowString("pprof server exited with error: %v", err))
					}
				}()
				args = args[1:]

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
				cli.Println("running with jaeger tracing reporting to localhost")
				exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://localhost:14268/api/traces")))
				if err != nil {
					cli.Println(color.YellowString("could not create jaeger collector: %v", err))
				} else {
					tp := tracesdk.NewTracerProvider(
						tracesdk.WithBatcher(exp),
						tracesdk.WithResource(resource.NewWithAttributes(
							semconv.SchemaURL,
							semconv.ServiceNameKey.String("dolt"),
						)),
					)
					otel.SetTracerProvider(tp)
					defer tp.Shutdown(context.Background())
					args = args[1:]
				}
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
				cli.Println("Using file contents as stdin:", stdInFile)

				f, err := os.Open(stdInFile)
				if err != nil {
					cli.PrintErrln("Failed to open", stdInFile, err.Error())
					return 1
				}

				os.Stdin = f
				args = args[2:]

			case stdOutFlag, stdErrFlag, stdOutAndErrFlag:
				filename := args[1]

				f, err := os.OpenFile(filename, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, os.ModePerm)
				if err != nil {
					cli.PrintErrln("Failed to open", filename, "for writing:", err.Error())
					return 1
				}

				switch args[0] {
				case stdOutFlag:
					cli.Println("Stdout being written to", filename)
					cli.CliOut = f
				case stdErrFlag:
					cli.Println("Stderr being written to", filename)
					cli.CliErr = f
				case stdOutAndErrFlag:
					cli.Println("Stdout and Stderr being written to", filename)
					cli.CliOut = f
					cli.CliErr = f
				}

				color.NoColor = true
				args = args[2:]

			case csMetricsFlag:
				csMetrics = true
				args = args[1:]

			case ignoreLocksFlag:
				ignoreLockFile = true
				args = args[1:]

			case featureVersionFlag:
				if featureVersion, err := strconv.Atoi(args[1]); err == nil {
					doltdb.DoltFeatureVersion = doltdb.FeatureVersion(featureVersion)
				} else {
					panic(err)
				}
				args = args[2:]

			default:
				doneDebugFlags = true
			}
		}
	}

	seedGlobalRand()

	restoreIO := cli.InitIO()
	defer restoreIO()

	warnIfMaxFilesTooLow()

	ctx := context.Background()
	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, Version)
	dEnv.IgnoreLockFile = ignoreLockFile

	root, err := env.GetCurrentUserHomeDir()
	if err != nil {
		cli.PrintErrln(color.RedString("Failed to load the HOME directory: %v", err))
		return 1
	}

	emitter := events.NewFileEmitter(root, dbfactory.DoltDir)

	defer func() {
		ces := events.GlobalCollector.Close()
		// events.WriterEmitter{cli.CliOut}.LogEvents(Version, ces)

		metricsDisabled := dEnv.Config.GetStringOrDefault(env.MetricsDisabled, "false")

		disabled, err := strconv.ParseBool(metricsDisabled)
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

	// Find all database names and add global variables for them. This needs to
	// occur before a call to dsess.InitPersistedSystemVars. Otherwise, database
	// specific persisted system vars will fail to load.
	//
	// In general, there is a lot of work TODO in this area. System global
	// variables are persisted to the Dolt local config if found and if not
	// found the Dolt global config (typically ~/.dolt/config_global.json).

	// Depending on what directory a dolt sql-server is started in, users may
	// see different variables values. For example, start a dolt sql-server in
	// the dolt database folder and persist some system variable.

	// If dolt sql-server is started outside that folder, those system variables
	// will be lost. This is particularly confusing for database specific system
	// variables like `${db_name}_default_branch` (maybe these should not be
	// part of Dolt config in the first place!).

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
	if err != nil {
		cli.PrintErrln("failed to load database names")
		return 1
	}
	_ = mrEnv.Iter(func(dbName string, dEnv *env.DoltEnv) (stop bool, err error) {
		dsess.DefineSystemVariablesForDB(dbName)
		return false, nil
	})

	err = dsess.InitPersistedSystemVars(dEnv)
	if err != nil {
		cli.Printf("error: failed to load persisted global variables: %s\n", err.Error())
	}

	start := time.Now()
	ctx, stop := context.WithCancel(ctx)
	res := doltCommand.Exec(ctx, "dolt", args, dEnv)
	stop()

	if err = dbfactory.CloseAllLocalDatabases(); err != nil {
		cli.PrintErrln(err)
		if res == 0 {
			res = 1
		}
	}

	if csMetrics && dEnv.DoltDB != nil {
		metricsSummary := dEnv.DoltDB.CSMetricsSummary()
		cli.Println("Command took", time.Since(start).Seconds())
		cli.PrintErrln(metricsSummary)
	}

	return res
}

func seedGlobalRand() {
	bs := make([]byte, 8)
	_, err := crand.Read(bs)
	if err != nil {
		panic("failed to initial rand " + err.Error())
	}
	rand.Seed(int64(binary.LittleEndian.Uint64(bs)))
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
