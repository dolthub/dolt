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
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"github.com/pkg/profile"
	"github.com/tidwall/gjson"
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
	"github.com/dolthub/dolt/go/cmd/dolt/commands/stashcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/tblcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

var dumpDocsCommand = &commands.DumpDocsCmd{}
var dumpZshCommand = &commands.GenZshCompCmd{}

var doltSubCommands = []cli.Command{
	commands.InitCmd{},
	commands.StatusCmd{},
	commands.AddCmd{},
	commands.DiffCmd{},
	commands.ResetCmd{},
	commands.CleanCmd{},
	commands.CommitCmd{},
	commands.SqlCmd{VersionStr: doltversion.Version},
	admin.Commands,
	sqlserver.SqlServerCmd{VersionStr: doltversion.Version},
	commands.LogCmd{},
	commands.ShowCmd{},
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
	commands.FsckCmd{},
	commands.FilterBranchCmd{},
	commands.MergeBaseCmd{},
	commands.RootsCmd{},
	commands.VersionCmd{VersionStr: doltversion.Version},
	commands.DumpCmd{},
	commands.InspectCmd{},
	dumpDocsCommand,
	dumpZshCommand,
	docscmds.Commands,
	stashcmds.StashCommands,
	&commands.Assist{},
	commands.ProfileCmd{},
	commands.QueryDiff{},
	commands.ReflogCmd{},
	commands.RebaseCmd{},
	commands.ArchiveCmd{},
}

var commandsWithoutCliCtx = []cli.Command{
	admin.Commands,
	sqlserver.SqlServerCmd{VersionStr: doltversion.Version},
	commands.CloneCmd{},
	commands.BackupCmd{},
	commands.LoginCmd{},
	credcmds.Commands,
	schcmds.Commands,
	cvcmds.Commands,
	commands.SendMetricsCmd{},
	commands.MigrateCmd{},
	indexcmds.Commands,
	commands.ReadTablesCmd{},
	commands.FilterBranchCmd{},
	commands.RootsCmd{},
	commands.VersionCmd{VersionStr: doltversion.Version},
	commands.DumpCmd{},
	commands.InspectCmd{},
	dumpDocsCommand,
	dumpZshCommand,
	docscmds.Commands,
	&commands.Assist{},
	commands.ProfileCmd{},
	commands.ArchiveCmd{},
	commands.FsckCmd{},
}

var commandsWithoutGlobalArgSupport = []cli.Command{
	commands.InitCmd{},
	commands.CloneCmd{},
	docscmds.Commands,
	commands.MigrateCmd{},
	commands.ReadTablesCmd{},
	commands.LoginCmd{},
	credcmds.Commands,
	sqlserver.SqlServerCmd{VersionStr: doltversion.Version},
	commands.VersionCmd{VersionStr: doltversion.Version},
	commands.ConfigCmd{},
}

// commands that do not need write access for the current directory
var commandsWithoutCurrentDirWrites = []cli.Command{
	commands.VersionCmd{VersionStr: doltversion.Version},
	commands.ConfigCmd{},
	commands.ProfileCmd{},
}

func initCliContext(commandName string) bool {
	for _, command := range commandsWithoutCliCtx {
		if command.Name() == commandName {
			return false
		}
	}
	return true
}

func supportsGlobalArgs(commandName string) bool {
	for _, command := range commandsWithoutGlobalArgSupport {
		if command.Name() == commandName {
			return false
		}
	}
	return true
}

func needsWriteAccess(commandName string) bool {
	for _, command := range commandsWithoutCurrentDirWrites {
		if command.Name() == commandName {
			return false
		}
	}
	return true
}

var doltCommand = cli.NewSubCommandHandler("dolt", "it's git for data", doltSubCommands)
var globalArgParser = cli.CreateGlobalArgParser("dolt")
var globalDocs = cli.CommandDocsForCommandString("dolt", doc, globalArgParser)

var globalSpecialMsg = `
Dolt subcommands are in transition to using the flags listed below as global flags.
Not all subcommands use these flags. If your command accepts these flags without error, then they are supported.
`

const disableEventFlushEnvVar = "DOLT_DISABLE_EVENT_FLUSH"

var eventFlushDisabled = false

func init() {
	dumpDocsCommand.DoltCommand = doltCommand
	dumpDocsCommand.GlobalDocs = globalDocs
	dumpDocsCommand.GlobalSpecialMsg = globalSpecialMsg
	dumpZshCommand.DoltCommand = doltCommand
	dfunctions.VersionString = doltversion.Version
	if _, ok := os.LookupEnv(disableEventFlushEnvVar); ok {
		eventFlushDisabled = true
	}
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
const verboseEngineSetupFlag = "--verbose-engine-setup"
const profilePath = "--prof-path"

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

	start := time.Now()

	if len(args) == 0 {
		doltCommand.PrintUsage("dolt")
		return 1
	}

	if os.Getenv(dconfig.EnvVerboseAssertTableFilesClosed) == "" {
		nbs.TableIndexGCFinalizerWithStackTrace = false
	}

	csMetrics := false
	verboseEngineSetup := false
	if len(args) > 0 {
		var doneDebugFlags bool
		var profileOpts []func(p *profile.Profile)
		hasUnstartedProfile := false
		for !doneDebugFlags && len(args) > 0 {
			switch args[0] {
			case profilePath:
				path := args[1]
				if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
					panic(fmt.Sprintf("profile path does not exist: %s", path))
				}
				profileOpts = append(profileOpts, profile.ProfilePath(path))
				args = args[2:]
			case profFlag:
				if hasUnstartedProfile {
					defer profile.Start(profileOpts...).Stop()
					profileOpts = nil
					hasUnstartedProfile = false
				}

				profileOpts = append(profileOpts, profile.NoShutdownHook)
				hasUnstartedProfile = true

				switch args[1] {
				case cpuProf:
					profileOpts = append(profileOpts, profile.CPUProfile)
					cli.Println("cpu profiling enabled.")
				case memProf:
					profileOpts = append(profileOpts, profile.MemProfile)
					cli.Println("mem profiling enabled.")
				case blockingProf:
					profileOpts = append(profileOpts, profile.BlockProfile)
					cli.Println("block profiling enabled")
				case traceProf:
					profileOpts = append(profileOpts, profile.TraceProfile)
					cli.Println("trace profiling enabled")
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

					err := http.ListenAndServe("0.0.0.0:6060", nil)

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

				f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, os.ModePerm)
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
				// Ignored -- deprecated option.
				args = args[1:]

			case featureVersionFlag:
				var err error
				if len(args) == 0 {
					err = fmt.Errorf("missing argument for the --feature-version flag")
				} else {
					if featureVersion, err := strconv.Atoi(args[1]); err == nil {
						doltdb.DoltFeatureVersion = doltdb.FeatureVersion(featureVersion)
					}
				}
				if err != nil {
					cli.PrintErrln(err.Error())
					return 1
				}

				args = args[2:]

			case verboseEngineSetupFlag:
				verboseEngineSetup = true
				args = args[1:]
			default:
				doneDebugFlags = true
			}
		}
		if hasUnstartedProfile {
			defer profile.Start(profileOpts...).Stop()
		}
	}

	seedGlobalRand()

	restoreIO := cli.InitIO()
	defer restoreIO()

	warnIfMaxFilesTooLow()

	ctx := context.Background()
	if ok, exit := interceptSendMetrics(ctx, args); ok {
		return exit
	}

	_, usage := cli.HelpAndUsagePrinters(globalDocs)

	var fs filesys.Filesys
	fs = filesys.LocalFS
	apr, _, err := globalArgParser.ParseGlobalArgs(args)
	if err == argparser.ErrHelp {
		doltCommand.PrintUsage("dolt")
		cli.Println(globalSpecialMsg)
		usage()
		return 0
	} else if err != nil {
		cli.PrintErrln(color.RedString("Failure to parse global arguments: %v", err))
		return 1
	}

	dataDir, hasDataDir := apr.GetValue(commands.DataDirFlag)
	if hasDataDir {
		// If a relative path was provided, this ensures we have an absolute path everywhere.
		dataDir, err = fs.Abs(dataDir)
		if err != nil {
			cli.PrintErrln(color.RedString("Failed to get absolute path for %s: %v", dataDir, err))
			return 1
		}
		if ok, dir := fs.Exists(dataDir); !ok || !dir {
			cli.Println(color.RedString("Provided data directory does not exist: %s", dataDir))
			return 1
		}
	}

	// Current working directory is preserved to ensure that user provided path arguments are always calculated
	// relative to this directory. The root environment's FS will be updated to be the --data-dir path if the user
	// specified one.
	cwdFS := fs
	dataDirFS, err := fs.WithWorkingDir(dataDir)
	if err != nil {
		cli.PrintErrln(color.RedString("Failed to set the data directory. %v", err))
		return 1
	}

	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, dataDirFS, doltdb.LocalDirDoltDB, doltversion.Version)

	homeDir, err := env.GetCurrentUserHomeDir()
	if err != nil {
		cli.PrintErrln(color.RedString("Failed to load the HOME directory: %v", err))
		return 1
	}

	if dEnv.CfgLoadErr != nil {
		cli.PrintErrln(color.RedString("Failed to load the global config. %v", dEnv.CfgLoadErr))
		return 1
	}

	strMetricsDisabled := dEnv.Config.GetStringOrDefault(config.MetricsDisabled, "false")
	var metricsEmitter events.Emitter
	metricsEmitter = events.NewFileEmitter(homeDir, dbfactory.DoltDir)
	metricsDisabled, err := strconv.ParseBool(strMetricsDisabled)
	if err != nil || metricsDisabled {
		metricsEmitter = events.NullEmitter{}
	}

	events.SetGlobalCollector(events.NewCollector(doltversion.Version, metricsEmitter))

	globalConfig, ok := dEnv.Config.GetConfig(env.GlobalConfig)
	if !ok {
		cli.PrintErrln(color.RedString("Failed to get global config"))
		return 1
	}

	globalConfig.Iter(func(name, val string) (stop bool) {
		option := strings.ToLower(name)
		if _, ok := config.ConfigOptions[option]; !ok && !strings.HasPrefix(option, env.SqlServerGlobalsPrefix) {
			cli.PrintErrf("Warning: Unknown global config option '%s'. Use `dolt config --global --unset %s` to remove.\n", name, name)
		}
		return false
	})

	// try verifying contents of local config
	localConfig, ok := dEnv.Config.GetConfig(env.LocalConfig)
	if ok {
		localConfig.Iter(func(name, val string) (stop bool) {
			option := strings.ToLower(name)
			if _, ok := config.ConfigOptions[option]; !ok && !strings.HasPrefix(option, env.SqlServerGlobalsPrefix) {
				cli.PrintErrf("Warning: Unknown local config option '%s'. Use `dolt config --local --unset %s` to remove.", name, name)
			}
			return false
		})
	}

	apr, remainingArgs, subcommandName, err := parseGlobalArgsAndSubCommandName(globalConfig, args)
	if err == argparser.ErrHelp {
		doltCommand.PrintUsage("dolt")
		cli.Println(globalSpecialMsg)
		usage()

		return 0
	} else if err != nil {
		cli.PrintErrln(color.RedString("Failure to parse arguments: %v", err))
		return 1
	}

	defer emitUsageEvents(metricsEmitter, args)

	if needsWriteAccess(subcommandName) {
		err = reconfigIfTempFileMoveFails(dEnv)

		if err != nil {
			cli.PrintErrln(color.RedString("Failed to setup the temporary directory. %v`", err))
			return 1
		}
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

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dataDirFS, dEnv.Version, dEnv)
	if err != nil {
		cli.PrintErrln("failed to load database names")
		return 1
	}
	_ = mrEnv.Iter(func(dbName string, dEnv *env.DoltEnv) (stop bool, err error) {
		dsess.DefineSystemVariablesForDB(dbName)
		return false, nil
	})

	// TODO: we set persisted vars here, and this should be deferred until after we know what command line arguments might change them
	err = dsess.InitPersistedSystemVars(dEnv)
	if err != nil {
		cli.Printf("error: failed to load persisted global variables: %s\n", err.Error())
	}

	var cliCtx cli.CliContext = nil
	if initCliContext(subcommandName) {
		// validate that --user and --password are set appropriately.
		aprAlt, creds, err := cli.BuildUserPasswordPrompt(apr)
		apr = aprAlt
		if err != nil {
			cli.PrintErrln(color.RedString("Failed to parse credentials: %v", err))
			return 1
		}

		lateBind, err := buildLateBinder(ctx, cwdFS, dEnv, mrEnv, creds, apr, subcommandName, verboseEngineSetup)

		if err != nil {
			cli.PrintErrln(color.RedString("%v", err))
			return 1
		}

		cliCtx, err = cli.NewCliContext(apr, dEnv.Config, lateBind)
		if err != nil {
			cli.PrintErrln(color.RedString("Unexpected Error: %v", err))
			return 1
		}
	} else {
		if args[0] != subcommandName {
			if supportsGlobalArgs(subcommandName) {
				cli.PrintErrln(
					`Global arguments are not supported for this command as it has not yet been migrated to function in a remote context. 
If you're interested in running this command against a remote host, hit us up on discord (https://discord.gg/gqr7K4VNKe).`)
			} else {
				cli.PrintErrln(
					`This command does not support global arguments. Please try again without the global arguments 
or check the docs for questions about usage.`)
			}
			return 1
		}
	}

	ctx, stop := context.WithCancel(ctx)
	res := doltCommand.Exec(ctx, "dolt", remainingArgs, dEnv, cliCtx)
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

// buildLateBinder builds a LateBindQueryist for which is used to obtain the Queryist used for the length of the
// command execution.
func buildLateBinder(ctx context.Context, cwdFS filesys.Filesys, rootEnv *env.DoltEnv, mrEnv *env.MultiRepoEnv, creds *cli.UserPassword, apr *argparser.ArgParseResults, subcommandName string, verbose bool) (cli.LateBindQueryist, error) {

	var targetEnv *env.DoltEnv = nil

	useDb, hasUseDb := apr.GetValue(commands.UseDbFlag)
	useBranch, hasBranch := apr.GetValue(cli.BranchParam)

	if subcommandName == "fetch" || subcommandName == "pull" || subcommandName == "push" {
		if apr.Contains(cli.HostFlag) {
			return nil, fmt.Errorf(`The %s command is not supported against a remote host yet. 
If you're interested in running this command against a remote host, hit us up on discord (https://discord.gg/gqr7K4VNKe).`, subcommandName)
		}
	}

	if hasUseDb && hasBranch {
		dbName, branchNameInDb := dsess.SplitRevisionDbName(useDb)
		if len(branchNameInDb) != 0 {
			return nil, fmt.Errorf("Ambiguous branch name: %s or %s", branchNameInDb, useBranch)
		}
		useDb = dbName + "/" + useBranch
	}
	// If the host flag is given, we are forced to use a remote connection to a server.
	host, hasHost := apr.GetValue(cli.HostFlag)
	if hasHost {
		if !hasUseDb && subcommandName != "sql" {
			return nil, fmt.Errorf("The --%s flag requires the additional --%s flag.", cli.HostFlag, commands.UseDbFlag)
		}

		port, hasPort := apr.GetInt(cli.PortFlag)
		if !hasPort {
			port = 3306
		}
		useTLS := !apr.Contains(cli.NoTLSFlag)
		return sqlserver.BuildConnectionStringQueryist(ctx, cwdFS, creds, apr, host, port, useTLS, useDb)
	} else {
		_, hasPort := apr.GetInt(cli.PortFlag)
		if hasPort {
			return nil, fmt.Errorf("The --%s flag is only meaningful with the --%s flag.", cli.PortFlag, cli.HostFlag)
		}
	}

	if hasUseDb {
		dbName, _ := dsess.SplitRevisionDbName(useDb)
		targetEnv = mrEnv.GetEnv(dbName)
		if targetEnv == nil {
			return nil, fmt.Errorf("The provided --use-db %s does not exist.", dbName)
		}
	} else {
		useDb = mrEnv.GetFirstDatabase()
		if hasBranch {
			useDb += "/" + useBranch
		}
	}

	// If our targetEnv is still |nil| and we don't have an environment
	// which we will be using based on |useDb|, then our initialization
	// here did not find a repository we will be operating against.
	noValidRepository := targetEnv == nil && (useDb == "" || mrEnv.GetEnv(useDb) == nil)

	// Not having a valid repository as we start to execute a CLI command
	// implementation is allowed for a small number of commands.  We don't
	// expect this set of commands to grow, so we list them here.
	//
	// This is also allowed when --help is passed. So we defer the error
	// until the caller tries to use the cli.LateBindQueryist.
	isValidRepositoryRequired := subcommandName != "init" && subcommandName != "sql" && subcommandName != "sql-server" && subcommandName != "sql-client"

	if noValidRepository && isValidRepositoryRequired {
		return func(ctx context.Context) (cli.Queryist, *sql.Context, func(), error) {
			err := fmt.Errorf("The current directory is not a valid dolt repository.")
			if errors.Is(rootEnv.DBLoadError, nbs.ErrUnsupportedTableFileFormat) {
				// This is fairly targeted and specific to allow for better error messaging. We should consider
				// breaking this out into its own function if we add more conditions.

				err = fmt.Errorf("The data in this database is in an unsupported format. Please upgrade to the latest version of Dolt.")
			}

			return nil, nil, nil, err
		}, nil
	}

	// nil targetEnv will happen if the user ran a command in an empty directory or when there is a server running with
	// no databases. CLI will try to connect to the server in this case.
	if targetEnv == nil {
		targetEnv = rootEnv
	}

	var lookForServer bool
	if targetEnv.DoltDB != nil && targetEnv.IsAccessModeReadOnly() {
		// If the loaded target environment has a DoltDB and we do not
		// have access to it, we look for a server.
		lookForServer = true
	} else if targetEnv.DoltDB == nil {
		// If the loaded environment itself does not have a DoltDB, we
		// may want to look for a server. We do so if all of the
		// repositories in our MultiEnv are ReadOnly. This includes the
		// case where there are no repositories in our MultiEnv
		var allReposAreReadOnly bool = true
		mrEnv.Iter(func(name string, dEnv *env.DoltEnv) (stop bool, err error) {
			if dEnv.DoltDB != nil {
				allReposAreReadOnly = allReposAreReadOnly && dEnv.IsAccessModeReadOnly()
			}
			return !allReposAreReadOnly, nil
		})
		lookForServer = allReposAreReadOnly
	}
	if lookForServer {
		localCreds, err := sqlserver.FindAndLoadLocalCreds(targetEnv.FS)
		if err != nil {
			return nil, err
		}
		if localCreds != nil {
			if verbose {
				cli.Println("verbose: starting remote mode")
			}

			if !creds.Specified {
				creds = &cli.UserPassword{Username: sqlserver.LocalConnectionUser, Password: localCreds.Secret, Specified: false}
			}
			return sqlserver.BuildConnectionStringQueryist(ctx, cwdFS, creds, apr, "localhost", localCreds.Port, false, useDb)
		}
	}

	if verbose {
		cli.Println("verbose: starting local mode")
	}
	return commands.BuildSqlEngineQueryist(ctx, cwdFS, mrEnv, creds, apr)
}

// doc is currently used only when a `initCliContext` command is specified. This will include all commands in time,
// otherwise you only see these docs if you specify a nonsense argument before the `sql` subcommand.
var doc = cli.CommandDocumentationContent{
	ShortDesc: "Dolt is git for data",
	LongDesc:  `Dolt comprises of multiple subcommands that allow users to import, export, update, and manipulate data with SQL.`,

	Synopsis: []string{
		"<--data-dir=<path>> subcommand <subcommand arguments>",
	},
}

func seedGlobalRand() {
	bs := make([]byte, 8)
	_, err := crand.Read(bs)
	if err != nil {
		panic("failed to initial rand " + err.Error())
	}
	rand.Seed(int64(binary.LittleEndian.Uint64(bs)))
}

// emitUsageEvents is called after a command is run to emit usage events and send them to metrics servers.
// Two controls of this behavior are possible:
//  1. The config key |metrics.disabled|, when set to |true|, disables all metrics emission
//  2. The environment key |DOLT_DISABLE_EVENT_FLUSH| allows writing events to disk but not sending them to the server.
//     This is mostly used for testing.
func emitUsageEvents(emitter events.Emitter, args []string) {
	// write events
	collector := events.GlobalCollector()
	ctx := context.Background()
	_ = emitter.LogEvents(ctx, doltversion.Version, collector.Close())

	// flush events
	if !eventFlushDisabled && len(args) > 0 && shouldFlushEvents(args[0]) {
		_ = flushEventsDir()
	}
}

// flushEventsDir flushes all logged events in a separate process.
// This is done without blocking so that the main process can exit immediately in the case of a slow network.
func flushEventsDir() error {
	path, err := os.Executable()
	if err != nil {
		return err
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	cmd := exec.Command(absPath, commands.SendMetricsCommand)

	if err := cmd.Start(); err != nil {
		return err
	}

	return nil
}

func shouldFlushEvents(command string) bool {
	ignoreCommands := map[string]struct{}{
		commands.SendMetricsCommand: {},
		"init":                      {},
		"config":                    {},
	}
	_, ok := ignoreCommands[command]
	return !ok
}

func interceptSendMetrics(ctx context.Context, args []string) (bool, int) {
	if len(args) < 1 || args[0] != commands.SendMetricsCommand {
		return false, 0
	}
	dEnv := env.LoadWithoutDB(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltversion.Version)
	return true, doltCommand.Exec(ctx, "dolt", args, dEnv, nil)
}

// parseGlobalArgsAndSubCommandName parses the global arguments, including a profile if given or a default profile if exists. Also returns the subcommand name.
func parseGlobalArgsAndSubCommandName(globalConfig config.ReadWriteConfig, args []string) (apr *argparser.ArgParseResults, remaining []string, subcommandName string, err error) {
	apr, remaining, err = globalArgParser.ParseGlobalArgs(args)
	if err != nil {
		return nil, nil, "", err
	}

	subcommandName = remaining[0]

	useDefaultProfile := false
	profileName, hasProfile := apr.GetValue(commands.ProfileFlag)
	encodedProfiles, err := globalConfig.GetString(commands.GlobalCfgProfileKey)
	if err != nil {
		if err == config.ErrConfigParamNotFound {
			if hasProfile {
				return nil, nil, "", fmt.Errorf("no profiles found")
			} else {
				return apr, remaining, subcommandName, nil
			}
		} else {
			return nil, nil, "", err
		}
	}
	profiles, err := commands.DecodeProfile(encodedProfiles)
	if err != nil {
		return nil, nil, "", err
	}

	if !hasProfile && supportsGlobalArgs(subcommandName) {
		defaultProfile := gjson.Get(profiles, commands.DefaultProfileName)
		if defaultProfile.Exists() {
			args = append([]string{"--profile", commands.DefaultProfileName}, args...)
			apr, remaining, err = globalArgParser.ParseGlobalArgs(args)
			if err != nil {
				return nil, nil, "", err
			}
			profileName, _ = apr.GetValue(commands.ProfileFlag)
			useDefaultProfile = true
		}
	}

	if hasProfile || useDefaultProfile {
		profileArgs, err := getProfile(apr, profileName, profiles)
		if err != nil {
			return nil, nil, "", err
		}
		args = append(profileArgs, args...)
		apr, remaining, err = globalArgParser.ParseGlobalArgs(args)
		if err != nil {
			return nil, nil, "", err
		}
	}

	return
}

// getProfile retrieves the given profile from the provided list of profiles and returns the args (as flags) and values
// for that profile in a []string. If the profile is not found, an error is returned.
func getProfile(apr *argparser.ArgParseResults, profileName, profiles string) (result []string, err error) {
	prof := gjson.Get(profiles, profileName)
	if prof.Exists() {
		hasPassword := false
		password := ""
		for flag, value := range prof.Map() {
			if !apr.Contains(flag) {
				if flag == cli.PasswordFlag {
					password = value.Str
				} else if flag == "has-password" {
					hasPassword = value.Bool()
				} else if flag == cli.NoTLSFlag {
					if value.Bool() {
						result = append(result, "--"+flag)
						continue
					}
				} else {
					if value.Str != "" {
						result = append(result, "--"+flag, value.Str)
					}
				}
			}
		}
		if !apr.Contains(cli.PasswordFlag) && hasPassword {
			result = append(result, "--"+cli.PasswordFlag, password)
		}
		return result, nil
	} else {
		return nil, fmt.Errorf("profile %s not found", profileName)
	}
}
