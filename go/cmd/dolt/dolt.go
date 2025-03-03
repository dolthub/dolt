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
	"github.com/dolthub/dolt/go/cmd/dolt/commands/ci"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/credcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/cvcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/docscmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/indexcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/schcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/cmd/dolt/doltcmd"
	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

var dumpDocsCommand = &commands.DumpDocsCmd{}
var dumpZshCommand = &commands.GenZshCompCmd{}

var commandsWithoutCliCtx = []cli.Command{
	admin.Commands,
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
	commands.ConfigCmd{},
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
	ci.Commands,
	commands.DebugCmd{},
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

var doltCommand = doltcmd.DoltCommand
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

	dtables.DoltCommand = doltCommand
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
					err = errors.New("missing argument for the --feature-version flag")
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

	cfg, terminate, status := createBootstrapConfig(ctx, args)
	if terminate {
		return status
	}
	args = nil

	// This is the dEnv passed to sub-commands, and is used to create the multi-repo environment.
	dEnv := env.LoadWithoutDB(ctx, env.GetCurrentUserHomeDir, cfg.dataDirFS, doltdb.LocalDirDoltDB, doltversion.Version)

	if dEnv.CfgLoadErr != nil {
		cli.PrintErrln(color.RedString("Failed to load the global config. %v", dEnv.CfgLoadErr))
		return 1
	}

	strMetricsDisabled := dEnv.Config.GetStringOrDefault(config.MetricsDisabled, "false")
	var metricsEmitter events.Emitter
	metricsEmitter = events.NewFileEmitter(cfg.homeDir, dbfactory.DoltDir)
	metricsDisabled, err := strconv.ParseBool(strMetricsDisabled)
	if err != nil || metricsDisabled {
		metricsEmitter = events.NullEmitter{}
	}

	events.SetGlobalCollector(events.NewCollector(doltversion.Version, metricsEmitter))

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

	defer emitUsageEvents(metricsEmitter, cfg.subCommand)

	if needsWriteAccess(cfg.subCommand) {
		err = reconfigIfTempFileMoveFails(cfg.dataDirFS)

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

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), cfg.dataDirFS, dEnv.Version, dEnv)
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
	if initCliContext(cfg.subCommand) {
		// validate that --user and --password are set appropriately.
		aprAlt, creds, err := cli.BuildUserPasswordPrompt(cfg.apr)
		if err != nil {
			cli.PrintErrln(color.RedString("Failed to parse credentials: %v", err))
			return 1
		}
		cfg.apr = aprAlt

		lateBind, err := buildLateBinder(ctx, cfg.cwdFs, dEnv, mrEnv, creds, cfg.apr, cfg.subCommand, verboseEngineSetup)

		if err != nil {
			cli.PrintErrln(color.RedString("%v", err))
			return 1
		}

		cliCtx, err = cli.NewCliContext(cfg.apr, dEnv.Config, cfg.cwdFs, lateBind)
		if err != nil {
			cli.PrintErrln(color.RedString("Unexpected Error: %v", err))
			return 1
		}
	} else {
		if cfg.hasGlobalArgs {
			if supportsGlobalArgs(cfg.subCommand) {
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
	res := doltCommand.Exec(ctx, "dolt", cfg.remainingArgs, dEnv, cliCtx)
	stop()

	if err = dbfactory.CloseAllLocalDatabases(); err != nil {
		cli.PrintErrln(err)
		if res == 0 {
			res = 1
		}
	}

	if csMetrics && dEnv.DoltDB(ctx) != nil {
		metricsSummary := dEnv.DoltDB(ctx).CSMetricsSummary()
		cli.Println("Command took", time.Since(start).Seconds())
		cli.PrintErrln(metricsSummary)
	}

	return res
}

// resolveDataDirDeeply goes through three levels of resolution for the data directory. The simple case is to look at
// the --data-dir flag which was provide before the sub-command. When runing sql-server, the data directory can also
// be specificed in the arguments after the sub-command, and the config file. This method will ensure there is only
// one of these three options specified, and return it as an absolute path. If there is an error, or if there are multiple
// options specified, an error is returned.
func resolveDataDirDeeply(gArgs *argparser.ArgParseResults, subCmd string, remainingArgs []string, cwdFs filesys.Filesys) (dataDir string, err error) {
	// global config is the dolt --data-dir <foo> sub-command version. Applies to most CLI commands.
	globalDir, hasGlobalDataDir := gArgs.GetValue(commands.DataDirFlag)
	if hasGlobalDataDir {
		// If a relative path was provided, this ensures we have an absolute path everywhere.
		dd, err := cwdFs.Abs(globalDir)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Failed to get absolute path for %s: %v", dataDir, err))
		}
		dataDir = dd
	}

	if subCmd == (sqlserver.SqlServerCmd{}).Name() {
		// GetDataDirPreStart always returns an absolute path.
		dd, err := sqlserver.GetDataDirPreStart(cwdFs, remainingArgs)
		if err != nil {
			return "", err
		}

		if dd != "" {
			if hasGlobalDataDir {
				return "", errors.New("cannot specify both global --data-dir argument and --data-dir in sql-server config. Please specify only one.")
			}
			dataDir = dd
		}
	}

	if dataDir == "" {
		// No data dir specified, so we default to the current directory.
		return cwdFs.Abs("")
	}

	return dataDir, nil
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
			err := errors.New("The current directory is not a valid dolt repository.")
			if errors.Is(rootEnv.DBLoadError, nbs.ErrUnsupportedTableFileFormat) {
				// This is fairly targeted and specific to allow for better error messaging. We should consider
				// breaking this out into its own function if we add more conditions.

				err = errors.New("The data in this database is in an unsupported format. Please upgrade to the latest version of Dolt.")
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
	if targetEnv.DoltDB(ctx) != nil && targetEnv.IsAccessModeReadOnly(ctx) {
		// If the loaded target environment has a doltDB and we do not
		// have access to it, we look for a server.
		lookForServer = true
	} else if targetEnv.DoltDB(ctx) == nil {
		// If the loaded environment itself does not have a doltDB, we
		// may want to look for a server. We do so if all of the
		// repositories in our MultiEnv are ReadOnly. This includes the
		// case where there are no repositories in our MultiEnv
		var allReposAreReadOnly bool = true
		mrEnv.Iter(func(name string, dEnv *env.DoltEnv) (stop bool, err error) {
			if dEnv.DoltDB(ctx) != nil {
				allReposAreReadOnly = allReposAreReadOnly && dEnv.IsAccessModeReadOnly(ctx)
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
func emitUsageEvents(emitter events.Emitter, subCmd string) {
	// write events
	collector := events.GlobalCollector()
	ctx := context.Background()
	_ = emitter.LogEvents(ctx, doltversion.Version, collector.Close())

	// flush events
	if !eventFlushDisabled && shouldFlushEvents(subCmd) {
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
	dEnv := env.LoadWithoutDB(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, "", doltversion.Version)
	return true, doltCommand.Exec(ctx, "dolt", args, dEnv, nil)
}

// bootstrapConfig is the struct that holds the parsed arguments and other configurations. Most importantly, is holds
// the data dir information for the process. There are multiple ways for users to specify the data directory, and constructing
// the bootstrap config early in the process start up allows us to simplify the startup process.
type bootstrapConfig struct {
	// apr is the parsed global arguments. This will not include hidden administrative arguments, which are pulled out first.
	// This APR will include profile injected arguments.
	apr *argparser.ArgParseResults
	// hasGlobalArgs is true if the global arguments were provided.
	hasGlobalArgs bool

	// dataDir is the absolute path to the data directory. This should be the final word - uses of dataDir downstream should
	// only use this path.
	dataDir string
	// dataDirFS is the filesys.Filesys for the data directory.
	dataDirFS filesys.Filesys
	// cwdFs is the filesys.Filesys where the process started. Used when there are relative path arguments provided by the user.
	cwdFs filesys.Filesys

	// remainingArgs is the remaining arguments after parsing global arguments. This includes the subCommand at location 0
	// always.
	remainingArgs []string
	subCommand    string

	// homeDir is the absolute path to the user's home directory. This is resolved using env.GetCurrentUserHomeDir, and saved
	homeDir string
}

// createBootstrapConfig parses the global arguments, inspects current working directory, loads the profile, and
// even digs into server config to build the bootstrapConfig struct. If all goes well, |cfg| is set to a struct that
// contains all the parsed arguments and other configurations. If there is an error, |cfg| will be nil.
// |terminate| is set to true if the process should end for any reason. Errors or messages to the user will be printed already.
// |status| is the exit code to terminate with, and can be ignored if |terminate| is false.
func createBootstrapConfig(ctx context.Context, args []string) (cfg *bootstrapConfig, terminate bool, status int) {
	lfs := filesys.LocalFS
	cwd, err := lfs.Abs("")
	cwdFs, err := lfs.WithWorkingDir(cwd)
	if err != nil {
		cli.PrintErrln(color.RedString("Failed to load the current working directory: %v", err))
		return nil, true, 1
	}

	tmpEnv := env.LoadWithoutDB(ctx, env.GetCurrentUserHomeDir, cwdFs, "", doltversion.Version)
	var globalConfig config.ReadWriteConfig

	homeDir, err := env.GetCurrentUserHomeDir()
	if err != nil {
		cli.PrintErrln(color.RedString("Failed to load the HOME directory: %v", err))
		return nil, true, 1
	}

	if tmpEnv.CfgLoadErr != nil {
		cli.PrintErrln(color.RedString("Failed to load the global config: %v", tmpEnv.CfgLoadErr))
		return nil, true, 1
	}

	if tmpEnv.Config != nil {
		var ok bool
		globalConfig, ok = tmpEnv.Config.GetConfig(env.GlobalConfig)
		if !ok {
			cli.PrintErrln(color.RedString("Failed to load the global config"))
			return nil, true, 1
		}
	} else {
		panic("runtime error. tmpEnv.Config is nil by no tmpEnv.CfgLoadErr set")
	}

	globalConfig.Iter(func(name, val string) (stop bool) {
		option := strings.ToLower(name)
		if _, ok := config.ConfigOptions[option]; !ok && !strings.HasPrefix(option, env.SqlServerGlobalsPrefix) {
			cli.PrintErrf("Warning: Unknown global config option '%s'. Use `dolt config --global --unset %s` to remove.\n", name, name)
		}
		return false
	})

	_, usage := cli.HelpAndUsagePrinters(globalDocs)
	apr, remainingArgs, err := globalArgParser.ParseGlobalArgs(args)
	if errors.Is(err, argparser.ErrHelp) {
		doltCommand.PrintUsage("dolt")
		cli.Println(globalSpecialMsg)
		usage()
		return nil, true, 0
	} else if err != nil {
		cli.PrintErrln(color.RedString("Failed to parse global arguments: %v", err))
		return nil, true, 1
	}

	hasGlobalArgs := false
	if len(remainingArgs) != len(args) {
		hasGlobalArgs = true
	}

	subCommand := remainingArgs[0]

	// If there is a profile flag, we want to load the profile and inject it's args into the global args.
	useDefaultProfile := false
	profileName, hasProfile := apr.GetValue(commands.ProfileFlag)
	encodedProfiles, err := globalConfig.GetString(commands.GlobalCfgProfileKey)
	if err != nil {
		if err == config.ErrConfigParamNotFound {
			if hasProfile {
				cli.PrintErrln(color.RedString("Unable to load profile: %s. Not found.", profileName))
				return nil, true, 1
			} else {
				// We done. Jump to returning what we have.
			}
		} else {
			cli.Println(color.RedString("Failed to retrieve config key: %v", err))
			return nil, true, 1
		}
	}
	profilesJson, err := commands.DecodeProfile(encodedProfiles)
	if err != nil {
		cli.PrintErrln(color.RedString("Failed to decode profiles: %v", err))
		return nil, true, 1
	}

	if !hasProfile && supportsGlobalArgs(subCommand) {
		defaultProfile := gjson.Get(profilesJson, commands.DefaultProfileName)
		if defaultProfile.Exists() {
			profileName = commands.DefaultProfileName
			useDefaultProfile = true
		}
	}

	if hasProfile || useDefaultProfile {
		apr, err = injectProfileArgs(apr, profileName, profilesJson)
		if err != nil {
			cli.PrintErrln(color.RedString("Failed to inject profile arguments: %v", err))
			return nil, true, 1
		}
	}

	dataDir, err := resolveDataDirDeeply(apr, subCommand, remainingArgs[1:], cwdFs)
	if err != nil {
		cli.PrintErrln(color.RedString("Failed to resolve the data directory: %v", err))
		return nil, true, 1
	}

	dataDirFS, err := cwdFs.WithWorkingDir(dataDir)
	if err != nil {
		cli.PrintErrln(color.RedString("Failed to set the data directory to: %s. %v", dataDir, err))
		return nil, true, 1
	}

	cfg = &bootstrapConfig{
		apr:           apr,
		hasGlobalArgs: hasGlobalArgs,
		remainingArgs: remainingArgs,
		dataDirFS:     dataDirFS,
		dataDir:       dataDir,
		cwdFs:         cwdFs,
		subCommand:    subCommand,
		homeDir:       homeDir,
	}

	return cfg, false, 0
}

// injectProfileArgs retrieves the given |profileName| from the provided |profilesJson| and inject the profile details
// in the provided |apr|. A new ArgParseResults is returned which contains the profile details. If the profile is not
// found, an error is returned.
func injectProfileArgs(apr *argparser.ArgParseResults, profileName, profilesJson string) (aprUpdated *argparser.ArgParseResults, err error) {
	prof := gjson.Get(profilesJson, profileName)
	aprUpdated = apr
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
						// There is currently no way to unset a flag, but setting is to the empty string at least sets it to true.
						aprUpdated, err = aprUpdated.SetArgument(flag, "")
						if err != nil {
							return nil, err
						}
					}
				} else {
					if value.Str != "" {
						aprUpdated, err = aprUpdated.SetArgument(flag, value.Str)
						if err != nil {
							return nil, err
						}
					}
				}
			}
		}
		if !apr.Contains(cli.PasswordFlag) && hasPassword {
			aprUpdated, err = aprUpdated.SetArgument(cli.PasswordFlag, password)
			if err != nil {
				return nil, err
			}
		}
		return aprUpdated, nil
	} else {
		return nil, fmt.Errorf("profile %s not found", profileName)
	}
}
