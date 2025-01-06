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

package sqlserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/svcs"
)

const (
	hostFlag                    = "host"
	portFlag                    = "port"
	passwordFlag                = "password"
	timeoutFlag                 = "timeout"
	readonlyFlag                = "readonly"
	logLevelFlag                = "loglevel"
	noAutoCommitFlag            = "no-auto-commit"
	configFileFlag              = "config"
	queryParallelismFlag        = "query-parallelism"
	maxConnectionsFlag          = "max-connections"
	allowCleartextPasswordsFlag = "allow-cleartext-passwords"
	socketFlag                  = "socket"
	remotesapiPortFlag          = "remotesapi-port"
	remotesapiReadOnlyFlag      = "remotesapi-readonly"
	goldenMysqlConn             = "golden"
	eventSchedulerStatus        = "event-scheduler"
)

func indentLines(s string) string {
	sb := strings.Builder{}
	lines := strings.Split(s, "\n")
	for _, line := range lines {
		sb.WriteRune('\t')
		sb.WriteString(line)
		sb.WriteRune('\n')
	}
	return sb.String()
}

var sqlServerDocs = cli.CommandDocumentationContent{
	ShortDesc: "Start a MySQL-compatible server.",
	LongDesc: "By default, starts a MySQL-compatible server on the dolt database in the current directory. " +
		"Databases are named after the directories they appear in." +
		"Parameters can be specified using a yaml configuration file passed to the server via " +
		"{{.EmphasisLeft}}--config <file>{{.EmphasisRight}}, or by using the supported switches and flags to configure " +
		"the server directly on the command line. If {{.EmphasisLeft}}--config <file>{{.EmphasisRight}} is provided all" +
		" other command line arguments are ignored.\n\nThis is an example yaml configuration file showing all supported" +
		" items and their default values:\n\n" +
		indentLines(servercfg.ServerConfigAsYAMLConfig(DefaultCommandLineServerConfig()).String()) + "\n\n" + `
SUPPORTED CONFIG FILE FIELDS:

{{.EmphasisLeft}}data_dir{{.EmphasisRight}}: A directory where the server will load dolt databases to serve, and create new ones. Defaults to the current directory.

{{.EmphasisLeft}}cfg_dir{{.EmphasisRight}}: A directory where the server will load and store non-database configuration data, such as permission information. Defaults {{.EmphasisLeft}}$data_dir/.doltcfg{{.EmphasisRight}}.

{{.EmphasisLeft}}log_level{{.EmphasisRight}}: Level of logging provided. Options are: {{.EmphasisLeft}}trace{{.EmphasisRight}}, {{.EmphasisLeft}}debug{{.EmphasisRight}}, {{.EmphasisLeft}}info{{.EmphasisRight}}, {{.EmphasisLeft}}warning{{.EmphasisRight}}, {{.EmphasisLeft}}error{{.EmphasisRight}}, and {{.EmphasisLeft}}fatal{{.EmphasisRight}}.

{{.EmphasisLeft}}privilege_file{{.EmphasisRight}}: "Path to a file to load and store users and grants. Defaults to {{.EmphasisLeft}}$doltcfg-dir/privileges.db{{.EmphasisRight}}. Will be created as needed.

{{.EmphasisLeft}}branch_control_file{{.EmphasisRight}}: Path to a file to load and store branch control permissions. Defaults to {{.EmphasisLeft}}$doltcfg-dir/branch_control.db{{.EmphasisRight}}. Will be created as needed.

{{.EmphasisLeft}}max_logged_query_len{{.EmphasisRight}}: If greater than zero, truncates query strings in logging to the number of characters given.

{{.EmphasisLeft}}behavior.read_only{{.EmphasisRight}}: If true database modification is disabled. Defaults to false.

{{.EmphasisLeft}}behavior.autocommit{{.EmphasisRight}}: If true every statement is committed automatically. Defaults to true. @@autocommit can also be specified in each session.

{{.EmphasisLeft}}behavior.dolt_transaction_commit{{.EmphasisRight}}: If true all SQL transaction commits will automatically create a Dolt commit, with a generated commit message. This is useful when a system working with Dolt wants to create versioned data, but doesn't want to directly use Dolt features such as dolt_commit(). 

{{.EmphasisLeft}}user.name{{.EmphasisRight}}: The username that connections should use for authentication

{{.EmphasisLeft}}user.password{{.EmphasisRight}}: The password that connections should use for authentication.

{{.EmphasisLeft}}listener.host{{.EmphasisRight}}: The host address that the server will run on.  This may be {{.EmphasisLeft}}localhost{{.EmphasisRight}} or an IPv4 or IPv6 address

{{.EmphasisLeft}}listener.port{{.EmphasisRight}}: The port that the server should listen on

{{.EmphasisLeft}}listener.max_connections{{.EmphasisRight}}: The number of simultaneous connections that the server will accept

{{.EmphasisLeft}}listener.read_timeout_millis{{.EmphasisRight}}: The number of milliseconds that the server will wait for a read operation

{{.EmphasisLeft}}listener.write_timeout_millis{{.EmphasisRight}}: The number of milliseconds that the server will wait for a write operation

{{.EmphasisLeft}}listener.require_secure_transport{{.EmphasisRight}}: Boolean flag to turn on TLS/SSL transport

{{.EmphasisLeft}}listener.tls_cert{{.EmphasisRight}}: The path to the TLS certicifcate used for secure transport

{{.EmphasisLeft}}listener.tls_key{{.EmphasisRight}}: The path to the TLS key used for secure transport

{{.EmphasisLeft}}remotesapi.port{{.EmphasisRight}}: A port to listen for remote API operations on. If set to a positive integer, this server will accept connections from clients to clone, pull, etc. databases being served.

{{.EmphasisLeft}}remotesapi.read_only{{.EmphasisRight}}: Boolean flag which disables the ability to perform pushes against the server.

{{.EmphasisLeft}}system_variables{{.EmphasisRight}}: A map of system variable name to desired value for all system variable values to override.

{{.EmphasisLeft}}user_session_vars{{.EmphasisRight}}: A map of user name to a map of session variables to set on connection for each session.

{{.EmphasisLeft}}cluster{{.EmphasisRight}}: Settings related to running this server in a replicated cluster. For information on setting these values, see https://docs.dolthub.com/sql-reference/server/replication

If a config file is not provided many of these settings may be configured on the command line.`,
	Synopsis: []string{
		"--config {{.LessThan}}file{{.GreaterThan}}",
		"[-H {{.LessThan}}host{{.GreaterThan}}] [-P {{.LessThan}}port{{.GreaterThan}}] [-u {{.LessThan}}user{{.GreaterThan}}] [-p {{.LessThan}}password{{.GreaterThan}}] [-t {{.LessThan}}timeout{{.GreaterThan}}] [-l {{.LessThan}}loglevel{{.GreaterThan}}] [--data-dir {{.LessThan}}directory{{.GreaterThan}}] [-r]",
	},
}

type SqlServerCmd struct {
	VersionStr string
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd SqlServerCmd) Name() string {
	return "sql-server"
}

// Description returns a description of the command
func (cmd SqlServerCmd) Description() string {
	return sqlServerDocs.ShortDesc
}

func (cmd SqlServerCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(sqlServerDocs, ap)
}

func (cmd SqlServerCmd) ArgParser() *argparser.ArgParser {
	return cmd.ArgParserWithName(cmd.Name())
}

func (cmd SqlServerCmd) ArgParserWithName(name string) *argparser.ArgParser {
	serverConfig := DefaultCommandLineServerConfig()

	ap := argparser.NewArgParserWithVariableArgs(name)
	ap.SupportsString(configFileFlag, "", "file", "When provided configuration is taken from the yaml config file and all command line parameters are ignored.")
	ap.SupportsString(hostFlag, "H", "host address", fmt.Sprintf("Defines the host address that the server will run on. Defaults to `%v`.", serverConfig.Host()))
	ap.SupportsUint(portFlag, "P", "port", fmt.Sprintf("Defines the port that the server will run on. Defaults to `%v`.", serverConfig.Port()))
	ap.SupportsString(commands.UserFlag, "u", "user", fmt.Sprintf("Defines the server user. Defaults to `%v`. This should be explicit if desired.", serverConfig.User()))
	ap.SupportsString(passwordFlag, "p", "password", fmt.Sprintf("Defines the server password. Defaults to `%v`.", serverConfig.Password()))
	ap.SupportsInt(timeoutFlag, "t", "connection timeout", fmt.Sprintf("Defines the timeout, in seconds, used for connections\nA value of `0` represents an infinite timeout. Defaults to `%v`.", serverConfig.ReadTimeout()))
	ap.SupportsFlag(readonlyFlag, "r", "Disable modification of the database.")
	ap.SupportsString(logLevelFlag, "l", "log level", fmt.Sprintf("Defines the level of logging provided\nOptions are: `trace`, `debug`, `info`, `warning`, `error`, `fatal`. Defaults to `%v`.", serverConfig.LogLevel()))
	ap.SupportsString(commands.DataDirFlag, "", "directory", "Defines a directory to find databases to serve. Defaults to the current directory.")
	ap.SupportsString(commands.MultiDBDirFlag, "", "directory", "Deprecated, use `--data-dir` instead.")
	ap.SupportsString(commands.CfgDirFlag, "", "directory", "Defines a directory that contains non-database storage for dolt. Defaults to `$data-dir/.doltcfg`. Will be created automatically as needed.")
	ap.SupportsFlag(noAutoCommitFlag, "", "Set @@autocommit = off for the server.")
	ap.SupportsInt(queryParallelismFlag, "", "num-go-routines", "Deprecated, no effect in current versions of Dolt")
	ap.SupportsInt(maxConnectionsFlag, "", "max-connections", fmt.Sprintf("Set the number of connections handled by the server. Defaults to `%d`.", serverConfig.MaxConnections()))
	ap.SupportsString(commands.PrivsFilePathFlag, "", "privilege file", "Path to a file to load and store users and grants. Defaults to `$doltcfg-dir/privileges.db`. Will be created as needed.")
	ap.SupportsString(commands.BranchCtrlPathFlag, "", "branch control file", "Path to a file to load and store branch control permissions. Defaults to `$doltcfg-dir/branch_control.db`. Will be created as needed.")
	ap.SupportsString(allowCleartextPasswordsFlag, "", "allow-cleartext-passwords", "Allows use of cleartext passwords. Defaults to false.")
	ap.SupportsOptionalString(socketFlag, "", "socket file", "Path for the unix socket file. Defaults to '/tmp/mysql.sock'.")
	ap.SupportsUint(remotesapiPortFlag, "", "remotesapi port", "Sets the port for a server which can expose the databases in this sql-server over remotesapi, so that clients can clone or pull from this server.")
	ap.SupportsFlag(remotesapiReadOnlyFlag, "", "Disable writes to the sql-server via the push operations. SQL writes are unaffected by this setting.")
	ap.SupportsString(goldenMysqlConn, "", "mysql connection string", "Provides a connection string to a MySQL instance to be used to validate query results")
	ap.SupportsString(eventSchedulerStatus, "", "status", "Determines whether the Event Scheduler is enabled and running on the server. It has one of the following values: 'ON', 'OFF' or 'DISABLED'.")
	return ap
}

// EventType returns the type of the event to log
func (cmd SqlServerCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SQL_SERVER
}

// RequiresRepo indicates that this command does not have to be run from within a dolt data repository directory.
// In this case it is because this command supports the dataDirFlag which can pass in a directory.  In the event that
// that parameter is not provided there is additional error handling within this command to make sure that this was in
// fact run from within a dolt data repository directory.
func (cmd SqlServerCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd SqlServerCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	controller := svcs.NewController()
	newCtx, cancelF := context.WithCancel(context.Background())
	go func() {
		// Here we only forward along the SIGINT if the server starts
		// up successfully.  If the service does not start up
		// successfully, or if WaitForStart() blocks indefinitely, then
		// startServer() should have returned an error and we do not
		// need to Stop the running server or deal with our canceled
		// parent context.
		if controller.WaitForStart() == nil {
			<-ctx.Done()
			controller.Stop()
			cancelF()
		}
	}()

	err := StartServer(newCtx, cmd.VersionStr, commandStr, args, dEnv, cliCtx.WorkingDir(), controller)
	if err != nil {
		cli.Println(color.RedString(err.Error()))
		return 1
	}

	return 0
}

func validateSqlServerArgs(apr *argparser.ArgParseResults) error {
	if apr.NArg() > 0 {
		args := strings.Join(apr.Args, ", ")
		return fmt.Errorf("error: sql-server does not take positional arguments, but found %d: %s", apr.NArg(), args)
	}
	_, multiDbDir := apr.GetValue(commands.MultiDBDirFlag)
	if multiDbDir {
		cli.PrintErrln("WARNING: --multi-db-dir is deprecated, use --data-dir instead")
	}
	return nil
}

// StartServer starts the sql server with the controller provided and blocks until the server is stopped.
func StartServer(ctx context.Context, versionStr, commandStr string, args []string, dEnv *env.DoltEnv, cwd filesys.Filesys, controller *svcs.Controller) error {
	ap := SqlServerCmd{}.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, sqlServerDocs, ap))
	serverConfig, err := ServerConfigFromArgs(ap, help, args, dEnv, cwd)
	if err != nil {
		return err
	}

	err = generateYamlConfigIfNone(ap, help, args, dEnv, serverConfig)
	if err != nil {
		return err
	}

	err = servercfg.ApplySystemVariables(serverConfig, sql.SystemVariables)
	if err != nil {
		return err
	}

	cli.PrintErrf("Starting server with Config %v\n", servercfg.ConfigInfo(serverConfig))

	startError, closeError := Serve(ctx, versionStr, serverConfig, controller, dEnv)
	if startError != nil {
		return startError
	}
	if closeError != nil {
		return closeError
	}

	return nil
}

// GetDataDirPreStart returns the data dir to use for the process. This is called early in the bootstrapping of the process
// to ensure that we know the data dir early. This function first parses the args for the --data-dir flag,
// then attempts to find it in the server's yaml config file if it was specified.
//
// The returned value is non-empty only if we found a data dir. The string will be an absolute path to the data dir. An
// empty string indicates that there was no data dir specified, and the caller should determine the data dir.
//
// If the --data-dir flag is specified in the command line, and the config file, an error is returned.
func GetDataDirPreStart(fs filesys.Filesys, args []string) (string, error) {
	ap := SqlServerCmd{}.ArgParser()
	apr, err := cli.ParseArgs(ap, args, nil)
	if err != nil {
		// Parse failure at this stage is ignored. We'll handle it during command execution, to be more consistent with
		// other commands.
		return "", nil
	}

	cliDataDir, hasDataDirCliArg := apr.GetValue(commands.DataDirFlag)
	if hasDataDirCliArg {
		cliDataDir, err = fs.Abs(cliDataDir)
		if err != nil {
			return "", err
		}
	}

	var cfgDataDir string
	confArg, hasConfArg := apr.GetValue(configFileFlag)
	if hasConfArg {
		reader := DoltServerConfigReader{}
		cfg, err := reader.ReadConfigFile(fs, confArg)
		if err != nil {
			return "", err
		}

		if cfg.DataDir() != "" {
			cfgDataDir, err = fs.Abs(cfg.DataDir())
			if err != nil {
				return "", err
			}
		}
	}

	if cfgDataDir != "" && cliDataDir != "" {
		return "", errors.New("--data-dir specified in both config file and command line. Please specify only one.")
	}
	if cfgDataDir != "" {
		return cfgDataDir, nil
	}
	if cliDataDir != "" {
		return cliDataDir, nil
	}
	return "", nil
}

// ServerConfigFromArgs returns a ServerConfig from the given args
func ServerConfigFromArgs(ap *argparser.ArgParser, help cli.UsagePrinter, args []string, dEnv *env.DoltEnv, cwd filesys.Filesys) (servercfg.ServerConfig, error) {
	return ServerConfigFromArgsWithReader(ap, help, args, dEnv, cwd, DoltServerConfigReader{})
}

// ServerConfigFromArgsWithReader returns a ServerConfig from the given args, using the provided ServerConfigReader
func ServerConfigFromArgsWithReader(
	ap *argparser.ArgParser,
	help cli.UsagePrinter,
	args []string,
	dEnv *env.DoltEnv,
	cwd filesys.Filesys,
	reader ServerConfigReader,
) (servercfg.ServerConfig, error) {
	apr := cli.ParseArgsOrDie(ap, args, help)
	if err := validateSqlServerArgs(apr); err != nil {
		cli.PrintErrln(color.RedString(err.Error()))
		return nil, err
	}

	dataDir, err := dEnv.FS.Abs("")
	if err != nil {
		return nil, err
	}

	serverConfig, err := getServerConfig(cwd, apr, dataDir, reader)
	if err != nil {
		return nil, fmt.Errorf("bad configuration: %w", err)
	}

	if err = setupDoltConfig(dEnv, cwd, apr, serverConfig); err != nil {
		return nil, fmt.Errorf("bad configuration: %w", err)
	}

	return serverConfig, nil
}

// getServerConfig returns ServerConfig that is set either from yaml file if given, if not it is set with values defined
// on command line. Server config variables not defined are set to default values.
func getServerConfig(cwdFS filesys.Filesys, apr *argparser.ArgParseResults, dataDirOverride string, reader ServerConfigReader) (servercfg.ServerConfig, error) {
	cfgFile, ok := apr.GetValue(configFileFlag)
	if !ok {
		return reader.ReadConfigArgs(apr, dataDirOverride)
	}

	cfg, err := reader.ReadConfigFile(cwdFS, cfgFile)
	if err != nil {
		return nil, err
	}

	// if command line user argument was given, override the config file's user and password
	if user, hasUser := apr.GetValue(commands.UserFlag); hasUser {
		if wcfg, ok := cfg.(servercfg.WritableServerConfig); ok {
			pass, _ := apr.GetValue(passwordFlag)
			wcfg.SetUserName(user)
			wcfg.SetPassword(pass)
		}
	}

	if connStr, ok := apr.GetValue(goldenMysqlConn); ok {
		if yamlCfg, ok := cfg.(servercfg.YAMLConfig); ok {
			cli.Println(connStr)
			yamlCfg.GoldenMysqlConn = &connStr
		}
	}

	return cfg, nil
}

// GetClientConfig returns configuration which is suitable for a client to use. The fact that it returns a ServerConfig
// is a little confusing, but it is because the client and server use the same configuration struct. The main difference
// between this method and getServerConfig is that this method required a cli.UserPassword argument. It is created by
// prompting the user, and we don't want the server to follow that code path.
func GetClientConfig(cwdFS filesys.Filesys, creds *cli.UserPassword, apr *argparser.ArgParseResults) (servercfg.ServerConfig, error) {
	cfgFile, hasCfgFile := apr.GetValue(configFileFlag)

	if !hasCfgFile {
		return NewCommandLineConfig(creds, apr, "")
	}

	var yamlCfg servercfg.YAMLConfig
	cfg, err := servercfg.YamlConfigFromFile(cwdFS, cfgFile)
	if err != nil {
		return nil, err
	}
	yamlCfg = cfg.(servercfg.YAMLConfig)

	// if command line user argument was given, replace yaml's user and password
	if creds.Specified {
		yamlCfg.UserConfig.Name = &creds.Username
		yamlCfg.UserConfig.Password = &creds.Password
	}

	if connStr, ok := apr.GetValue(goldenMysqlConn); ok {
		cli.Println(connStr)
		yamlCfg.GoldenMysqlConn = &connStr
	}

	return yamlCfg, nil
}

// setupDoltConfig updates the given server config with where to create .doltcfg directory
func setupDoltConfig(dEnv *env.DoltEnv, cwd filesys.Filesys, apr *argparser.ArgParseResults, config servercfg.ServerConfig) error {
	if _, ok := apr.GetValue(configFileFlag); ok {
		return nil
	}

	serverConfig := config.(*commandLineServerConfig)

	_, dataDirFlag1 := apr.GetValue(commands.MultiDBDirFlag)
	_, dataDirFlag2 := apr.GetValue(commands.DataDirFlag)
	dataDirSpecified := dataDirFlag1 || dataDirFlag2

	var cfgDirPath string
	dataDir := serverConfig.DataDir()
	cfgDir, cfgDirSpecified := apr.GetValue(commands.CfgDirFlag)
	if cfgDirSpecified {
		if !filepath.IsAbs(cfgDir) {
			var err error
			cfgDir, err = cwd.Abs(cfgDir)
			if err != nil {
				return err
			}
		}
		cfgDirPath = cfgDir
	} else if dataDirSpecified {
		cfgDirPath = filepath.Join(dataDir, commands.DefaultCfgDirName)
	} else {
		// Look in parent directory for doltcfg
		parentDirCfg := filepath.Join("..", commands.DefaultCfgDirName)
		parentExists, isDir := dEnv.FS.Exists(parentDirCfg)
		parentDirExists := parentExists && isDir

		// Look in data directory (which is necessarily current directory) for doltcfg
		currDirCfg := filepath.Join(dataDir, commands.DefaultCfgDirName)
		currExists, isDir := dEnv.FS.Exists(currDirCfg)
		currDirExists := currExists && isDir

		// Error if both current and parent exist
		if currDirExists && parentDirExists {
			p1, err := dEnv.FS.Abs(cfgDirPath)
			if err != nil {
				return err
			}
			p2, err := dEnv.FS.Abs(parentDirCfg)
			if err != nil {
				return err
			}
			return commands.ErrMultipleDoltCfgDirs.New(p1, p2)
		}

		// Assign the one that exists, defaults to current if neither exist
		if parentDirExists {
			cfgDirPath = parentDirCfg
		} else {
			cfgDirPath = currDirCfg
		}
	}
	serverConfig.withCfgDir(cfgDirPath)

	if cfgDirSpecified {
		serverConfig.valuesSet[servercfg.CfgDirKey] = struct{}{}
	}

	if dataDirSpecified {
		serverConfig.valuesSet[servercfg.DataDirKey] = struct{}{}
	}

	if privsFp, ok := apr.GetValue(commands.PrivsFilePathFlag); ok {
		serverConfig.withPrivilegeFilePath(privsFp)
		serverConfig.valuesSet[servercfg.PrivilegeFilePathKey] = struct{}{}
	} else {
		path, err := dEnv.FS.Abs(filepath.Join(cfgDirPath, commands.DefaultPrivsName))
		if err != nil {
			return err
		}
		serverConfig.withPrivilegeFilePath(path)
	}

	if branchControlFilePath, ok := apr.GetValue(commands.BranchCtrlPathFlag); ok {
		serverConfig.withBranchControlFilePath(branchControlFilePath)
		serverConfig.valuesSet[servercfg.BranchControlFilePathKey] = struct{}{}
	} else {
		path, err := dEnv.FS.Abs(filepath.Join(cfgDirPath, commands.DefaultBranchCtrlName))
		if err != nil {
			return err
		}
		serverConfig.withBranchControlFilePath(path)
	}

	return nil
}

// generateYamlConfigIfNone creates a YAML config file in the database directory if one is not specified in the args
// and one doesn't already exist in the database directory. The fields of the generated YAML config file are set
// using serverConfig if serverConfig specifies a value for the field, otherwise the field is set to a default value
// or is replaced with a commented-out placeholder.
func generateYamlConfigIfNone(
	ap *argparser.ArgParser,
	help cli.UsagePrinter,
	args []string,
	dEnv *env.DoltEnv,
	serverConfig servercfg.ServerConfig) error {
	const yamlConfigName = "config.yaml"

	apr := cli.ParseArgsOrDie(ap, args, help)
	if err := validateSqlServerArgs(apr); err != nil {
		cli.PrintErrln(color.RedString(err.Error()))
		return err
	}

	if apr.Contains(configFileFlag) {
		return nil
	}

	path := filepath.Join(serverConfig.DataDir(), yamlConfigName)
	exists, _ := dEnv.FS.Exists(path)
	if exists {
		return nil
	}

	yamlConfig := servercfg.ServerConfigSetValuesAsYAMLConfig(serverConfig)

	if connStr, ok := apr.GetValue(goldenMysqlConn); ok {
		yamlConfig.GoldenMysqlConn = &connStr
	}

	generatedYaml := `# This file was generated using your configuration.
# Uncomment and edit lines as necessary to modify your configuration.` + "\n\n" + yamlConfig.VerboseString()

	err := dEnv.FS.WriteFile(path, []byte(generatedYaml), os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
