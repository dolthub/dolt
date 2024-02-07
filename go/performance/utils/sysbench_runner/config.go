// Copyright 2019-2022 Dolthub, Inc.
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

package sysbench_runner

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

	"github.com/google/uuid"
)

const (
	Dolt     ServerType = "dolt"
	Doltgres ServerType = "doltgres"
	Postgres ServerType = "postgres"
	MySql    ServerType = "mysql"

	CsvFormat  = "csv"
	JsonFormat = "json"

	CsvExt  = ".csv"
	JsonExt = ".json"

	defaultHost = "127.0.0.1"
	defaultPort = 3306

	defaultMysqlSocket = "/var/run/mysqld/mysqld.sock"

	tcpProtocol  = "tcp"
	unixProtocol = "unix"

	sysbenchUsername  = "sysbench"
	sysbenchUserLocal = "'sysbench'@'localhost'"
	sysbenchPassLocal = "sysbenchpass"

	userFlag                    = "--user"
	hostFlag                    = "--host"
	portFlag                    = "--port"
	skipBinLogFlag              = "--skip-log-bin"
	profileFlag                 = "--prof"
	profilePathFlag             = "--prof-path"
	cpuProfile                  = "cpu"
	doltgresDataDirFlag         = "--data-dir"
	MysqlDataDirFlag            = "--datadir"
	MysqlInitializeInsecureFlag = "--initialize-insecure"
)

var (
	ErrTestNameNotDefined            = errors.New("test name not defined")
	ErrNoServersDefined              = errors.New("servers not defined")
	ErrTooManyServersDefined         = errors.New("too many servers defined, two max")
	ErrUnsupportedConnectionProtocol = errors.New("unsupported connection protocol")
)

var defaultSysbenchParams = []string{
	"--db-ps-mode=disable",
	"--rand-type=uniform",
}

var defaultDoltServerParams = []string{"sql-server"}

var defaultSysbenchTests = []*ConfigTest{
	NewConfigTest("oltp_read_only", []string{}, false),
	NewConfigTest("oltp_insert", []string{}, false),
	NewConfigTest("bulk_insert", []string{}, false),
	NewConfigTest("oltp_point_select", []string{}, false),
	NewConfigTest("select_random_points", []string{}, false),
	NewConfigTest("select_random_ranges", []string{}, false),
	NewConfigTest("oltp_write_only", []string{}, false),
	NewConfigTest("oltp_read_write", []string{}, false),
	NewConfigTest("oltp_update_index", []string{}, false),
	NewConfigTest("oltp_update_non_index", []string{}, false),
}

var defaultDoltLuaScripts = map[string]string{
	"covering_index_scan.lua": "covering_index_scan.lua",
	"groupby_scan.lua":        "groupby_scan.lua",
	"index_join.lua":          "index_join.lua",
	"index_join_scan.lua":     "index_join_scan.lua",
	"index_scan.lua":          "index_scan.lua",
	"oltp_delete_insert.lua":  "oltp_delete_insert.lua",
	"table_scan.lua":          "table_scan.lua",
	"types_delete_insert.lua": "types_delete_insert.lua",
	"types_table_scan.lua":    "types_table_scan.lua",
}

// todo: check expressions need to be supported in doltgres for these
// todo: postgres does not have geometry types also
var defaultDoltgresLuaScripts = map[string]string{
	//"covering_index_scan_postgres.lua": "covering_index_scan_postgres.lua",
	//"groupby_scan_postgres.lua":        "groupby_scan_postgres.lua",
	//"index_join_postgres.lua":          "index_join_postgres.lua",
	//"index_join_scan_postgres.lua":     "index_join_scan_postgres.lua",
	//"index_scan_postgres.lua":          "index_scan_postgres.lua",
	//"oltp_delete_insert_postgres.lua":  "oltp_delete_insert_postgres.lua",
	//"table_scan_postgres.lua":          "table_scan_postgres.lua",
	//"types_delete_insert_postgres.lua": "types_delete_insert_postgres.lua",
	//"types_table_scan_postgres.lua":    "types_table_scan_postgres.lua",
}

type ServerType string

// Test is a single sysbench test
type Test struct {
	id string

	// Name is the test name
	Name string

	// Params are the parameters passed to sysbench
	Params []string

	// FromScript indicates if this test is from a lua script
	FromScript bool
}

// Prepare returns a test's args for sysbench's prepare step
func (t *Test) Prepare() []string {
	return withCommand(t.Params, "prepare")
}

// Run returns a test's args for sysbench's run step
func (t *Test) Run() []string {
	return withCommand(t.Params, "run")
}

// Cleanup returns a test's args for sysbench's cleanup step
func (t *Test) Cleanup() []string {
	return withCommand(t.Params, "cleanup")
}

func withCommand(params []string, command string) []string {
	c := make([]string, 0)
	c = append(c, params...)
	return append(c, command)
}

// ConfigTest provides users a way to define a test for multiple tablesizes
type ConfigTest struct {
	// Name is the test name
	Name string

	// N is the number of times a test should run
	N int

	// Options are additional sysbench test options a user can supply to run with this test
	Options []string

	// FromScript is a boolean indicating that this test is from a lua script
	FromScript bool
}

// NewConfigTest returns a ConfigTest containing the supplied args
func NewConfigTest(name string, opts []string, fromScript bool) *ConfigTest {
	options := make([]string, 0)
	options = append(options, opts...)
	return &ConfigTest{
		Name:       name,
		N:          1,
		Options:    options,
		FromScript: fromScript,
	}
}

// GetTests returns a slice of Tests
func (ct *ConfigTest) GetTests(serverConfig *ServerConfig, testIdFunc func() string) ([]*Test, error) {
	if ct.Name == "" {
		return nil, ErrTestNameNotDefined
	}
	if ct.N < 1 {
		ct.N = 1
	}

	params := fromConfigTestParams(ct, serverConfig)
	tests := make([]*Test, 0)

	var idFunc func() string
	if testIdFunc == nil {
		idFunc = func() string {
			return uuid.New().String()
		}
	} else {
		idFunc = testIdFunc
	}

	for i := 0; i < ct.N; i++ {
		p := make([]string, len(params))
		copy(p, params)
		tests = append(tests, &Test{
			id:         idFunc(),
			Name:       ct.Name,
			Params:     p,
			FromScript: ct.FromScript,
		})
	}
	return tests, nil
}

// fromConfigTestParams returns params formatted for sysbench:
// `sysbench [options]... [testname] [command]`
func fromConfigTestParams(ct *ConfigTest, serverConfig *ServerConfig) []string {
	params := make([]string, 0)
	params = append(params, defaultSysbenchParams...)
	if serverConfig.Server == MySql || serverConfig.Server == Dolt {
		params = append(params, fmt.Sprintf("--mysql-db=%s", dbName))
		params = append(params, "--db-driver=mysql")
		params = append(params, fmt.Sprintf("--mysql-host=%s", serverConfig.Host))
		if serverConfig.Port != 0 {
			params = append(params, fmt.Sprintf("--mysql-port=%d", serverConfig.Port))
		}
	} else if serverConfig.Server == Doltgres || serverConfig.Server == Postgres {
		params = append(params, "--db-driver=pgsql")
		params = append(params, fmt.Sprintf("--pgsql-db=%s", dbName))
		params = append(params, fmt.Sprintf("--pgsql-host=%s", serverConfig.Host))
		if serverConfig.Port != 0 {
			params = append(params, fmt.Sprintf("--pgsql-port=%d", serverConfig.Port))
		}
	}

	// handle sysbench user for local mysql server
	if serverConfig.Server == MySql && serverConfig.Host == defaultHost {
		params = append(params, "--mysql-user=sysbench")
		params = append(params, fmt.Sprintf("--mysql-password=%s", sysbenchPassLocal))
	} else if serverConfig.Server == Dolt {
		params = append(params, "--mysql-user=root")
	} else if serverConfig.Server == Doltgres {
		params = append(params, "--pgsql-user=doltgres")
	} else if serverConfig.Server == Postgres {
		params = append(params, "--pgsql-user=postgres")
	}

	params = append(params, ct.Options...)
	params = append(params, ct.Name)
	return params
}

// ServerConfig is the configuration for a server to test against
type ServerConfig struct {
	// Id is a unique id for this servers benchmarking
	Id string

	// Host is the server host
	Host string

	// Port is the server port
	Port int

	// Server is the type of server
	Server ServerType

	// Version is the server version
	Version string

	// ResultsFormat is the format the results should be written in
	ResultsFormat string

	// ServerExec is the path to a server executable
	ServerExec string

	// InitExec is the path to the server init db executable
	InitExec string

	// ServerUser is the user account that should start the server
	ServerUser string

	// SkipLogBin will skip bin logging
	SkipLogBin bool

	// ServerArgs are the args used to start a server
	ServerArgs []string

	// ConnectionProtocol defines the protocol for connecting to the server
	ConnectionProtocol string

	// Socket is the path to the server socket
	Socket string

	// ServerProfile specifies the golang profile to take of a Dolt server
	ServerProfile string

	// ProfilePath path to directory where server profile will be written
	ProfilePath string
}

func (sc *ServerConfig) GetId() string {
	if sc.Id == "" {
		sc.Id = uuid.New().String()
	}
	return sc.Id
}

// GetServerArgs returns the args used to start a server
func (sc *ServerConfig) GetServerArgs() ([]string, error) {
	params := make([]string, 0)

	if sc.Server == Dolt {
		if sc.ServerProfile != "" {
			if sc.ServerProfile == cpuProfile {
				params = append(params, profileFlag, cpuProfile)
			} else {
				return nil, fmt.Errorf("unsupported server profile: %s", sc.ServerProfile)
			}
			if sc.ProfilePath != "" {
				params = append(params, profilePathFlag, sc.ProfilePath)
			}
		}
		params = append(params, defaultDoltServerParams...)
	} else if sc.Server == MySql {
		if sc.ServerUser != "" {
			params = append(params, fmt.Sprintf("%s=%s", userFlag, sc.ServerUser))
		}
		if sc.SkipLogBin {
			params = append(params, skipBinLogFlag)
		}
	}

	if sc.Server == Dolt || sc.Server == Doltgres {
		params = append(params, fmt.Sprintf("%s=%s", hostFlag, sc.Host))
	}
	if sc.Port != 0 {
		params = append(params, fmt.Sprintf("%s=%d", portFlag, sc.Port))
	}

	params = append(params, sc.ServerArgs...)
	return params, nil
}

// Config is the configuration for a benchmarking run
type Config struct {
	// Runs is the number of times to run all tests
	Runs int
	// RuntimeOS is the platform the benchmarks ran on
	RuntimeOS string
	// RuntimeGoArch is the runtime architecture
	RuntimeGoArch string
	// Servers are the servers to benchmark
	Servers []*ServerConfig
	// Tests are the tests to run. If no tests are provided,
	// the default tests will be used
	Tests []*ConfigTest
	// TestOptions a list of sysbench test options to apply to all tests
	TestOptions []string
	// ScriptDir is a path to a directory of lua scripts
	ScriptDir string
	// InitBigRepo downloads a database with existing chunks and commits
	InitBigRepo bool
	// NomsBinFormat specifies the NomsBinFormat
	NomsBinFormat string
}

// NewConfig returns a new Config
func NewConfig() *Config {
	return &Config{
		Servers: make([]*ServerConfig, 0),
	}
}

// Validate checks the config for the required fields and sets defaults
// where necessary
func (c *Config) Validate() error {
	if len(c.Servers) < 1 {
		return ErrNoServersDefined
	}
	if len(c.Servers) > 2 {
		return ErrTooManyServersDefined
	}
	err := c.setDefaults()
	if err != nil {
		return err
	}
	return c.validateServerConfigs()
}

// validateServerConfigs ensures the ServerConfigs are valid
func (c *Config) validateServerConfigs() error {
	portMap := make(map[int]ServerType)
	for _, s := range c.Servers {
		if s.Server != Dolt && s.Server != MySql && s.Server != Doltgres && s.Server != Postgres {
			return fmt.Errorf("unsupported server type: %s", s.Server)
		}

		err := ValidateRequiredFields(string(s.Server), s.Version, s.ResultsFormat)
		if err != nil {
			return err
		}

		if s.Server == MySql {
			err = CheckProtocol(s.ConnectionProtocol)
			if err != nil {
				return err
			}
		}

		if s.Host == "" {
			s.Host = defaultHost
		}

		portMap, err = CheckUpdatePortMap(s, portMap)
		if err != nil {
			return err
		}

		err = CheckExec(s.ServerExec, "server exec")
		if err != nil {
			return err
		}

		if s.Server == Postgres {
			err = CheckExec(s.InitExec, "initdb exec")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Config) Contains(st ServerType) bool {
	for _, s := range c.Servers {
		if s.Server == st {
			return true
		}
	}
	return false
}

func ValidateRequiredFields(server, version, format string) error {
	if server == "" {
		return getMustSupplyError("server")
	}
	if version == "" {
		return getMustSupplyError("version")
	}
	if format == "" {
		return getMustSupplyError("results format")
	}
	return nil
}

// setDefaults sets defaults on the Config
func (c *Config) setDefaults() error {
	if c.RuntimeOS == "" {
		c.RuntimeOS = runtime.GOOS
	}
	if c.RuntimeGoArch == "" {
		c.RuntimeGoArch = runtime.GOARCH
	}
	if len(c.Tests) < 1 {
		fmt.Printf("Preparing to benchmark against default tests\n")
		if c.ScriptDir != "" {
			abs, err := filepath.Abs(c.ScriptDir)
			if err != nil {
				return err
			}
			if _, err := os.Stat(abs); os.IsNotExist(err) {
				return fmt.Errorf("script dir not found: %s", abs)
			}
			c.ScriptDir = abs
		}
		tests, err := getDefaultTests(c)
		if err != nil {
			return err
		}
		c.Tests = tests
	}
	if c.Runs < 1 {
		c.Runs = 1
	}
	return nil
}

// CheckUpdatePortMap returns an error if multiple servers have specified the same port
func CheckUpdatePortMap(serverConfig *ServerConfig, portMap map[int]ServerType) (map[int]ServerType, error) {
	if serverConfig.Port == 0 {
		serverConfig.Port = defaultPort
	}
	srv, ok := portMap[serverConfig.Port]
	if ok && srv != serverConfig.Server {
		return nil, fmt.Errorf("servers have port conflict on port: %d\n", serverConfig.Port)
	}
	if !ok {
		portMap[serverConfig.Port] = serverConfig.Server
	}
	return portMap, nil
}

// CheckExec verifies the binary exists
func CheckExec(path, messageIfMissing string) error {
	if path == "" {
		return getMustSupplyError(messageIfMissing)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if _, err := os.Stat(abs); os.IsNotExist(err) {
		return fmt.Errorf("exec not found: %s", abs)
	}
	return nil
}

// CheckProtocol ensures the given protocol is supported
func CheckProtocol(protocol string) error {
	if protocol == "" {
		return getMustSupplyError("connection protocol")
	}
	if protocol == tcpProtocol || protocol == unixProtocol {
		return nil
	}
	return ErrUnsupportedConnectionProtocol
}

// GetTests returns a slice of Tests created from the
// defined ServerConfig.Tests
func GetTests(config *Config, serverConfig *ServerConfig, testIdFunc func() string) ([]*Test, error) {
	flattened := make([]*Test, 0)
	for _, t := range config.Tests {
		if len(config.TestOptions) > 0 {
			t.Options = append(t.Options, config.TestOptions...)
		}
		tests, err := t.GetTests(serverConfig, testIdFunc)
		if err != nil {
			return nil, err
		}
		flattened = append(flattened, tests...)
	}
	return flattened, nil
}

// FromFileConfig returns a validated Config based on the config file at the configPath
func FromFileConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := NewConfig()
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func getMustSupplyError(name string) error {
	return fmt.Errorf("Must supply %s", name)
}

func getDefaultTests(config *Config) ([]*ConfigTest, error) {
	defaultTests := make([]*ConfigTest, 0)
	defaultTests = append(defaultTests, defaultSysbenchTests...)
	if config.ScriptDir != "" {
		var luaScriptTests []*ConfigTest
		var err error
		if !config.Contains(Doltgres) && !config.Contains(Postgres) {
			luaScriptTests, err = getLuaScriptTestsFromDir(config.ScriptDir, defaultDoltLuaScripts)
		} else {
			luaScriptTests, err = getLuaScriptTestsFromDir(config.ScriptDir, defaultDoltgresLuaScripts)
		}
		if err != nil {
			return nil, err
		}
		defaultTests = append(defaultTests, luaScriptTests...)
	}
	return defaultTests, nil
}

func getLuaScriptTestsFromDir(dir string, toInclude map[string]string) ([]*ConfigTest, error) {
	luaScripts := make([]*ConfigTest, 0)
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	err = filepath.Walk(abs, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		file := filepath.Base(path)
		if _, ok := toInclude[file]; ok {
			luaScripts = append(luaScripts, NewConfigTest(path, []string{}, true))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return luaScripts, nil
}
