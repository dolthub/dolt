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

package benchmark_runner

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

var (
	ErrTestNameNotDefined            = errors.New("test name not defined")
	ErrNoServersDefined              = errors.New("servers not defined")
	ErrTooManyServersDefined         = errors.New("too many servers defined, two max")
	ErrUnsupportedConnectionProtocol = errors.New("unsupported connection protocol")
)

var defaultSysbenchParams = []string{
	fmt.Sprintf("%s=%s", sysbenchDbPsModeFlag, sysbenchDbPsModeDisable),
	fmt.Sprintf("%s=%s", sysbenchRandTypeFlag, sysbenchRandTypeUniform),
}

var defaultDoltServerParams = []string{doltSqlServerCommand}

var defaultSysbenchTests = []TestConfig{
	// NewTestConfig(sysbenchOltpReadOnlyTestName, []string{}, false),
	// NewTestConfig(sysbenchOltpInsertTestName, []string{}, false),
	// NewTestConfig(sysbenchBulkInsertTestName, []string{}, false),
	// NewTestConfig(sysbenchOltpPointSelectTestName, []string{}, false),
	// NewTestConfig(sysbenchSelectRandomPointsTestName, []string{}, false),
	// NewTestConfig(sysbenchSelectRandomRangesTestName, []string{}, false),
	// NewTestConfig(sysbenchOltpWriteOnlyTestName, []string{}, false),
	// NewTestConfig(sysbenchOltpReadWriteTestName, []string{}, false),
	// NewTestConfig(sysbenchOltpUpdateIndexTestName, []string{}, false),
	// NewTestConfig(sysbenchOltpUpdateNonIndexTestName, []string{}, false),
}

var defaultDoltLuaScripts = map[string]string{
	// sysbenchCoveringIndexScanLuaTestName: sysbenchCoveringIndexScanLuaTestName,
	// sysbenchGroupByScanLuaTestName:       sysbenchGroupByScanLuaTestName,
	sysbenchIndexJoinLuaTestName:         sysbenchIndexJoinLuaTestName,
	sysbenchIndexJoinScanLuaTestName:     sysbenchIndexJoinScanLuaTestName,
	// sysbenchIndexScanLuaTestName:         sysbenchIndexScanLuaTestName,
	// sysbenchOltpDeleteInsertLuaTestName:  sysbenchOltpDeleteInsertLuaTestName,
	// sysbenchTableScanLuaTestName:         sysbenchTableScanLuaTestName,
	// sysbenchTypesDeleteInsertLuaTestName: sysbenchTypesDeleteInsertLuaTestName,
	// sysbenchTypesTableScanLuaTestName:    sysbenchTypesTableScanLuaTestName,
}

var defaultDoltgresLuaScripts = map[string]string{
	sysbenchCoveringIndexScanPostgresLuaTestName: sysbenchCoveringIndexScanPostgresLuaTestName,
	sysbenchGroupByScanPostgresLuaTestName:       sysbenchGroupByScanPostgresLuaTestName,
	sysbenchIndexJoinPostgresLuaTestName:         sysbenchIndexJoinPostgresLuaTestName,
	sysbenchIndexJoinScanPostgresLuaTestName:     sysbenchIndexJoinScanPostgresLuaTestName,
	sysbenchIndexScanPostgresLuaTestName:         sysbenchIndexScanPostgresLuaTestName,
	sysbenchTableScanPostgresLuaTestName:         sysbenchTableScanPostgresLuaTestName,
	sysbenchTypesTableScanPostgresLuaTestName:    sysbenchTypesTableScanPostgresLuaTestName,
	sysbenchOltpDeleteInsertPostgresLuaTestName:  sysbenchOltpDeleteInsertPostgresLuaTestName,
	sysbenchTypesDeleteInsertPostgresLuaTestName: sysbenchTypesDeleteInsertPostgresLuaTestName,
}

// sysbenchRunnerConfigImpl is the configuration for a benchmarking run
type sysbenchRunnerConfigImpl struct {
	// Runs is the number of times to run all tests
	Runs int
	// RuntimeOS is the platform the benchmarks ran on
	RuntimeOS string
	// RuntimeGoArch is the runtime architecture
	RuntimeGoArch string
	// Servers are the servers to benchmark
	Servers []ServerConfig
	// Tests are the tests to run. If no tests are provided,
	// the default tests will be used
	Tests []TestConfig
	// TestOptions a list of sysbench test options to apply to all tests
	TestOptions []string
	// ScriptDir is a path to a directory of lua scripts
	ScriptDir string
	// InitBigRepo downloads a database with existing chunks and commits
	InitBigRepo bool
	// NomsBinFormat specifies the NomsBinFormat
	NomsBinFormat string
}

var _ SysbenchConfig = &sysbenchRunnerConfigImpl{}

// NewRunnerConfig returns a new sysbenchRunnerConfigImpl
func NewRunnerConfig() *sysbenchRunnerConfigImpl {
	return &sysbenchRunnerConfigImpl{
		Servers: make([]ServerConfig, 0),
	}
}

func (c *sysbenchRunnerConfigImpl) GetRuns() int {
	return c.Runs
}

func (c *sysbenchRunnerConfigImpl) GetScriptDir() string {
	return c.ScriptDir
}

func (c *sysbenchRunnerConfigImpl) GetNomsBinFormat() string {
	return c.NomsBinFormat
}

func (c *sysbenchRunnerConfigImpl) GetRuntimeOs() string {
	return c.RuntimeOS
}

func (c *sysbenchRunnerConfigImpl) GetRuntimeGoArch() string {
	return c.RuntimeGoArch
}

func (c *sysbenchRunnerConfigImpl) GetTestConfigs() []TestConfig {
	return c.Tests
}

func (c *sysbenchRunnerConfigImpl) GetTestOptions() []string {
	return c.TestOptions
}

func (c *sysbenchRunnerConfigImpl) GetServerConfigs() []ServerConfig {
	return c.Servers
}

// Validate checks the config for the required fields and sets defaults
// where necessary
func (c *sysbenchRunnerConfigImpl) Validate(ctx context.Context) error {
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
func (c *sysbenchRunnerConfigImpl) validateServerConfigs() error {
	portMap := make(map[int]ServerType)
	for _, s := range c.Servers {
		st := s.GetServerType()
		if st != Dolt && st != MySql && st != Doltgres && st != Postgres {
			return fmt.Errorf("unsupported server type: %s", st)
		}

		err := s.Validate()
		if err != nil {
			return err
		}

		err = s.SetDefaults()
		if err != nil {
			return err
		}

		portMap, err = CheckUpdatePortMap(s, portMap)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *sysbenchRunnerConfigImpl) ContainsServerOfType(st ServerType) bool {
	for _, s := range c.Servers {
		if s.GetServerType() == st {
			return true
		}
	}
	return false
}

// setDefaults sets defaults on the sysbenchRunnerConfigImpl
func (c *sysbenchRunnerConfigImpl) setDefaults() error {
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
		tests, err := c.getDefaultTests()
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

func (c *sysbenchRunnerConfigImpl) getLuaScriptTestsFromDir(toInclude map[string]string) ([]TestConfig, error) {
	luaScripts := make([]TestConfig, 0)
	abs, err := filepath.Abs(c.ScriptDir)
	if err != nil {
		return nil, err
	}
	err = filepath.Walk(abs, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		file := filepath.Base(path)
		if _, ok := toInclude[file]; ok {
			luaScripts = append(luaScripts, NewTestConfig(path, []string{}, true))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return luaScripts, nil
}

func (c *sysbenchRunnerConfigImpl) getDefaultTests() ([]TestConfig, error) {
	defaultTests := make([]TestConfig, 0)
	defaultTests = append(defaultTests, defaultSysbenchTests...)
	if c.ScriptDir != "" {
		var luaScriptTests []TestConfig
		var err error
		if !c.ContainsServerOfType(Doltgres) && !c.ContainsServerOfType(Postgres) {
			luaScriptTests, err = c.getLuaScriptTestsFromDir(defaultDoltLuaScripts)
		} else {
			luaScriptTests, err = c.getLuaScriptTestsFromDir(defaultDoltgresLuaScripts)
		}
		if err != nil {
			return nil, err
		}
		defaultTests = append(defaultTests, luaScriptTests...)
	}
	return defaultTests, nil
}

// CheckUpdatePortMap returns an error if multiple servers have specified the same port
func CheckUpdatePortMap(serverConfig ServerConfig, portMap map[int]ServerType) (map[int]ServerType, error) {
	port := serverConfig.GetPort()
	st := serverConfig.GetServerType()
	srv, ok := portMap[port]
	if ok && srv != st {
		return nil, fmt.Errorf("servers have port conflict on port: %d\n", port)
	}
	if !ok {
		portMap[port] = st
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

// GetTests returns a slice of Tests
func GetTests(config SysbenchConfig, serverConfig ServerConfig) ([]Test, error) {
	flattened := make([]Test, 0)
	for _, t := range config.GetTestConfigs() {
		opts := config.GetTestOptions()
		for _, o := range opts {
			t.AppendOption(o)
		}
		tests, err := t.GetTests(serverConfig)
		if err != nil {
			return nil, err
		}
		flattened = append(flattened, tests...)
	}
	return flattened, nil
}

func getMustSupplyError(name string) error {
	return fmt.Errorf("Must supply %s", name)
}
