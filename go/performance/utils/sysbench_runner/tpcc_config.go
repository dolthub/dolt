// Copyright 2022 Dolthub, Inc.
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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
)

var defaultTpccParams = []string{
	fmt.Sprintf("%s=%s", tpccMysqlDbFlag, tpccDbName),
	fmt.Sprintf("%s=%s", tpccDbDriverFlag, mysqlDriverName),
}

// tpccConfigImpl represents a configuration for an execution of the TPCC Benchmark. It executes a series of tests
// against different ServerConfigurations.
type tpccConfigImpl struct {
	// RuntimeOS is the platform the benchmarks ran on
	RuntimeOS string

	// RuntimeGoArch is the runtime architecture
	RuntimeGoArch string

	// ScriptDir represents the location of the TPCC tests
	ScriptDir string

	// Servers are the servers to benchmark.
	Servers []ServerConfig

	// ScaleFactors represent the scale at which to run each TpccBenchmark at.
	ScaleFactors []int

	// NomsBinFormat specifies the NomsBinFormat
	NomsBinFormat string
}

var _ TpccConfig = &tpccConfigImpl{}

func NewTpccRunnerConfig() *tpccConfigImpl {
	return &tpccConfigImpl{
		Servers:      make([]ServerConfig, 0),
		ScaleFactors: make([]int, 0),
	}
}

func (c *tpccConfigImpl) GetRuns() int {
	return 1
}

func (c *tpccConfigImpl) GetScriptDir() string {
	return c.ScriptDir
}

func (c *tpccConfigImpl) GetNomsBinFormat() string {
	return c.NomsBinFormat
}

func (c *tpccConfigImpl) GetRuntimeOs() string {
	return c.RuntimeOS
}

func (c *tpccConfigImpl) GetRuntimeGoArch() string {
	return c.RuntimeGoArch
}

func (c *tpccConfigImpl) ContainsServerOfType(st ServerType) bool {
	for _, s := range c.Servers {
		if s.GetServerType() == st {
			return true
		}
	}
	return false
}

func (c *tpccConfigImpl) GetScaleFactors() []int {
	return c.ScaleFactors
}

func (c *tpccConfigImpl) GetServerConfigs() []ServerConfig {
	return c.Servers
}

func (c *tpccConfigImpl) setDefaults() {
	// TODO: Eventually we need to support scale factors all the way to 10
	if len(c.ScaleFactors) == 0 {
		c.ScaleFactors = append(c.ScaleFactors, 1)
	}
	if c.RuntimeOS == "" {
		c.RuntimeOS = runtime.GOOS
	}
	if c.RuntimeGoArch == "" {
		c.RuntimeGoArch = runtime.GOARCH
	}
}

// validateServerConfigs ensures the ServerConfigs are valid
func (c *tpccConfigImpl) validateServerConfigs() error {
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

func (c *tpccConfigImpl) Validate(ctx context.Context) error {
	if len(c.Servers) < 1 {
		return ErrNoServersDefined
	}
	if len(c.Servers) > 2 {
		return ErrTooManyServersDefined
	}
	c.setDefaults()
	return c.validateServerConfigs()
}

// FromFileTpccConfig returns a validated TpccConfig based on the config file at the configPath
func FromFileTpccConfig(configPath string) (TpccConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := NewTpccRunnerConfig()
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
