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
	"fmt"

	"github.com/google/uuid"
)

type doltgresServerConfigImpl struct {
	// Id is a unique id for this servers benchmarking
	Id string

	// Host is the server host
	Host string

	// Port is the server port
	Port int

	// Version is the server version
	Version string

	// ConfigFilePath is the path to a doltgres config file
	ConfigFilePath string

	// ResultsFormat is the format the results should be written in
	ResultsFormat string

	// ServerExec is the path to a server executable
	ServerExec string

	// ServerUser is the user account that should start the server
	ServerUser string

	// ServerArgs are the args used to start a server
	ServerArgs []string
}

var _ ServerConfig = &doltgresServerConfigImpl{}

func NewDoltgresServerConfig(version, serverExec, serverUser, host, resultsFormat, configFilePath string, port int, serverArgs []string) *doltgresServerConfigImpl {
	return &doltgresServerConfigImpl{
		Id:             uuid.New().String(),
		Host:           host,
		Port:           port,
		Version:        version,
		ConfigFilePath: configFilePath,
		ResultsFormat:  resultsFormat,
		ServerExec:     serverExec,
		ServerUser:     serverUser,
		ServerArgs:     serverArgs,
	}
}

func (sc *doltgresServerConfigImpl) GetServerType() ServerType {
	return Doltgres
}

func (sc *doltgresServerConfigImpl) GetServerExec() string {
	return sc.ServerExec
}

func (sc *doltgresServerConfigImpl) GetResultsFormat() string {
	return sc.ResultsFormat
}

func (sc *doltgresServerConfigImpl) GetServerArgs() ([]string, error) {

	params := make([]string, 0)

	// TODO: doltgres removed support for command line flag configuration
	// TODO: see https://github.com/dolthub/doltgresql/issues/321
	params = append(params, fmt.Sprintf("%s=%s", configFlag, sc.ConfigFilePath))

	//if sc.Host != "" {
	//	params = append(params, fmt.Sprintf("%s=%s", hostFlag, sc.Host))
	//}
	//if sc.Port != 0 {
	//	params = append(params, fmt.Sprintf("%s=%d", portFlag, sc.Port))
	//}

	params = append(params, sc.ServerArgs...)
	return params, nil
}

func (sc *doltgresServerConfigImpl) GetTestingParams(testConfig TestConfig) TestParams {
	params := NewSysbenchTestParams()
	params.Append(defaultSysbenchParams...)
	params.Append(fmt.Sprintf("%s=%s", sysbenchDbDriverFlag, sysbenchPostgresDbDriver))
	params.Append(fmt.Sprintf("%s=%s", sysbenchPostgresDbFlag, dbName))
	params.Append(fmt.Sprintf("%s=%s", sysbenchPostgresHostFlag, sc.Host))
	params.Append(fmt.Sprintf("%s=%s", sysbenchPostgresUserFlag, doltgresUser))
	params.Append(fmt.Sprintf("%s=%s", sysbenchPostgresPasswordFlag, doltgresPassword))
	if sc.Port != 0 {
		params.Append(fmt.Sprintf("%s=%d", sysbenchPostgresPortFlag, sc.Port))
	}
	params.Append(testConfig.GetOptions()...)
	params.Append(testConfig.GetName())
	return params
}

func (sc *doltgresServerConfigImpl) Validate() error {
	if sc.Version == "" {
		return getMustSupplyError("version")
	}
	if sc.ResultsFormat == "" {
		return getMustSupplyError("results format")
	}
	if sc.ServerExec == "" {
		return getMustSupplyError("server exec")
	}
	if sc.ConfigFilePath == "" {
		return getMustSupplyError("config file path")
	}
	return CheckExec(sc.ServerExec, "server exec")
}

func (sc *doltgresServerConfigImpl) SetDefaults() error {
	if sc.Host == "" {
		sc.Host = defaultHost
	}
	if sc.Port < 1 {
		sc.Port = defaultDoltgresPort
	}
	return nil
}

func (sc *doltgresServerConfigImpl) GetId() string {
	return sc.Id
}

func (sc *doltgresServerConfigImpl) GetHost() string {
	return sc.Host
}

func (sc *doltgresServerConfigImpl) GetPort() int {
	return sc.Port
}

func (sc *doltgresServerConfigImpl) GetVersion() string {
	return sc.Version
}
