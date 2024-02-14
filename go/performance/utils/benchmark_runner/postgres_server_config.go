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

type postgresServerConfigImpl struct {
	// Id is a unique id for this servers benchmarking
	Id string

	// Host is the server host
	Host string

	// Port is the server port
	Port int

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

	// ServerArgs are the args used to start a server
	ServerArgs []string
}

var _ InitServerConfig = &postgresServerConfigImpl{}

func NewPostgresServerConfig(version, serverExec, initDbExec, serverUser, host, resultsFormat string, port int, serverArgs []string) *postgresServerConfigImpl {
	return &postgresServerConfigImpl{
		Id:            uuid.New().String(),
		Host:          host,
		Port:          port,
		Version:       version,
		ResultsFormat: resultsFormat,
		ServerExec:    serverExec,
		InitExec:      initDbExec,
		ServerUser:    serverUser,
		ServerArgs:    serverArgs,
	}
}

func (sc *postgresServerConfigImpl) GetServerExec() string {
	return sc.ServerExec
}

func (sc *postgresServerConfigImpl) GetInitDbExec() string {
	return sc.InitExec
}

func (sc *postgresServerConfigImpl) GetId() string {
	return sc.Id
}

func (sc *postgresServerConfigImpl) GetHost() string {
	return sc.Host
}

func (sc *postgresServerConfigImpl) GetPort() int {
	return sc.Port
}

func (sc *postgresServerConfigImpl) GetVersion() string {
	return sc.Version
}

func (sc *postgresServerConfigImpl) GetResultsFormat() string {
	return sc.ResultsFormat
}

func (sc *postgresServerConfigImpl) GetServerType() ServerType {
	return Postgres
}

func (sc *postgresServerConfigImpl) GetServerArgs() ([]string, error) {
	params := make([]string, 0)
	if sc.Port != 0 {
		params = append(params, fmt.Sprintf("%s=%d", portFlag, sc.Port))
	}
	params = append(params, sc.ServerArgs...)
	return params, nil
}

func (sc *postgresServerConfigImpl) GetTestingParams(testConfig TestConfig) TestParams {
	params := NewSysbenchTestParams()
	params.Append(defaultSysbenchParams...)
	params.Append(fmt.Sprintf("%s=%s", sysbenchDbDriverFlag, sysbenchPostgresDbDriver))
	params.Append(fmt.Sprintf("%s=%s", sysbenchPostgresDbFlag, dbName))
	params.Append(fmt.Sprintf("%s=%s", sysbenchPostgresHostFlag, sc.Host))
	params.Append(fmt.Sprintf("%s=%s", sysbenchPostgresUserFlag, postgresUsername))
	if sc.Port != 0 {
		params.Append(fmt.Sprintf("%s=%d", sysbenchPostgresPortFlag, sc.Port))
	}
	params.Append(testConfig.GetOptions()...)
	params.Append(testConfig.GetName())
	return params
}

func (sc *postgresServerConfigImpl) Validate() error {
	if sc.Version == "" {
		return getMustSupplyError("version")
	}
	if sc.ResultsFormat == "" {
		return getMustSupplyError("results format")
	}
	if sc.ServerExec == "" {
		return getMustSupplyError("server exec")
	}
	err := CheckExec(sc.ServerExec, "server exec")
	if err != nil {
		return err
	}
	return CheckExec(sc.InitExec, "initdb exec")
}

func (sc *postgresServerConfigImpl) SetDefaults() error {
	if sc.Host == "" {
		sc.Host = defaultHost
	}
	if sc.Port < 1 {
		sc.Port = defaultPostgresPort
	}
	return nil
}
