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

type ServerType string

type ServerConfig interface {
	GetId() string
	GetHost() string
	GetPort() int
	GetVersion() string
	GetServerExec() string
	GetResultsFormat() string
	GetServerType() ServerType
	GetServerArgs() ([]string, error)
	GetTestingParams(testConfig TestConfig) TestParams
	Validate() error
	SetDefaults() error
}

type InitServerConfig interface {
	ServerConfig
	GetInitDbExec() string
}

type ProtocolServerConfig interface {
	ServerConfig
	GetConnectionProtocol() string
	GetSocket() string
}

type ProfilingServerConfig interface {
	ServerConfig
	GetServerProfile() ServerProfile
	GetProfilePath() string
}
