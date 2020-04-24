// Copyright 2020 Liquidata, Inc.
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
	"gopkg.in/yaml.v2"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

type BehaviorYAMLConfig struct {
	ReadOnly   bool `yaml:"read_only"`
	AutoCommit bool
}

type UserYAMLConfig struct {
	Name     string
	Password string
}

type DatabaseYAMLConfig struct {
	Name string
	Path string
}

type ListenerYAMLConfig struct {
	HostStr            string `yaml:"host"`
	PortNumber         int    `yaml:"port"`
	MaxConnectoins     uint64 `yaml:"max_connections"`
	ReadTimeoutMillis  uint64 `yaml:"read_timeout_millis"`
	WriteTimeoutMillis uint64 `yaml:"write_timeout_millis"`
}

type YAMLConfig struct {
	LogLevelStr    string               `yaml:"log_level"`
	BehaviorConfig BehaviorYAMLConfig   `yaml:"behavior"`
	UserConfig     UserYAMLConfig       `yaml:"user"`
	ListenerConfig ListenerYAMLConfig   `yaml:"listener"`
	DatabaseConfig []DatabaseYAMLConfig `yaml:"databases"`
}

func (cfg YAMLConfig) String() string {
	data, err := yaml.Marshal(cfg)

	if err != nil {
		return "Failed to marshal as yaml: " + err.Error()
	}

	return string(data)
}

// Host returns the domain that the server will run on. Accepts an IPv4 or IPv6 address, in addition to localhost.
func (cfg YAMLConfig) Host() string {
	return cfg.ListenerConfig.HostStr
}

// Port returns the port that the server will run on. The valid range is [1024, 65535].
func (cfg YAMLConfig) Port() int {
	return cfg.ListenerConfig.PortNumber
}

// ReadTimeout returns the read timeout in milliseconds.
func (cfg YAMLConfig) ReadTimeout() uint64 {
	return cfg.ListenerConfig.ReadTimeoutMillis
}

// WriteTimeout returns the write timeout in milliseconds.
func (cfg YAMLConfig) WriteTimeout() uint64 {
	return cfg.ListenerConfig.WriteTimeoutMillis
}

// User returns the username that connecting clients must use.
func (cfg YAMLConfig) User() string {
	return cfg.UserConfig.Name
}

// Password returns the password that connecting clients must use.
func (cfg YAMLConfig) Password() string {
	return cfg.UserConfig.Password
}

// ReadOnly returns whether the server will only accept read statements or all statements.
func (cfg YAMLConfig) ReadOnly() bool {
	return cfg.BehaviorConfig.ReadOnly
}

// Autocommit defines the value of the @@autocommit session variable used on every connection
func (cfg YAMLConfig) AutoCommit() bool {
	return cfg.BehaviorConfig.AutoCommit
}

// LogLevel returns the level of logging that the server will use.
func (cfg YAMLConfig) LogLevel() LogLevel {
	return LogLevel(cfg.LogLevelStr)
}

// DatabaseNamesAndPaths returns an array of env.EnvNameAndPathObjects corresponding to the databases to be loaded in
// a multiple db configuration. If nil is returned the server will look for a database in the current directory and
// give it a name automatically.
func (cfg YAMLConfig) DatabaseNamesAndPaths() []env.EnvNameAndPath {
	var dbNamesAndPaths []env.EnvNameAndPath
	for _, dbConfig := range cfg.DatabaseConfig {
		dbNamesAndPaths = append(dbNamesAndPaths, env.EnvNameAndPath{Name: dbConfig.Name, Path: dbConfig.Path})
	}

	return dbNamesAndPaths
}

// MaxConnections returns the maximum number of simultaneous connections the server will allow.  The default is 1
func (cfg YAMLConfig) MaxConnections() uint64 {
	return cfg.ListenerConfig.MaxConnectoins
}
