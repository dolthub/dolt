// Copyright 2019 Liquidata, Inc.
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

package env

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dbfactory"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/config"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/set"
)

const (
	localConfigName  = "local"
	globalConfigName = "global"

	UserEmailKey = "user.email"
	UserNameKey  = "user.name"

	// should be able to have remote specific creds?
	UserCreds = "user.creds"

	DoltEditor = "core.editor"

	RemotesApiHostKey     = "remotes.default_host"
	RemotesApiHostPortKey = "remotes.default_port"

	AddCredsUrlKey = "creds.add_url"
)

var LocalConfigWhitelist = set.NewStrSet([]string{UserNameKey, UserEmailKey})
var GlobalConfigWhitelist = set.NewStrSet([]string{UserNameKey, UserEmailKey})

// DoltConfigElement is an enum representing the elements that make up the ConfigHierarchy
type DoltConfigElement int

const (
	// LocalConfig is the repository's local config portion of the ConfigHierarchy
	LocalConfig DoltConfigElement = iota

	// GlobalConfig is the user's global config portion of the ConfigHierarchy
	GlobalConfig
)

// String gives the string name of an element that was used when it was added to the ConfigHierarchy, which is the
// same name that is used to retrieve that element of the string hierarchy.
func (ce DoltConfigElement) String() string {
	switch ce {
	case LocalConfig:
		return localConfigName
	case GlobalConfig:
		return globalConfigName
	}

	return ""
}

// DoltCliConfig is the config for the cli
type DoltCliConfig struct {
	config.ReadableConfig

	ch *config.ConfigHierarchy
	fs filesys.ReadWriteFS
}

func loadDoltCliConfig(hdp HomeDirProvider, fs filesys.ReadWriteFS) (*DoltCliConfig, error) {
	ch := config.NewConfigHierarchy()

	gPath, err := getGlobalCfgPath(hdp)
	lPath := getLocalConfigPath()

	gCfg, err := ensureGlobalConfig(gPath, fs)

	if err != nil {
		return nil, err
	}

	ch.AddConfig(globalConfigName, gCfg)

	if exists, _ := fs.Exists(lPath); exists {
		lCfg, err := config.FromFile(lPath, fs)

		if err == nil {
			ch.AddConfig(localConfigName, lCfg)
		}
	}

	return &DoltCliConfig{ch, ch, fs}, nil
}

func ensureGlobalConfig(path string, fs filesys.ReadWriteFS) (config.ReadWriteConfig, error) {
	if exists, isDir := fs.Exists(path); exists {
		if isDir {
			return nil, errors.New("A directory exists where this file should be. path: " + path)
		}

		return config.FromFile(path, fs)
	}

	return config.NewFileConfig(path, fs, map[string]string{})
}

// CreateLocalConfig creates a new repository local config file.  The current directory must have already been initialized
// as a data repository before a local config can be created.
func (dcc *DoltCliConfig) CreateLocalConfig(vals map[string]string) error {
	return dcc.createLocalConfigAt(".", vals)
}

func (dcc *DoltCliConfig) createLocalConfigAt(dir string, vals map[string]string) error {
	doltDir := filepath.Join(dir, dbfactory.DoltDir)
	if exists, isDir := dcc.fs.Exists(doltDir); !exists {
		return errors.New(dbfactory.DoltDir + " directory not found. Is the current directory a repository directory?")
	} else if !isDir {
		return errors.New("A file exists with the name \"" + dbfactory.DoltDir + "\". This is not a valid file within a data repository directory.")
	}

	path := filepath.Join(dir, getLocalConfigPath())
	cfg, err := config.NewFileConfig(path, dcc.fs, vals)

	if err != nil {
		return err
	}

	dcc.ch.AddConfig(localConfigName, cfg)

	return nil
}

// GetConfig retrieves a specific element of the config hierarchy.
func (dcc *DoltCliConfig) GetConfig(element DoltConfigElement) (config.ReadWriteConfig, bool) {
	return dcc.ch.GetConfig(element.String())
}

// GetStringOrDefault retrieves a string from the config hierarchy and returns it if available.  Otherwise it returns
// the default string value
func (dcc *DoltCliConfig) GetStringOrDefault(key, defStr string) *string {
	val, err := dcc.ch.GetString(key)

	if err != nil {
		return &defStr
	}

	return &val
}

// IfEmptyUseConfig looks at a strings value and if it is an empty string will try to return a value from the config
// hierarchy.  If it is missing in the config a pointer to an empty string will be returned.
func (dcc *DoltCliConfig) IfEmptyUseConfig(val, key string) string {
	if len(strings.TrimSpace(val)) > 0 {
		return val
	}

	cfgVal, err := dcc.ch.GetString(key)

	if err != nil {
		s := ""
		return s
	}

	return cfgVal
}
