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

package env

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
)

const (
	localConfigName  = "local"
	globalConfigName = "global"

	// should be able to have remote specific creds?

)

// ConfigScope is an enum representing the elements that make up the ConfigHierarchy
type ConfigScope int

const (
	// LocalConfig is the repository's local config portion of the ConfigHierarchy
	LocalConfig ConfigScope = iota

	// GlobalConfig is the user's global config portion of the ConfigHierarchy
	GlobalConfig
)

const (
	// SqlServerGlobalsPrefix is config namespace accessible by the SQL engine (ex: sqlserver.global.key)
	SqlServerGlobalsPrefix = "sqlserver.global"
)

// String gives the string name of an element that was used when it was added to the ConfigHierarchy, which is the
// same name that is used to retrieve that element of the string hierarchy.
func (ce ConfigScope) String() string {
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

var _ config.ReadableConfig = &DoltCliConfig{}

func NewTestDoltCliConfig(gcfg, lcfg config.ReadWriteConfig, fs filesys.Filesys) *DoltCliConfig {
	cfgHierarchy := config.NewConfigHierarchy()

	if gcfg != nil {
		cfgHierarchy.AddConfig(globalConfigName, gcfg)
	}

	if lcfg != nil {
		cfgHierarchy.AddConfig(localConfigName, lcfg)
	}

	return &DoltCliConfig{cfgHierarchy, cfgHierarchy, fs}
}

func NewTestDoltCliConfigFromHierarchy(ch *config.ConfigHierarchy, fs filesys.Filesys) *DoltCliConfig {
	return &DoltCliConfig{ch, ch, fs}
}

func LoadDoltCliConfig(hdp HomeDirProvider, fs filesys.ReadWriteFS) (*DoltCliConfig, error) {
	ch := config.NewConfigHierarchy()

	lPath := getLocalConfigPath()
	if exists, _ := fs.Exists(lPath); exists {
		lCfg, err := config.FromFile(lPath, fs)

		if err == nil {
			ch.AddConfig(localConfigName, lCfg)
		}
	}

	gPath, err := getGlobalCfgPath(hdp)
	if err != nil {
		return nil, err
	}

	gCfg, err := ensureGlobalConfig(gPath, fs)
	if err != nil {
		return nil, err
	}

	ch.AddConfig(globalConfigName, gCfg)

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

// CreateLocalConfig creates a new repository local config file with the values from |val|
// at the directory |dir|. The |dir| directory must have already been initialized
// as a data repository before a local config can be created.
func (dcc *DoltCliConfig) CreateLocalConfig(dir string, vals map[string]string) error {
	return dcc.createLocalConfigAt(dir, vals)
}

func (dcc *DoltCliConfig) createLocalConfigAt(dir string, vals map[string]string) error {
	doltDir := filepath.Join(dir, dbfactory.DoltDir)
	if exists, isDir := dcc.fs.Exists(doltDir); !exists {
		return errors.New(dbfactory.DoltDir + " directory not found. Is the current directory a repository directory?")
	} else if !isDir {
		return errors.New("A file exists with the name \"" + dbfactory.DoltDir + "\". This is not a valid file within a data repository directory.")
	}

	path := filepath.Join(dir, getLocalConfigPath())
	if exists, _ := dcc.fs.Exists(path); exists {
		return nil
	}

	cfg, err := config.NewFileConfig(path, dcc.fs, vals)
	if err != nil {
		return err
	}

	dcc.ch.AddConfig(localConfigName, cfg)


	return nil
}

// GetConfig retrieves a specific element of the config hierarchy.
func (dcc *DoltCliConfig) GetConfig(element ConfigScope) (config.ReadWriteConfig, bool) {
	switch element {
	case LocalConfig, GlobalConfig:
		return dcc.ch.GetConfig(element.String())
	default:
		return nil, false
	}
}

// GetStringOrDefault retrieves a string from the config hierarchy and returns it if available.  Otherwise it returns
// the default string value
func (dcc *DoltCliConfig) GetStringOrDefault(key, defStr string) string {
	return GetStringOrDefault(dcc.ch, key, defStr)
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

func GetStringOrDefault(cfg config.ReadableConfig, key, defStr string) string {
	if cfg == nil {
		return defStr
	}
	val, err := cfg.GetString(key)
	if err != nil {
		return defStr
	}
	return val
}

// GetNameAndEmail returns the name and email from the supplied config
func GetNameAndEmail(cfg config.ReadableConfig) (string, string, error) {
	name, err := cfg.GetString(config.UserNameKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", datas.ErrNameNotConfigured
	} else if err != nil {
		return "", "", err
	}

	email, err := cfg.GetString(config.UserEmailKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", datas.ErrEmailNotConfigured
	} else if err != nil {
		return "", "", err
	}

	return name, email, nil
}

// writeableLocalDoltCliConfig is an extension to DoltCliConfig that reads values from the hierarchy but writes to
// local config.
type writeableLocalDoltCliConfig struct {
	*DoltCliConfig
}

// WriteableConfig returns a ReadWriteConfig reading from this config hierarchy. The config will read from the hierarchy
// and write to the local config if it's available, or the global config otherwise.
func (dcc *DoltCliConfig) WriteableConfig() config.ReadWriteConfig {
	return writeableLocalDoltCliConfig{dcc}
}

// SetFailsafes sets the config values given as failsafes, i.e. values that will be returned as a last resort if they
// are not found elsewhere in the config hierarchy. The "failsafe" config can be written to in order to conform to the
// interface of ConfigHierarchy, but values will not persist beyond this session.
// Calling SetFailsafes more than once will overwrite any previous values.
// Should only be called after primary configuration of the config hierarchy has been completed.
func (dcc DoltCliConfig) SetFailsafes(cfg map[string]string) {
	existing, ok := dcc.ch.GetConfig("failsafe")
	if !ok {
		existing = config.NewEmptyMapConfig()
		dcc.ch.AddConfig("failsafe", existing)
	}

	_ = existing.SetStrings(cfg)
}

const (
	DefaultEmail = "doltuser@dolthub.com"
	DefaultName  = "Dolt System Account"
)

var DefaultFailsafeConfig = map[string]string{
	config.UserEmailKey: DefaultEmail,
	config.UserNameKey:  DefaultName,
}

func (w writeableLocalDoltCliConfig) SetStrings(updates map[string]string) error {
	cfg, ok := w.GetConfig(LocalConfig)
	if !ok {
		cfg, ok = w.GetConfig(GlobalConfig)
		if !ok {
			return errors.New("no local or global config found")
		}
	}

	return cfg.SetStrings(updates)
}

func (w writeableLocalDoltCliConfig) Unset(params []string) error {
	cfg, ok := w.GetConfig(LocalConfig)
	if !ok {
		cfg, ok = w.GetConfig(GlobalConfig)
		if !ok {
			return errors.New("no local or global config found")
		}
	}

	return cfg.Unset(params)
}
