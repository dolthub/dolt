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
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dbfactory"
	"os"
	"os/user"
	"path/filepath"
)

const (
	doltRootPathEnvVar = "DOLT_ROOT_PATH"
	credsDir           = "creds"

	configFile   = "config.json"
	globalConfig = "config_global.json"

	repoStateFile = "repo_state.json"
)

// HomeDirProvider is a function that returns the users home directory.  This is where global dolt state is stored for
// the current user
type HomeDirProvider func() (string, error)

// GetCurrentUserHomeDir will return the current user's home directory by default.  This directory is where global dolt
// state will be stored inside of the .dolt directory.  The environment variable DOLT_ROOT_PATH can be used to
// provide a different directory where the root .dolt directory should be located and global state will be stored there.
func GetCurrentUserHomeDir() (string, error) {
	if doltRootPath, ok := os.LookupEnv(doltRootPathEnvVar); ok && doltRootPath != "" {
		return doltRootPath, nil
	}

	if usr, err := user.Current(); err != nil {
		return "", err
	} else {
		return usr.HomeDir, nil
	}
}

func getCredsDir(hdp HomeDirProvider) (string, error) {
	homeDir, err := hdp()
	if err != nil {
		return "", err
	}

	return filepath.Join(homeDir, dbfactory.DoltDir, credsDir), nil
}

func getGlobalCfgPath(hdp HomeDirProvider) (string, error) {
	homeDir, err := hdp()
	if err != nil {
		return "", err
	}

	return filepath.Join(homeDir, dbfactory.DoltDir, globalConfig), nil
}

func getLocalConfigPath() string {
	return filepath.Join(dbfactory.DoltDir, configFile)
}

func getRepoStateFile() string {
	return filepath.Join(dbfactory.DoltDir, repoStateFile)
}
