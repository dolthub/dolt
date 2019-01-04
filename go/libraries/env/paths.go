package env

import (
	"os/user"
	"path/filepath"
)

const (
	configFile   = "config.json"
	globalConfig = "config_global.json"

	repoStateFile = "repo_state.json"
)

type HomeDirProvider func() (string, error)

func GetCurrentUserHomeDir() (string, error) {
	if usr, err := user.Current(); err != nil {
		return "", err
	} else {
		return usr.HomeDir, nil
	}
}

func getGlobalCfgPath(hdp HomeDirProvider) (string, error) {
	homeDir, err := hdp()
	if err != nil {
		return "", err
	}

	return filepath.Join(homeDir, DoltDir, globalConfig), nil
}

func getLocalConfigPath() string {
	return filepath.Join(".", DoltDir, configFile)
}

func getDoltDir() string {
	return filepath.Join(".", DoltDir)
}

func getRepoStateFile() string {
	return filepath.Join(getDoltDir(), repoStateFile)
}
