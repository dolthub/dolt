// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package env

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type historyConfigDialer struct {
	dbfactory.GRPCDialProvider
	root string
}

func (d historyConfigDialer) GitCacheRoot() (string, bool) { return d.root, true }

func TestGitRemoteHistoryConfig_LocalOverridesGlobal(t *testing.T) {
	home, root := t.TempDir(), t.TempDir()
	t.Setenv("DOLT_ROOT_PATH", home)
	require.NoError(t, os.MkdirAll(filepath.Join(home, ".dolt"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".dolt"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(home, ".dolt", "config_global.json"), []byte(`{"git-remote.max-history-commits":"256","git-remote.reset-history-on-prune":"true"}`), 0600))
	dialer := historyConfigDialer{root: root}
	params := make(map[string]interface{})
	require.NoError(t, addGitRemoteHistoryConfig(params, dialer))
	require.Equal(t, "256", params[config.GitRemoteMaxHistoryCommits])
	require.Equal(t, "true", params[config.GitRemoteResetOnPrune])
	require.NoError(t, os.WriteFile(filepath.Join(root, ".dolt", "config.json"), []byte(`{"git-remote.max-history-commits":"0","git-remote.reset-history-on-prune":"false"}`), 0600))
	params = make(map[string]interface{})
	require.NoError(t, addGitRemoteHistoryConfig(params, dialer))
	require.Equal(t, "0", params[config.GitRemoteMaxHistoryCommits])
	require.Equal(t, "false", params[config.GitRemoteResetOnPrune])
}

func TestGitRemoteHistoryConfig_MissingFiles(t *testing.T) {
	home, root := t.TempDir(), t.TempDir()
	t.Setenv("DOLT_ROOT_PATH", home)
	params := make(map[string]interface{})
	require.NoError(t, addGitRemoteHistoryConfig(params, historyConfigDialer{root: root}))
	require.Empty(t, params)
	_, err := os.Stat(filepath.Join(home, ".dolt", "config_global.json"))
	require.ErrorIs(t, err, os.ErrNotExist, "reading remote settings must not create config files")
}

func TestGitRemoteHistoryConfig_InvalidFiles(t *testing.T) {
	for _, scope := range []string{"local", "global"} {
		for _, kind := range []string{"malformed", "directory"} {
			t.Run(scope+"/"+kind, func(t *testing.T) {
				home, root := t.TempDir(), t.TempDir()
				t.Setenv("DOLT_ROOT_PATH", home)
				path := filepath.Join(root, ".dolt", "config.json")
				if scope == "global" {
					path = filepath.Join(home, ".dolt", "config_global.json")
				}
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
				if kind == "directory" {
					require.NoError(t, os.Mkdir(path, 0755))
				} else {
					require.NoError(t, os.WriteFile(path, []byte("{"), 0600))
				}
				err := addGitRemoteHistoryConfig(make(map[string]interface{}), historyConfigDialer{root: root})
				require.ErrorContains(t, err, "loading "+scope+" config")
				fs, err := filesys.LocalFS.WithWorkingDir(root)
				require.NoError(t, err)
				_, err = LoadDoltCliConfig(GetCurrentUserHomeDir, fs)
				require.ErrorContains(t, err, "loading "+scope+" config")
			})
		}
	}
}

type unreadableConfigFS struct {
	filesys.ReadWriteFS
}

func (fs unreadableConfigFS) ReadFile(string) ([]byte, error) {
	return nil, os.ErrPermission
}

func TestLoadDoltConfig_ReadError(t *testing.T) {
	t.Setenv("DOLT_ROOT_PATH", t.TempDir())
	_, err := loadDoltConfig(GetCurrentUserHomeDir, unreadableConfigFS{ReadWriteFS: filesys.LocalFS})
	require.ErrorIs(t, err, os.ErrPermission)
}
