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
