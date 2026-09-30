// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package dbfactory

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/config"
)

func TestGitRemoteHistoryOptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		params  map[string]interface{}
		env     map[string]string
		limit   *int
		reset   *bool
		invalid bool
	}{
		{name: "unset"},
		{name: "explicit zero", params: map[string]interface{}{config.GitRemoteMaxHistoryCommits: "0", config.GitRemoteResetOnPrune: "false"}, limit: intPtr(0), reset: boolPtr(false)},
		{name: "configured", params: map[string]interface{}{config.GitRemoteMaxHistoryCommits: "256", config.GitRemoteResetOnPrune: "true"}, limit: intPtr(256), reset: boolPtr(true)},
		{name: "environment overrides", params: map[string]interface{}{config.GitRemoteMaxHistoryCommits: "invalid", config.GitRemoteResetOnPrune: "invalid"}, env: map[string]string{"DOLT_GIT_REMOTE_MAX_HISTORY_COMMITS": "0", "DOLT_GIT_REMOTE_RESET_HISTORY_ON_PRUNE": "false"}, limit: intPtr(0), reset: boolPtr(false)},
		{name: "negative", params: map[string]interface{}{config.GitRemoteMaxHistoryCommits: "-1"}, invalid: true},
		{name: "empty", env: map[string]string{"DOLT_GIT_REMOTE_MAX_HISTORY_COMMITS": ""}, invalid: true},
		{name: "overflow", params: map[string]interface{}{config.GitRemoteMaxHistoryCommits: "999999999999999999999"}, invalid: true},
		{name: "invalid bool", params: map[string]interface{}{config.GitRemoteResetOnPrune: "yes"}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range []string{"DOLT_GIT_REMOTE_MAX_HISTORY_COMMITS", "DOLT_GIT_REMOTE_RESET_HISTORY_ON_PRUNE"} {
				t.Setenv(key, "")
				require.NoError(t, os.Unsetenv(key))
			}
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			opts, err := gitRemoteHistoryOptions(tc.params)
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.limit, opts.MaxHistoryCommits)
			require.Equal(t, tc.reset, opts.ResetHistoryOnPrune)
		})
	}
}

func intPtr(n int) *int    { return &n }
func boolPtr(b bool) *bool { return &b }
