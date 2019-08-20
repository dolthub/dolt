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

package commands

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/config"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/osutil"
)

func TestGetAbsRemoteUrl(t *testing.T) {
	cwd := osutil.PathToNative("/User/name/datasets")
	testRepoDir := filepath.Join(cwd, "test-repo")
	fs := filesys.NewInMemFS([]string{cwd, testRepoDir}, nil, cwd)
	if osutil.IsWindows {
		cwd = "/" + filepath.ToSlash(cwd)
	}

	tests := []struct {
		str            string
		cfg            *config.MapConfig
		expectedUrl    string
		expectedScheme string
		expectErr      bool
	}{
		{
			"",
			config.NewMapConfig(map[string]string{}),
			"https://doltremoteapi.beta.dolthub.com",
			"https",
			false,
		},
		{
			"ts/emp",
			config.NewMapConfig(map[string]string{}),
			"https://doltremoteapi.beta.dolthub.com/ts/emp",
			"https",
			false,
		},
		{
			"ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostKey: "host.dom",
			}),
			"https://host.dom/ts/emp",
			"https",
			false,
		},
		{
			"http://dolthub.com/ts/emp",
			config.NewMapConfig(map[string]string{}),
			"http://dolthub.com/ts/emp",
			"http",
			false,
		},
		{
			"https://test.org:443/ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostKey: "host.dom",
			}),
			"https://test.org:443/ts/emp",
			"https",
			false,
		},
		{
			"localhost/ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostKey: "host.dom",
			}),
			"https://localhost/ts/emp",
			"https",
			false,
		},
		{
			fmt.Sprintf("file://%s", cwd),
			config.NewMapConfig(map[string]string{}),
			fmt.Sprintf("file://%s", cwd),
			"file",
			false,
		},
		{
			"file://./",
			config.NewMapConfig(map[string]string{}),
			fmt.Sprintf("file://%s", cwd),
			"file",
			false,
		},
		{
			"file://./test-repo",
			config.NewMapConfig(map[string]string{}),
			fmt.Sprintf("file://%s/test-repo", cwd),
			"file",
			false,
		},
		{
			// directory doesnt exist
			"file://./doesnt_exist",
			config.NewMapConfig(map[string]string{}),
			"",
			"",
			true,
		},
		{
			":/:/:/", // intended to fail earl.Parse
			config.NewMapConfig(map[string]string{}),
			"",
			"",
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.str, func(t *testing.T) {
			actualScheme, actualUrl, err := getAbsRemoteUrl(fs, test.cfg, test.str)

			if test.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, test.expectedUrl, actualUrl)
			assert.Equal(t, test.expectedScheme, actualScheme)
		})
	}
}
