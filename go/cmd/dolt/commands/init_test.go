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

package commands

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

type initTest struct {
	Name          string
	Args          []string
	GlobalConfig  map[string]string
	ExpectSuccess bool
}

func TestInit(t *testing.T) {
	tests := []initTest{
		{
			"Command Line name and email",
			[]string{"-name", "Bill Billerson", "-email", "bigbillieb@fake.horse"},
			map[string]string{},
			true,
		},
		{
			"Global config name and email",
			[]string{},
			map[string]string{
				config.UserNameKey:  "Bill Billerson",
				config.UserEmailKey: "bigbillieb@fake.horse",
			},
			true,
		},
		{
			"No Name",
			[]string{"-email", "bigbillieb@fake.horse"},
			map[string]string{},
			false,
		},
		{
			"No Email",
			[]string{"-name", "Bill Billerson"},
			map[string]string{},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := createUninitializedEnv()
			gCfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
			gCfg.SetStrings(test.GlobalConfig)
			apr := argparser.ArgParseResults{}
			latebind := func(ctx context.Context) (cli.Queryist, *sql.Context, func(), error) { return nil, nil, func() {}, nil }
			cliCtx, _ := cli.NewCliContext(&apr, dEnv.Config, dEnv.FS, latebind)

			result := InitCmd{}.Exec(ctx, "dolt init", test.Args, dEnv, cliCtx)
			defer dEnv.DoltDB(ctx).Close()

			require.Equalf(t, test.ExpectSuccess, result == 0, "- Expected success: %t; result: %t;", test.ExpectSuccess, result == 0)

			if test.ExpectSuccess {
				require.True(t, dEnv.HasDoltDir(), "- .dolt dir should exist after initialization")
				testLocalConfigValue(t, dEnv, test, usernameParamName, config.UserNameKey)
				testLocalConfigValue(t, dEnv, test, emailParamName, config.UserEmailKey)
			} else {
				require.False(t, dEnv.HasDoltDir(),
					"- dolt directory shouldn't exist after failure to initialize")
			}
		})
	}
}

func TestInitTwice(t *testing.T) {
	ctx := context.Background()
	dEnv := createUninitializedEnv()
	result := InitCmd{}.Exec(ctx, "dolt init", []string{"-name", "Bill Billerson", "-email", "bigbillieb@fake.horse"}, dEnv, nil)
	require.True(t, result == 0, "First init should succeed")
	defer dEnv.DoltDB(ctx).Close()

	result = InitCmd{}.Exec(ctx, "dolt init", []string{"-name", "Bill Billerson", "-email", "bigbillieb@fake.horse"}, dEnv, nil)
	require.True(t, result != 0, "Second init should fail")
}

// testLocalConfigValue tests that local config data is set correctly when the specified argument
// is present in the command line args, and is not set when the argument is not present.
func testLocalConfigValue(t *testing.T, dEnv *env.DoltEnv, test initTest, argKey, envKey string) {
	localConfig, ok := dEnv.Config.GetConfig(env.LocalConfig)
	require.True(t, ok, "- Unable to load local configuration")

	found := false
	expectedValue := ""
	for i := 0; i <= len(test.Args)-2; i = i + 2 {
		if test.Args[i] == "-"+argKey {
			found = true
			expectedValue = test.Args[i+1]
		}
	}

	actualValue, err := localConfig.GetString(envKey)
	if found {
		require.NoErrorf(t, err, "- Expected '%s', but not found in local config; error: %v",
			expectedValue, err)
		require.Equalf(t, expectedValue, actualValue, "- Expected '%s' in local config, but found '%s'",
			expectedValue, actualValue)
	} else {
		require.Errorf(t, err, "- Expected nothing in local config, but found '%s'", actualValue)
	}
}
