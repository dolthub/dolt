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

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

func TestInit(t *testing.T) {
	tests := []struct {
		Name          string
		Args          []string
		GlobalConfig  map[string]string
		ExpectSuccess bool
	}{
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
				env.UserNameKey:  "Bill Billerson",
				env.UserEmailKey: "bigbillieb@fake.horse",
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
			dEnv := createUninitializedEnv()
			gCfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
			gCfg.SetStrings(test.GlobalConfig)

			result := InitCmd{}.Exec(context.Background(), "dolt init", test.Args, dEnv)

			if (result == 0) != test.ExpectSuccess {
				t.Error(test.Name, "- Expected success:", test.ExpectSuccess, "result:", result == 0)
			} else if test.ExpectSuccess {
				// succceeded as expected
				if !dEnv.HasDoltDir() {
					t.Error(test.Name, "- .dolt dir should exist after initialization")
				}
			} else {
				// failed as expected
				if dEnv.HasDoltDir() {
					t.Error(test.Name, "- dolt directory shouldn't exist after failure to initialize")
				}
			}
		})
	}
}

func TestInitTwice(t *testing.T) {
	dEnv := createUninitializedEnv()
	result := InitCmd{}.Exec(context.Background(), "dolt init", []string{"-name", "Bill Billerson", "-email", "bigbillieb@fake.horse"}, dEnv)

	if result != 0 {
		t.Error("First init should succeed")
	}

	result = InitCmd{}.Exec(context.Background(), "dolt init", []string{"-name", "Bill Billerson", "-email", "bigbillieb@fake.horse"}, dEnv)

	if result == 0 {
		t.Error("Second init should fail")
	}
}
