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
	"testing"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/stretchr/testify/require"
)

func TestLoginUpdateConfig(t *testing.T) {
	for _, local := range []bool{false, true} {
		scope := "global"
		if local {
			scope = "local"
		}
		t.Run(scope, func(t *testing.T) {
			for _, tt := range []struct {
				name, displayName, email, wantName, wantEmail string
			}{
				{"nonempty identity", "New Name", "new@example.com", "New Name", "new@example.com"},
				{"empty display name", "", "new@example.com", "Existing Name", "new@example.com"},
				{"empty email", "New Name", "", "New Name", "existing@example.com"},
				{"empty identity", "", "", "Existing Name", "existing@example.com"},
			} {
				t.Run(tt.name, func(t *testing.T) {
					global := config.NewMapConfig(map[string]string{config.UserNameKey: "Existing Name", config.UserEmailKey: "existing@example.com"})
					var localCfg config.ReadWriteConfig
					target := config.ReadWriteConfig(global)
					if local {
						localCfg = config.NewMapConfig(map[string]string{config.UserNameKey: "Existing Name", config.UserEmailKey: "existing@example.com"})
						target = localCfg
					}
					dEnv := &env.DoltEnv{Config: env.NewTestDoltCliConfig(global, localCfg, nil)}
					dc := creds.DoltCreds{}
					updateConfig(dEnv, &remotesapi.WhoAmIResponse{DisplayName: tt.displayName, EmailAddress: tt.email}, dc)
					require.Equal(t, tt.wantName, target.GetStringOrDefault(config.UserNameKey, ""))
					require.Equal(t, tt.wantEmail, target.GetStringOrDefault(config.UserEmailKey, ""))
					require.Equal(t, dc.KeyIDBase32Str(), global.GetStringOrDefault(config.UserCreds, "missing"))
					if local {
						require.Equal(t, "Existing Name", global.GetStringOrDefault(config.UserNameKey, ""))
						require.Equal(t, "existing@example.com", global.GetStringOrDefault(config.UserEmailKey, ""))
					}
				})
			}
		})
	}
}
