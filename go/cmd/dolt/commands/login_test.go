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

	"github.com/stretchr/testify/require"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

func TestLoginUpdateConfig(t *testing.T) {
	t.Run("global", func(t *testing.T) {
		// Without a local config, login updates the global identity with nonempty response fields.
		for _, tt := range []struct {
			name, displayName, email, wantName, wantEmail string
		}{
			{"nonempty identity", "New Name", "new@example.com", "New Name", "new@example.com"},
			{"empty display name", "", "new@example.com", "Existing Global Name", "new@example.com"},
			{"empty email", "New Name", "", "New Name", "global@example.com"},
			{"empty identity", "", "", "Existing Global Name", "global@example.com"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				global := config.NewMapConfig(map[string]string{config.UserNameKey: "Existing Global Name", config.UserEmailKey: "global@example.com"})
				dEnv := &env.DoltEnv{Config: env.NewTestDoltCliConfig(global, nil, nil)}
				dc := creds.DoltCreds{}

				updateConfig(dEnv, &remotesapi.WhoAmIResponse{DisplayName: tt.displayName, EmailAddress: tt.email}, dc)

				require.Equal(t, tt.wantName, global.GetStringOrDefault(config.UserNameKey, ""))
				require.Equal(t, tt.wantEmail, global.GetStringOrDefault(config.UserEmailKey, ""))
				require.Equal(t, dc.KeyIDBase32Str(), global.GetStringOrDefault(config.UserCreds, "missing"))
			})
		}
	})

	t.Run("local", func(t *testing.T) {
		// With a local config, login updates the local identity and preserves the global identity.
		for _, tt := range []struct {
			name, displayName, email, wantName, wantEmail string
		}{
			{"nonempty identity", "New Name", "new@example.com", "New Name", "new@example.com"},
			{"empty display name", "", "new@example.com", "Existing Local Name", "new@example.com"},
			{"empty email", "New Name", "", "New Name", "local@example.com"},
			{"empty identity", "", "", "Existing Local Name", "local@example.com"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				global := config.NewMapConfig(map[string]string{config.UserNameKey: "Existing Global Name", config.UserEmailKey: "global@example.com"})
				local := config.NewMapConfig(map[string]string{config.UserNameKey: "Existing Local Name", config.UserEmailKey: "local@example.com"})
				dEnv := &env.DoltEnv{Config: env.NewTestDoltCliConfig(global, local, nil)}
				dc := creds.DoltCreds{}

				updateConfig(dEnv, &remotesapi.WhoAmIResponse{DisplayName: tt.displayName, EmailAddress: tt.email}, dc)

				require.Equal(t, tt.wantName, local.GetStringOrDefault(config.UserNameKey, ""))
				require.Equal(t, tt.wantEmail, local.GetStringOrDefault(config.UserEmailKey, ""))
				require.Equal(t, "Existing Global Name", global.GetStringOrDefault(config.UserNameKey, ""))
				require.Equal(t, "global@example.com", global.GetStringOrDefault(config.UserEmailKey, ""))
				require.Equal(t, dc.KeyIDBase32Str(), global.GetStringOrDefault(config.UserCreds, "missing"))
			})
		}
	})
}
