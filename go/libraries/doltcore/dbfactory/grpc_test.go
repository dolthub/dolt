// Copyright 2026 Dolthub, Inc.
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

package dbfactory

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
)

type captureDialProvider struct{ got grpcendpoint.Config }

var errStop = errors.New("stop")

func (c *captureDialProvider) GetGRPCDialParams(cfg grpcendpoint.Config) (GRPCRemoteConfig, error) {
	c.got = cfg
	return GRPCRemoteConfig{}, errStop
}

func TestRemoteUserFromEnv(t *testing.T) {
	tests := []struct {
		name   string
		env    string
		params map[string]interface{}
		want   string
	}{
		{"no env, no flag", "", map[string]interface{}{}, ""},
		{"env used without flag", "envuser", map[string]interface{}{}, "envuser"},
		{"flag wins over env", "envuser", map[string]interface{}{GRPCUsernameAuthParam: "flaguser"}, "flaguser"},
		{"flag without env", "", map[string]interface{}{GRPCUsernameAuthParam: "flaguser"}, "flaguser"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(dconfig.EnvDoltRemoteUser, tt.env)
			dp := &captureDialProvider{}
			u, _ := url.Parse("http://localhost:50051/db")
			_, err := DoltRemoteFactory{insecure: true}.newChunkStore(context.Background(), nil, u, tt.params, dp)
			require.ErrorIs(t, err, errStop)
			require.Equal(t, tt.want, dp.got.UserIdForOsEnvAuth)
		})
	}
}
