// Copyright 2022 Dolthub, Inc.
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

package engine

import (
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var jwksName = "jwksname"
var sub = "test_user"
var iss = "dolthub.com"
var aud = "my_resource"
var onBehalfOf = "my_user"
var jwt = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImUwNjA2Y2QwLTkwNWQtNGFiYS05MjBjLTZlNTE0YTFjYmIyNiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsibXlfcmVzb3VyY2UiXSwiZXhwIjoxNjU4Mjc1OTAzLCJpYXQiOjE2NTgyNzU4NzMsImlzcyI6ImRvbHRodWIuY29tIiwianRpIjoiN2ViZTg3YmMtOTkzMi00ZTljLTk5N2EtNjQzMDk0NTBkMWVjIiwib25fYmVoYWxmX29mIjoibXlfdXNlciIsInN1YiI6InRlc3RfdXNlciJ9.u2cUGUkQ2hk4AaxtNQB-6Jcdf5LtehFA7XX2FG8LGgTf6KfwE3cuuGaBIU8Jz9ktD9g8TjAbfAfbrNaFNYnKG6SnDUHp0t7VbfLdgfNDQqSyH0nOK2UF8ffxqa46PRxeMwTSJv8prE07rcmiZNL9Ie4vSGYLncJfMzo_RdE-A-PH7z-ZyZ_TxOMhkgMFq2Af5Px3zFuAKq-Y-PrQNopSuzjPJc0DQ93Q7EcIHfU6Fx6gOVTkzHxnOFcg3Nj-4HhqBSvBa_BdMYEzHJKx3F_9rrCCPqEGUFnxXAqFFmnZUQuQKpN2yW_zhviCVqrvbP7vOCIXmxi8YXLiGiV-4KlxHA"

func TestJWTAuth(t *testing.T) {
	jwksConfig := []servercfg.JwksConfig{
		{
			Name:        jwksName,
			LocationUrl: "file:///testdata/test_jwks.json",
			Claims: map[string]string{
				"alg": "RS256",
				"aud": aud,
				"iss": iss,
				"sub": sub,
			},
			FieldsToLog: []string{"id", "on_behalf_of"},
		},
	}

	// Success
	tokenCreated := time.Date(2022, 07, 20, 0, 12, 0, 0, time.UTC) // Update time if creating new token
	authed, err := validateJWT(jwksConfig, sub, fmt.Sprintf("jwks=%s,sub=%s,iss=%s,aud=%s", jwksName, sub, iss, aud), jwt, tokenCreated)
	require.NoError(t, err)
	require.True(t, authed)

	// Token expired
	now := time.Now()
	authed, err = validateJWT(jwksConfig, sub, fmt.Sprintf("jwks=%s,sub=%s,iss=%s,aud=%s", jwksName, sub, iss, aud), jwt, now)
	require.Error(t, err)
	require.False(t, authed)

	// Expected sub does not match
	authed, err = validateJWT(jwksConfig, sub, fmt.Sprintf("jwks=%s,sub=%s,iss=%s,aud=%s", jwksName, "wrong-sub", iss, aud), jwt, tokenCreated)
	require.Error(t, err)
	require.False(t, authed)

	// Jwks config doesn't exist
	authed, err = validateJWT([]servercfg.JwksConfig{}, sub, fmt.Sprintf("jwks=%s,sub=%s,iss=%s,aud=%s", jwksName, sub, iss, aud), jwt, tokenCreated)
	require.Error(t, err)
	require.False(t, authed)

	// No token
	authed, err = validateJWT(jwksConfig, sub, fmt.Sprintf("jwks=%s,sub=%s,iss=%s,aud=%s", jwksName, sub, iss, aud), "", tokenCreated)
	require.Error(t, err)
	require.False(t, authed)

	// Unknown claim in identity string
	authed, err = validateJWT(jwksConfig, sub, fmt.Sprintf("jwks=%s,sub=%s,iss=%s,unknown=%s", jwksName, sub, iss, aud), "", tokenCreated)
	require.Error(t, err)
	require.False(t, authed)
}
