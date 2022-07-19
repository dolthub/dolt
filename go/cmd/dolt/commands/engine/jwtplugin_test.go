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
	"testing"

	"github.com/stretchr/testify/require"
)

var locationUrl = "https://hostedapi.hosteddev.ld-corp.com/hosted_ui_keys.jwks"
var iss = "dolthub.com"
var testUser = "hosted-ui-reader"
var aud = "some_id"
var jwksName = "jwks-testing"

func TestJWTAuth(t *testing.T) {
	jwksConfig := []JwksConfig{
		{
			Name:        jwksName,
			LocationUrl: locationUrl,
			Claims: map[string]string{
				"alg": "RS256",
				"aud": "some_id",
				"iss": iss,
				"sub": testUser,
			},
			FieldsToLog: []string{"id", "on_behalf_of"},
		},
	}
	// jwks, err := getJWKSFromUrl(locationUrl)
	// require.NoError(t, err)
	authed, err := validateJWT(jwksConfig, testUser, fmt.Sprintf("jwks=%s,sub=%s,iss=%s,aud=%s", jwksName, testUser, iss, aud), "eyJhbGciOiJSUzI1NiIsImtpZCI6IjNlMTZkY2NmLTI0YmYtNDQ3Yi04ZDcyLTI5NTAwNDJiNDM1ZiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsic29tZV9pZCJdLCJpYXQiOjE2NTgxOTIwNzcsImlzcyI6ImRvbHRodWIuY29tIiwianRpIjoiM2UxNmRjY2YtMjRiZi00NDdiLThkNzItMjk1MDA0MmI0MzVmIiwib25fYmVoYWxmX29mIjoibXktdXNlcm5hbWUiLCJzdWIiOiJ0ZXN0LXVzZXIifQ.DNEexsNM5GVZfnZ7peaiaOuSL_0wDv7Ooa_7fp4ag1ZbzbXpglLYi2ZP1aJnPBlJ32U9i4gyydMBr5eMrs0A-WvLUMw5ZDTJK2nEOriorVFVVUzD6--r9FURSfHrXpnSzHuYbsKDMTMZ6RuU0jzNrBc_k2fMEUhDyYOlIUmx71YdNIYTQ5MOHqTZ9dR78YBELWKv2HnMvMUm7m5IieoRSnxvQ3Fu9R3q2fEKgW_KPUcxZ9cwA_6XNFkHxIQMueh66_D_VZhZHcfZG6oYa255ejqYwNQwD6Hx2F_pvF96GvqLdl8NUOZra5VEDXA20WmslktKvgdr-1SZKsrd1Na-aA")
	require.NoError(t, err)
	require.True(t, authed)
}

// func generateTestToken(t *testing.T, jwks jose.JSONWebKeySet) string {
// 	numKeys := len(jwks.Keys)
// 	key := jwks.Keys[rand.Intn(numKeys)]

// 	id, err := uuid.NewRandom()
// 	require.NoError(t, err)

// 	now := time.Now()
// 	claims := jwt.Claims{
// 		ID:       id.String(),
// 		Audience: []string{aud},
// 		Issuer:   iss,
// 		Subject:  testUser,
// 		IssuedAt: jwt.NewNumericDate(now),
// 		Expiry:   jwt.NewNumericDate(now.Add(30 * time.Second)),
// 	}
// 	privClaims := struct {
// 		OnBehalfOf string `json:"on_behalf_of"`
// 	}{
// 		"username",
// 	}

// 	sig := jose.SigningKey{Algorithm: jose.RS256, Key: key.PrivKey}
// 	opts := (&jose.SignerOptions{ExtraHeaders: map[jose.HeaderKey]interface{}{
// 		"kid": key.ID,
// 	}}).WithType("JWT")

// 	signer, err := jose.NewSigner(sig, opts)
// 	require.NoError(t, err)

// 	jwtBuilder := jwt.Signed(signer)
// 	jwtBuilder = jwtBuilder.Claims(claims)
// 	jwtBuilder = jwtBuilder.Claims(privClaims)

// 	com, err := jwtBuilder.CompactSerialize()
// 	require.NoError(t, err)
// 	return com
// }
