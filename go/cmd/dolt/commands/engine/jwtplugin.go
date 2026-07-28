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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/utils/jwtauth"
)

// authenticateDoltJWTPlugin is used to authenticate plaintext user plugins
type authenticateDoltJWTPlugin struct {
	jwksConfig []servercfg.JwksConfig
}

func NewAuthenticateDoltJWTPlugin(jwksConfig []servercfg.JwksConfig) mysql_db.PlaintextAuthPlugin {
	return &authenticateDoltJWTPlugin{jwksConfig: jwksConfig}
}

func (p *authenticateDoltJWTPlugin) Authenticate(db *mysql_db.MySQLDb, user string, userEntry *mysql_db.User, pass string) (bool, error) {
	return validateJWT(p.jwksConfig, userEntry.Identity, pass, time.Now())
}

// validateJWT authenticates a token against the claims declared in the user's
// identity string. The identity string is a comma-separated set of expected
// claims (jwks, iss, aud, sub); the token authenticates iff it satisfies all
// of them. The connecting username is deliberately not consulted here: if an
// operator wants to bind the token subject to the account name, they set
// sub=<username> in the identity, which is then enforced against the token's
// sub claim like any other claim.
func validateJWT(config []servercfg.JwksConfig, identity, token string, reqTime time.Time) (bool, error) {
	if len(config) == 0 {
		return false, errors.New("ValidateJWT: JWKS server config not found")
	}

	expectedClaimsMap, err := parseUserIdentity(identity)
	if err != nil {
		return false, err
	}

	jwksConfig, err := getMatchingJwksConfig(config, expectedClaimsMap["jwks"])
	if err != nil {
		return false, err
	}

	pr, err := getJWTProvider(expectedClaimsMap, jwksConfig.LocationUrl)
	if err != nil {
		return false, err
	}
	vd, err := jwtauth.NewJWTValidator(pr)
	if err != nil {
		return false, err
	}
	claims, err := vd.ValidateJWT(token, reqTime)
	if err != nil {
		return false, err
	}

	logString := "Authenticating with JWT: "
	for _, field := range jwksConfig.FieldsToLog {
		logString += fmt.Sprintf("%s: %s,", field, getClaimFromKey(claims, field))
	}
	logrus.Info(logString)
	return true, nil
}

func getJWTProvider(expectedClaimsMap map[string]string, url string) (jwtauth.JWTProvider, error) {
	pr := jwtauth.JWTProvider{URL: url}
	for name, claim := range expectedClaimsMap {
		switch name {
		case "iss":
			pr.Issuer = claim
		case "aud":
			pr.Audience = claim
		case "sub":
			pr.Subject = claim
		case "jwks":
			continue
		default:
			return pr, errors.New("ValidateJWT: Unexpected expected claim found in user identity")
		}
	}
	return pr, nil
}

func getClaimFromKey(claims *jwtauth.Claims, field string) string {
	switch field {
	case "id":
		return claims.ID
	case "iss":
		return claims.Issuer
	case "sub":
		return claims.Subject
	case "on_behalf_of":
		return claims.OnBehalfOf
	}
	return ""
}

func getMatchingJwksConfig(config []servercfg.JwksConfig, name string) (*servercfg.JwksConfig, error) {
	for _, item := range config {
		if item.Name == name {
			return &item, nil
		}
	}
	return nil, errors.New("ValidateJWT: Matching JWKS config not found")
}

func parseUserIdentity(identity string) (map[string]string, error) {
	idMap := make(map[string]string)
	items := strings.Split(identity, ",")
	for _, item := range items {
		name, value, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("ValidateJWT: malformed identity %q: expected comma-separated key=value pairs", identity)
		}
		idMap[name] = value
	}
	return idMap, nil
}
