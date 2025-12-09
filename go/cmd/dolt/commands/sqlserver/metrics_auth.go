// Copyright 2025 Dolthub, Inc.
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

package sqlserver

import (
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/utils/jwtauth"
)

func validateJWT(jwksConfig *servercfg.JwksConfig, token string, reqTime time.Time) (bool, *jwtauth.Claims, error) {
	if jwksConfig == nil {
		return false, nil, errors.New("ValidateJWT: JWKS metrics config not found")
	}

	pr, err := getJWTProvider(jwksConfig.Claims, jwksConfig.LocationUrl)
	if err != nil {
		return false, nil, fmt.Errorf("unable to get JWT provider: %w", err)
	}

	vd, err := jwtauth.NewJWTValidator(pr)
	if err != nil {
		return false, nil, fmt.Errorf("unable to get JWT validator: %w", err)
	}

	privClaims, err := vd.ValidateJWT(token, reqTime)
	if err != nil {
		return false, nil, fmt.Errorf("unable to validate JWT token: %w", err)
	}

	logString := "Metrics Auth with JWT: "
	for _, field := range jwksConfig.FieldsToLog {
		logString += fmt.Sprintf("%s: %s,", field, getClaimFromKey(privClaims, field))
	}

	logrus.Info(logString)
	return true, privClaims, nil
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
		default:
			return pr, errors.New("ValidateJWT: Unexpected expected claim found in user identity")
		}
	}
	return pr, nil
}
