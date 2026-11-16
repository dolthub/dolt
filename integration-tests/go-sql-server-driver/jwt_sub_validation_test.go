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

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"

	"github.com/stretchr/testify/require"
)

// TestJWTSubClaimValidation is a regression test for
// https://github.com/dolthub/dolt/issues/11289.
//
// A user created `IDENTIFIED WITH authentication_dolt_jwt AS '...,sub=X,...'`
// declares a set of expected claims (jwks, iss, aud, sub). A presented token
// authenticates iff it satisfies all of them, including sub. Historically the
// token's own `sub` claim was never enforced against the identity's `sub`, so
// any validly-signed token (correct iss/aud, unexpired) authenticated any
// JWT-backed user of that JWKS.
//
// These subtests assert the intended semantics: the identity string is the
// source of truth for the claims the token must satisfy, and the token's `sub`
// is enforced against the identity's `sub` independently of the connecting
// username.
func TestJWTSubClaimValidation(t *testing.T) {
	t.Parallel()

	const (
		issuer   = "dolthub.com"
		audience = "my_resource"
	)

	// Serve the public JWKS that testdata/test_jwks_private.json signs against,
	// so the running sql-server can fetch it and verify token signatures.
	jwksPort := GlobalPorts.GetPort(t)
	t.Cleanup(func() { GlobalPorts.Return(jwksPort) })
	absJWKS, err := filepath.Abs("./testdata/test_jwks.json")
	require.NoError(t, err)
	runJWKSServer(t, absJWKS, jwksPort)

	server := startJWTAuthServer(t, jwksPort, issuer, audience)

	// As root, create JWT-backed users. `alice` and `bob` bind their identity
	// sub to their own account name; `svc` deliberately declares a sub
	// (`alice`) that differs from its username, to exercise that the token is
	// checked against the identity's declared claims, not the username.
	rootConn := driver.Connection{
		User:         "root",
		DriverParams: map[string]string{"allowCleartextPasswords": "true"},
	}
	rootDB, err := server.DB(rootConn)
	require.NoError(t, err)
	defer rootDB.Close()

	rootSQL, err := rootDB.Conn(context.Background())
	require.NoError(t, err)
	defer rootSQL.Close()

	// user -> the sub claim declared in its identity string.
	identitySub := map[string]string{
		"alice": "alice",
		"bob":   "bob",
		"svc":   "alice",
	}
	for user, sub := range identitySub {
		_, err = rootSQL.ExecContext(context.Background(), fmt.Sprintf(
			"CREATE USER '%s'@'%%' IDENTIFIED WITH authentication_dolt_jwt "+
				"AS 'jwks=jwksname,sub=%s,iss=%s,aud=%s'", user, sub, issuer, audience))
		require.NoError(t, err)
		_, err = rootSQL.ExecContext(context.Background(), fmt.Sprintf(
			"GRANT ALL ON *.* TO '%s'@'%%' WITH GRANT OPTION", user))
		require.NoError(t, err)
	}

	connectWithToken := func(user, tokenSub string) error {
		token := createJWT(t, issuer, audience, tokenSub)
		conn := driver.Connection{
			User:         user,
			Pass:         token,
			DriverParams: map[string]string{"allowCleartextPasswords": "true"},
		}
		db, err := server.DB(conn)
		if db != nil {
			db.Close()
		}
		return err
	}

	// Positive control: a token whose sub matches the identity's declared sub
	// authenticates. This also proves the JWKS/issuer/audience/TLS plumbing is
	// correct, so the rejections below are specifically about the sub claim.
	t.Run("token matching identity sub authenticates", func(t *testing.T) {
		require.NoError(t, connectWithToken("bob", "bob"))
	})

	// The issue #11289 scenario: a token minted for `alice` must not
	// authenticate as `bob`, whose identity declares sub=bob.
	t.Run("token not matching identity sub is rejected", func(t *testing.T) {
		err := connectWithToken("bob", "alice")
		require.Error(t, err, "a token whose sub is 'alice' must not authenticate 'bob' (identity sub=bob)")
	})

	// The identity's sub is the source of truth, not the username. `svc`'s
	// identity declares sub=alice, so an alice token authenticates it and an
	// svc token (which matches only the username) does not.
	t.Run("identity sub is enforced against the token, not the username", func(t *testing.T) {
		require.NoError(t, connectWithToken("svc", "alice"),
			"svc's identity declares sub=alice, so an alice token must authenticate it")
		require.Error(t, connectWithToken("svc", "svc"),
			"a token whose sub is 'svc' must not authenticate 'svc' (identity sub=alice)")
	})
}

// startJWTAuthServer starts a dolt sql-server configured with TLS,
// require_secure_transport, and a single JWKS entry backed by the JWKS server
// running on jwksPort. It returns the running server; the connecting DBName is
// set to the repo's database.
func startJWTAuthServer(t *testing.T, jwksPort int, issuer, audience string) *driver.SqlServer {
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	serverPort := ports.GetOrAllocatePort("server")

	gendir := os.Getenv("TESTGENDIR")
	require.NotEmpty(t, gendir, "TESTGENDIR must be set by TestMain")
	tlsKey := filepath.Join(gendir, "rsa_key.pem")
	tlsCert := filepath.Join(gendir, "rsa_chain.pem")

	config := fmt.Sprintf(`listener:
  tls_key: %s
  tls_cert: %s
  require_secure_transport: true
  port: %d
jwks:
- name: jwksname
  location_url: http://127.0.0.1:%d/jwks.json
  claims:
    alg: RS256
    aud: %s
    iss: %s
  fields_to_log: [sub]
`, tlsKey, tlsCert, serverPort, jwksPort, audience, issuer)

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo("jwt_sub_validation")
	require.NoError(t, err)

	f, err := os.CreateTemp("", "jwt-sub-config-*.yaml")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(f.Name())
	})
	_, err = f.WriteString(config)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	srvSettings := &driver.Server{
		Args:        []string{"--config", f.Name()},
		DynamicPort: "server",
	}

	t.Log("Starting server with config:\n" + config)
	server := MakeServer(t, repo, srvSettings, &ports)
	require.NotNil(t, server)
	server.DBName = "jwt_sub_validation"
	return server
}
