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
	"encoding/base64"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestRemotesAPIAuthentication asserts that the gRPC remotesapi endpoint of a
// sql-server validates credentials against the account for the address the
// request came from, rather than against a fixed localhost account.
//
// The account an unauthenticated request resolves to is the crux. A request
// with no authorization metadata is credentialed as root with an empty
// password, which is what the default first-run root@localhost account has when
// DOLT_ROOT_PASSWORD is unset. Validating that against the requesting address
// keeps it to clients the root account is defined for; validating it against
// localhost accepts it from anywhere.
//
// The dolt remote client always sends an authorization header when it has
// credentials to send, so these cases are driven by a raw gRPC client against
// ChunkStoreService/Root, a CLONE_ADMIN method.
func TestRemotesAPIAuthentication(t *testing.T) {
	t.Parallel()

	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	_, err = rs.MakeRepo("repo1")
	require.NoError(t, err)

	// DOLT_ROOT_PASSWORD is deliberately unset, so the first-run initializer
	// creates root@localhost with an empty password. That is what a default
	// install looks like.
	srvSettings := &driver.Server{
		Args: []string{
			"-P", `{{get_port "server_port"}}`,
			"--remotesapi-port", `{{get_port "remotesapi_port"}}`,
		},
		DynamicPort: "server_port",
	}
	server := MakeServer(t, rs, srvSettings, &ports)
	require.NotNil(t, server)
	server.DBName = "repo1"

	remotesapiPort, ok := ports.GetPort("remotesapi_port")
	require.True(t, ok)

	ctx := t.Context()

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	// Give the served repo a root to return.
	_, err = conn.ExecContext(ctx, "CREATE TABLE vals (id int primary key)")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "INSERT INTO vals VALUES (0),(1),(2)")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', 'insert some data')")
	require.NoError(t, err)

	// An administrator who wants remote clones sets up accounts reachable from
	// off-box. Both of these have a password; neither should be usable without
	// presenting it.
	_, err = conn.ExecContext(ctx, "CREATE USER cloner@'%' IDENTIFIED BY 'clonerpassword'")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "GRANT CLONE_ADMIN ON *.* TO cloner@'%'")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CREATE USER root@'%' IDENTIFIED BY 'rootpassword'")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "GRANT CLONE_ADMIN ON *.* TO root@'%'")
	require.NoError(t, err)

	loopback := fmt.Sprintf("127.0.0.1:%d", remotesapiPort)
	// The remotesapi listener binds every interface, so this dials the same
	// server at an address which is not localhost. That is what a client on
	// another machine looks like to the server.
	remote := fmt.Sprintf("%s:%d", nonLoopbackAddr(t), remotesapiPort)

	// A request with no credentials is deliberately still credentialed as root
	// with an empty password, which the default first-run root@localhost
	// account has. This is what lets a co-located client, such as a
	// dolt_clone() of this server's own remotesapi, work without configuring
	// an account. It is only reachable from an address root is defined for,
	// which the two cases after it pin down.
	t.Run("NoAuthorizationHeaderFromLoopbackIsRootAtLocalhost", func(t *testing.T) {
		_, err := remotesapiRoot(t, loopback, "")
		require.NoError(t, err)
	})

	t.Run("NoAuthorizationHeaderFromRemote", func(t *testing.T) {
		_, err := remotesapiRoot(t, remote, "")
		requireUnauthenticated(t, err)
	})

	t.Run("EmptyRootPasswordFromRemote", func(t *testing.T) {
		// root@localhost with an empty password is the default first-run
		// account. It is scoped to localhost and must not authenticate a
		// request arriving from another address, whatever root@'%' is
		// configured to allow.
		_, err := remotesapiRoot(t, remote, basicAuth("root", ""))
		requireUnauthenticated(t, err)
	})

	t.Run("WrongPasswordFromLoopback", func(t *testing.T) {
		_, err := remotesapiRoot(t, loopback, basicAuth("cloner", "notthepassword"))
		requireUnauthenticated(t, err)
	})

	// Positive controls: accounts granted CLONE_ADMIN for any host still work
	// when they present their password.
	t.Run("GrantedUserFromLoopback", func(t *testing.T) {
		_, err := remotesapiRoot(t, loopback, basicAuth("cloner", "clonerpassword"))
		require.NoError(t, err)
	})

	t.Run("GrantedUserFromRemote", func(t *testing.T) {
		_, err := remotesapiRoot(t, remote, basicAuth("cloner", "clonerpassword"))
		require.NoError(t, err)
	})

	// The mirror image of EmptyRootPasswordFromRemote, and the same root cause:
	// because every request is validated against the localhost account, the
	// root@'%' account an administrator configured for remote access cannot be
	// used, while the local empty-password account can.
	t.Run("RootWithPasswordFromRemote", func(t *testing.T) {
		_, err := remotesapiRoot(t, remote, basicAuth("root", "rootpassword"))
		require.NoError(t, err)
	})
}

// remotesapiRoot calls ChunkStoreService/Root against |addr| for repo1. If
// |authorization| is non-empty it is sent as the authorization metadata;
// otherwise the request carries no authorization metadata at all.
func remotesapiRoot(t *testing.T, addr, authorization string) (*remotesapi.RootResponse, error) {
	t.Helper()

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer cc.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if authorization != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", authorization)
	}

	client := remotesapi.NewChunkStoreServiceClient(cc)
	return client.Root(ctx, &remotesapi.RootRequest{RepoPath: "repo1"})
}

// basicAuth renders credentials the way ExtractBasicAuthCreds parses them.
func basicAuth(username, password string) string {
	return "Basic " + base64.URLEncoding.EncodeToString([]byte(username+":"+password))
}

// requireUnauthenticated asserts the request was turned away at authentication.
// PermissionDenied is not accepted here: it means the server accepted the
// credentials and only the privilege lookup, which does use the peer address,
// happened to turn the request away. That is the behavior which hides this bug
// whenever the account in question has no grants for the peer's host.
func requireUnauthenticated(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err, "request was served without valid credentials")
	require.Equalf(t, codes.Unauthenticated, status.Code(err),
		"expected the credentials to be rejected, got: %v", err)
}

// nonLoopbackAddr returns an IPv4 address of this host which is not a loopback
// address. Dialing the server there makes the server see a peer address that is
// not localhost.
func nonLoopbackAddr(t *testing.T) string {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	require.NoError(t, err)
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		if ip4 := ipnet.IP.To4(); ip4 != nil {
			return ip4.String()
		}
	}
	require.FailNow(t, "no non-loopback IPv4 address on this host")
	return ""
}
