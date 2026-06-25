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

package env

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

func TestCredentialHelperUsesGlobalConfig(t *testing.T) {
	dEnv, _ := createTestEnv(true, true)
	global, _ := dEnv.Config.GetConfig(GlobalConfig)
	local, _ := dEnv.Config.GetConfig(LocalConfig)

	require.NoError(t, global.SetStrings(map[string]string{
		config.RemotesApiCredentialHelper: "global-helper",
	}))
	require.NoError(t, local.SetStrings(map[string]string{
		config.RemotesApiCredentialHelper: "local-helper",
	}))

	executable, ok, err := NewGRPCDialProviderFromDoltEnv(dEnv).globalCredentialHelper()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "global-helper", executable)
}

func TestCredentialHelperIgnoresLocalConfig(t *testing.T) {
	dEnv, _ := createTestEnv(true, true)
	local, _ := dEnv.Config.GetConfig(LocalConfig)
	require.NoError(t, local.SetStrings(map[string]string{
		config.RemotesApiCredentialHelper: "local-helper",
	}))

	_, ok, err := NewGRPCDialProviderFromDoltEnv(dEnv).globalCredentialHelper()
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCredentialHelperIsAppliedByGRPCDialProvider(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper uses a POSIX shell")
	}

	executable := filepath.Join(t.TempDir(), "credential-helper")
	err := os.WriteFile(executable, []byte(`#!/bin/sh
request=$(cat)
if [ "$1" != "get" ] || [ "$request" != '{"uri":"http://bufnet:80"}' ]; then
  exit 1
fi
printf '%s' '{"headers":{"Proxy-Authorization":["Bearer proxy-token"]},"expires":"2099-01-01T00:00:00Z"}'
`), 0700)
	require.NoError(t, err)

	dEnv, _ := createTestEnv(true, true)
	global, _ := dEnv.Config.GetConfig(GlobalConfig)
	require.NoError(t, global.SetStrings(map[string]string{
		config.RemotesApiCredentialHelper: executable,
	}))
	dEnv.UserPassConfig = &creds.DoltCredsForPass{
		Username: "user",
		Password: "password",
	}

	dialConfig, err := NewGRPCDialProviderFromDoltEnv(dEnv).GetGRPCDialParams(grpcendpoint.Config{
		Endpoint:     "bufnet:80",
		Insecure:     true,
		WithEnvCreds: true,
	})
	require.NoError(t, err)

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		assert.Equal(t, []string{"Basic dXNlcjpwYXNzd29yZA=="}, md.Get("authorization"))
		assert.Equal(t, []string{"Bearer proxy-token"}, md.Get("proxy-authorization"))
		return handler(ctx, req)
	}))
	healthpb.RegisterHealthServer(server, health.NewServer())

	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	dialOptions := append(dialConfig.DialOptions, grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))
	conn, err := grpc.DialContext(context.Background(), dialConfig.Endpoint, dialOptions...)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	_, err = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
}
