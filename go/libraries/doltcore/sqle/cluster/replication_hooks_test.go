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

package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	replicationapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/replicationapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/types"
)

type testClusterConfig struct{}

func (testClusterConfig) StandbyRemotes() []servercfg.ClusterStandbyRemoteConfig {
	return []servercfg.ClusterStandbyRemoteConfig{testStandby{}}
}
func (testClusterConfig) BootstrapRole() string { return "primary" }
func (testClusterConfig) BootstrapEpoch() int   { return 1 }
func (testClusterConfig) RemotesAPIConfig() servercfg.ClusterRemotesAPIConfig {
	return testRemotesAPI{}
}

type testStandby struct{}

func (testStandby) Name() string              { return "standby" }
func (testStandby) RemoteURLTemplate() string { return "http://localhost:50051/{database}" }

type testRemotesAPI struct{}

func (testRemotesAPI) Address() string                { return "" }
func (testRemotesAPI) Port() int                      { return 50051 }
func (testRemotesAPI) TLSKey() string                 { return "" }
func (testRemotesAPI) TLSCert() string                { return "" }
func (testRemotesAPI) TLSCA() string                  { return "" }
func (testRemotesAPI) ServerNameURLMatches() []string { return nil }
func (testRemotesAPI) ServerNameDNSMatches() []string { return nil }

func TestReplicationHooksAreReached(t *testing.T) {
	savedRegister, savedClient := registerGrpcServices, newReplicationServiceClient
	t.Cleanup(func() { registerGrpcServices, newReplicationServiceClient = savedRegister, savedClient })
	var calls []string
	registerGrpcServices = func(*grpc.Server) { calls = append(calls, "registerGrpcServices") }
	newReplicationServiceClient = func(cc grpc.ClientConnInterface) replicationapi.ReplicationServiceClient {
		calls = append(calls, "newReplicationServiceClient")
		return savedClient(cc)
	}

	c, err := NewController(logrus.New(), testClusterConfig{}, config.NewMapConfig(make(map[string]string)))
	require.NoError(t, err)
	require.Equal(t, []string{"newReplicationServiceClient"}, calls)
	srv := grpc.NewServer()
	defer srv.Stop()
	c.RegisterGrpcServices(nil, srv)
	require.Equal(t, []string{"newReplicationServiceClient", "registerGrpcServices"}, calls)
}

func TestOpenStandbyDBIsReached(t *testing.T) {
	ctx := context.Background()
	refused := errors.New("refused")
	saved := openStandbyDB
	t.Cleanup(func() { openStandbyDB = saved })
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()
	var opened []string
	openStandbyDB = func(_ *Controller, ctx context.Context, remoteName string, _ env.Remote, _ *types.NomsBinFormat, ddb *doltdb.DoltDB, _ dbfactory.GRPCDialProvider) (*doltdb.DoltDB, error) {
		require.Same(t, dEnv.DoltDB(ctx), ddb)
		opened = append(opened, remoteName)
		return nil, refused
	}

	c, err := NewController(logrus.New(), testClusterConfig{}, config.NewMapConfig(make(map[string]string)))
	require.NoError(t, err)
	hooks, err := c.applyCommitHooks(ctx, "db", dEnv)
	require.NoError(t, err)
	require.Len(t, hooks, 1)
	_, err = hooks[0].destDBF(ctx)
	require.ErrorIs(t, err, refused)
	require.Equal(t, []string{"standby"}, opened)
}
