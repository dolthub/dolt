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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

type testConn struct {
	grpc.ClientConnInterface
}

func TestOpenStandbyDBChecksTheStandby(t *testing.T) {
	ctx := context.Background()
	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, doltdb.InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	defer ddb.Close()

	conn := &testConn{}
	c := &Controller{replicationClients: []*replicationServiceClient{{remote: "other"}, {remote: "standby", conn: conn}}}
	errToy := errors.New("toy")
	type call struct {
		conn   grpc.ClientConnInterface
		remote string
	}
	var calls []call
	checkErr := errToy
	prev := standbyCheck
	t.Cleanup(func() { standbyCheck = prev })
	standbyCheck = func(_ context.Context, conn grpc.ClientConnInterface, remote string, got *doltdb.DoltDB) error {
		assert.Same(t, ddb, got)
		calls = append(calls, call{conn, remote})
		return checkErr
	}

	dir := t.TempDir()
	remote := env.NewRemote("standby", earl.FileUrlFromPath(dir, '/'), nil)
	_, err = c.openStandbyDB(ctx, "standby", remote, types.Format_DOLT, ddb, nil)
	assert.ErrorIs(t, err, errToy)
	_, err = c.openStandbyDB(ctx, "missing", remote, types.Format_DOLT, ddb, nil)
	assert.ErrorIs(t, err, errToy)
	assert.Equal(t, []call{{conn, "standby"}, {nil, "missing"}}, calls)

	checkErr = nil
	standby, err := c.openStandbyDB(ctx, "standby", remote, types.Format_DOLT, ddb, nil)
	require.NoError(t, err)
	require.NoError(t, standby.Close())
}

func TestRegisterGrpcServicesAddsServices(t *testing.T) {
	prev := grpcServices
	t.Cleanup(func() { grpcServices = prev })
	grpcServices = []func(*grpc.Server){func(srv *grpc.Server) {
		healthpb.RegisterHealthServer(srv, health.NewServer())
	}}

	srv := grpc.NewServer()
	c := &Controller{lgr: logrus.New()}
	c.RegisterGrpcServices(func(context.Context) (*sql.Context, error) { return nil, nil }, srv)
	info := srv.GetServiceInfo()
	assert.Contains(t, info, healthpb.Health_ServiceDesc.ServiceName)
	assert.Contains(t, info, "dolt.services.replicationapi.v1alpha1.ReplicationService")
}

func TestGrpcDialProviderUnwraps(t *testing.T) {
	orig := env.GRPCDialProvider{}
	d, ok := dbfactory.DialerAs[env.GRPCDialProvider](grpcDialProvider{orig: orig})
	assert.True(t, ok)
	assert.Equal(t, orig, d)
}
