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

package cluster

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type server struct {
	md metadata.MD
}

func (s *server) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	s.md, _ = metadata.FromIncomingContext(ctx)
	return nil, status.Errorf(codes.Unimplemented, "method Check not implemented")
}

func (s *server) Watch(req *grpc_health_v1.HealthCheckRequest, ss grpc_health_v1.Health_WatchServer) error {
	s.md, _ = metadata.FromIncomingContext(ss.Context())
	return status.Errorf(codes.Unimplemented, "method Watch not implemented")
}

func noopSetRole(string, int) {
}

var lgr = logrus.StandardLogger().WithFields(logrus.Fields{})

func withClient(t *testing.T, cb func(*testing.T, grpc_health_v1.HealthClient), serveropts []grpc.ServerOption, dialopts []grpc.DialOption) *server {
	addr, err := net.ResolveUnixAddr("unix", "test_grpc.socket")
	require.NoError(t, err)
	lis, err := net.ListenUnix("unix", addr)
	require.NoError(t, err)

	var wg sync.WaitGroup
	var srvErr error
	wg.Add(1)

	srv := grpc.NewServer(serveropts...)
	hs := new(server)
	grpc_health_v1.RegisterHealthServer(srv, hs)
	defer func() {
		if srv != nil {
			srv.GracefulStop()
			wg.Wait()
		}
	}()

	go func() {
		defer wg.Done()
		srvErr = srv.Serve(lis)
	}()

	cc, err := grpc.Dial("unix:test_grpc.socket", append([]grpc.DialOption{grpc.WithInsecure()}, dialopts...)...)
	require.NoError(t, err)
	client := grpc_health_v1.NewHealthClient(cc)

	cb(t, client)

	srv.GracefulStop()
	wg.Wait()
	srv = nil
	require.NoError(t, srvErr)

	return hs
}

func TestServerInterceptorAddsUnaryResponseHeaders(t *testing.T) {
	var si serverinterceptor
	si.setRole(RoleStandby, 10)
	si.roleSetter = noopSetRole
	si.lgr = lgr
	withClient(t, func(t *testing.T, client grpc_health_v1.HealthClient) {
		var md metadata.MD
		_, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{}, grpc.Header(&md))
		assert.Equal(t, codes.Unimplemented, status.Code(err))
		if assert.Len(t, md.Get(clusterRoleHeader), 1) {
			assert.Equal(t, "standby", md.Get(clusterRoleHeader)[0])
		}
		if assert.Len(t, md.Get(clusterRoleEpochHeader), 1) {
			assert.Equal(t, "10", md.Get(clusterRoleEpochHeader)[0])
		}
	}, si.Options(), nil)
}

func TestServerInterceptorAddsStreamResponseHeaders(t *testing.T) {
	var si serverinterceptor
	si.setRole(RoleStandby, 10)
	si.roleSetter = noopSetRole
	si.lgr = lgr
	withClient(t, func(t *testing.T, client grpc_health_v1.HealthClient) {
		var md metadata.MD
		srv, err := client.Watch(context.Background(), &grpc_health_v1.HealthCheckRequest{}, grpc.Header(&md))
		require.NoError(t, err)
		_, err = srv.Recv()
		assert.Equal(t, codes.Unimplemented, status.Code(err))
		if assert.Len(t, md.Get(clusterRoleHeader), 1) {
			assert.Equal(t, "standby", md.Get(clusterRoleHeader)[0])
		}
		if assert.Len(t, md.Get(clusterRoleEpochHeader), 1) {
			assert.Equal(t, "10", md.Get(clusterRoleEpochHeader)[0])
		}
	}, si.Options(), nil)
}

func TestServerInterceptorAsPrimaryDoesNotSendRequest(t *testing.T) {
	var si serverinterceptor
	si.setRole(RolePrimary, 10)
	si.roleSetter = noopSetRole
	si.lgr = lgr
	srv := withClient(t, func(t *testing.T, client grpc_health_v1.HealthClient) {
		ctx := metadata.AppendToOutgoingContext(context.Background(), "test-header", "test-header-value")
		_, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		ctx = metadata.AppendToOutgoingContext(context.Background(), "test-header", "test-header-value")
		ss, err := client.Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
		assert.NoError(t, err)
		_, err = ss.Recv()
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	}, si.Options(), nil)
	assert.Nil(t, srv.md)
}

func TestClientInterceptorAddsUnaryRequestHeaders(t *testing.T) {
	var ci clientinterceptor
	ci.setRole(RolePrimary, 10)
	ci.roleSetter = noopSetRole
	ci.lgr = lgr
	srv := withClient(t, func(t *testing.T, client grpc_health_v1.HealthClient) {
		_, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		assert.Equal(t, codes.Unimplemented, status.Code(err))
	}, nil, ci.Options())
	if assert.Len(t, srv.md.Get(clusterRoleHeader), 1) {
		assert.Equal(t, "primary", srv.md.Get(clusterRoleHeader)[0])
	}
	if assert.Len(t, srv.md.Get(clusterRoleEpochHeader), 1) {
		assert.Equal(t, "10", srv.md.Get(clusterRoleEpochHeader)[0])
	}
}

func TestClientInterceptorAddsStreamRequestHeaders(t *testing.T) {
	var ci clientinterceptor
	ci.setRole(RolePrimary, 10)
	ci.roleSetter = noopSetRole
	ci.lgr = lgr
	srv := withClient(t, func(t *testing.T, client grpc_health_v1.HealthClient) {
		srv, err := client.Watch(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		require.NoError(t, err)
		_, err = srv.Recv()
		assert.Equal(t, codes.Unimplemented, status.Code(err))
	}, nil, ci.Options())
	if assert.Len(t, srv.md.Get(clusterRoleHeader), 1) {
		assert.Equal(t, "primary", srv.md.Get(clusterRoleHeader)[0])
	}
	if assert.Len(t, srv.md.Get(clusterRoleEpochHeader), 1) {
		assert.Equal(t, "10", srv.md.Get(clusterRoleEpochHeader)[0])
	}
}

func TestClientInterceptorAsStandbyDoesNotSendRequest(t *testing.T) {
	var ci clientinterceptor
	ci.setRole(RolePrimary, 10)
	ci.roleSetter = noopSetRole
	ci.lgr = lgr
	srv := withClient(t, func(t *testing.T, client grpc_health_v1.HealthClient) {
		_, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		assert.Equal(t, codes.Unimplemented, status.Code(err))
		ci.setRole(RoleStandby, 11)
		_, err = client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		_, err = client.Watch(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	}, nil, ci.Options())
	if assert.Len(t, srv.md.Get(clusterRoleHeader), 1) {
		assert.Equal(t, "primary", srv.md.Get(clusterRoleHeader)[0])
	}
	if assert.Len(t, srv.md.Get(clusterRoleEpochHeader), 1) {
		assert.Equal(t, "10", srv.md.Get(clusterRoleEpochHeader)[0])
	}
}
