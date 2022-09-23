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
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const clusterRoleHeader = "x-dolt-cluster-role"
const clusterRoleEpochHeader = "x-dolt-cluster-role-epoch"

// clientinterceptor is installed as a Unary and Stream client interceptor on
// the client conns that are used to communicate with standby remotes. The
// cluster.Controller sets this server's current Role and role epoch on the
// interceptor anytime it changes. In turn, this interceptor:
// * adds the server's current role and epoch to the request headers for every
// outbound request.
// * fails all outgoing requests immediately with codes.Unavailable if the role
// == RoleStandby, since this server should not be replicating when it believes
// it is a standby.
// * watches returned response headers for a situation which causes this server
// to force downgrade from primary to standby. In particular, when a returned
// response header asserts that the standby replica is a primary at a higher
// epoch than this server, this incterceptor coordinates with the Controller to
// immediately transition to standby and to stop replicating to the standby.
type clientinterceptor struct {
	role  Role
	epoch int
	mu    sync.Mutex
}

func (ci *clientinterceptor) setRole(role Role, epoch int) {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.role = role
	ci.epoch = epoch
}

func (ci *clientinterceptor) getRole() (Role, int) {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	return ci.role, ci.epoch
}

func (ci *clientinterceptor) Stream() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		role, epoch := ci.getRole()
		if role == RoleStandby {
			return nil, status.Error(codes.Unavailable, "this server is a standby and is not currently replicating to its standby")
		}
		ctx = metadata.AppendToOutgoingContext(ctx, clusterRoleHeader, string(role), clusterRoleEpochHeader, strconv.Itoa(epoch))
		var header metadata.MD
		stream, err := streamer(ctx, desc, cc, method, append(opts, grpc.Header(&header))...)
		ci.handleResponseHeaders(header, role, epoch)
		return stream, err
	}
}

func (ci *clientinterceptor) Unary() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		role, epoch := ci.getRole()
		if role == RoleStandby {
			return status.Error(codes.Unavailable, "this server is a standby and is not currently replicating to its standby")
		}
		ctx = metadata.AppendToOutgoingContext(ctx, clusterRoleHeader, string(role), clusterRoleEpochHeader, strconv.Itoa(epoch))
		var header metadata.MD
		err := invoker(ctx, method, req, reply, cc, append(opts, grpc.Header(&header))...)
		ci.handleResponseHeaders(header, role, epoch)
		return err
	}
}

func (ci *clientinterceptor) handleResponseHeaders(header metadata.MD, role Role, epoch int) {
	epochs := header.Get(clusterRoleEpochHeader)
	roles := header.Get(clusterRoleHeader)
	if len(epochs) > 0 && len(roles) > 0 && roles[0] == string(RolePrimary) {
		if retepoch, err := strconv.Atoi(epochs[0]); err == nil {
			if retepoch > epoch {
				// The server we replicate to thinks it is the primary at a higher epoch than us...
				// TODO: Signal to controller that we are forced to become a standby at epoch |retepoch|...
			}
		}
	}
}

func (ci *clientinterceptor) Options() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(ci.Unary()),
		grpc.WithChainStreamInterceptor(ci.Stream()),
	}
}

// serverinterceptor is installed as a Unary and Stream interceptor on a
// ChunkStoreServer which is serving a SQL database as a standby remote. The
// cluster.Controller sets this server's current Role and role epoch on the
// interceptor anytime it changes. In turn, this interceptor:
// * adds the server's current role and epoch to the response headers for every
// request.
// * fails all incoming requests immediately with codes.Unavailable if the
// current role == RolePrimary, since nothing should be replicating to us in
// that state.
// * watches incoming request headers for a situation which causes this server
// to force downgrade from primary to standby. In particular, when an incoming
// request asserts that the client is the current primary at an epoch higher
// than our current epoch, this interceptor coordinates with the Controller to
// immediately transition to standby and allow replication requests through.
type serverinterceptor struct {
	role  Role
	epoch int
	mu    sync.Mutex
}

func (si *serverinterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, into *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if md, ok := metadata.FromIncomingContext(ss.Context()); ok {
			role, epoch := si.getRole()
			si.handleRequestHeaders(md, role, epoch)
		}
		// After handleRequestHeaders, our role may have changed, so we fetch it again here.
		role, epoch := si.getRole()
		if err := grpc.SetHeader(ss.Context(), metadata.Pairs(clusterRoleHeader, string(role), clusterRoleEpochHeader, strconv.Itoa(epoch))); err != nil {
			return err
		}
		if role == RolePrimary {
			// As a primary, we do not accept replication requests.
			return status.Error(codes.Unavailable, "this server is a primary and is not currently accepting replication")
		}
		return handler(srv, ss)
	}
}

func (si *serverinterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			role, epoch := si.getRole()
			si.handleRequestHeaders(md, role, epoch)
		}
		// After handleRequestHeaders, our role may have changed, so we fetch it again here.
		role, epoch := si.getRole()
		if err := grpc.SetHeader(ctx, metadata.Pairs(clusterRoleHeader, string(role), clusterRoleEpochHeader, strconv.Itoa(epoch))); err != nil {
			return nil, err
		}
		if role == RolePrimary {
			// As a primary, we do not accept replication requests.
			return nil, status.Error(codes.Unavailable, "this server is a primary and is not currently accepting replication")
		}
		return handler(ctx, req)
	}
}

func (si *serverinterceptor) handleRequestHeaders(header metadata.MD, role Role, epoch int) {
	epochs := header.Get(clusterRoleEpochHeader)
	roles := header.Get(clusterRoleHeader)
	if len(epochs) > 0 && len(roles) > 0 && roles[0] == string(RolePrimary) && role == RolePrimary {
		if reqepoch, err := strconv.Atoi(epochs[0]); err == nil {
			if reqepoch > epoch {
				// The client replicating to us thinks it is the primary at a higher epoch than us.
				// TODO: Signal to controller that we are forced to become a standby at epoch |reqepoch|
			}
		}
	}
}

func (si *serverinterceptor) Options() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(si.Unary()),
		grpc.ChainStreamInterceptor(si.Stream()),
	}
}

func (si *serverinterceptor) setRole(role Role, epoch int) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.role = role
	si.epoch = epoch
}

func (si *serverinterceptor) getRole() (Role, int) {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.role, si.epoch
}
