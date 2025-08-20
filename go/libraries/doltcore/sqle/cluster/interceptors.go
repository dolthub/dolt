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
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gopkg.in/go-jose/go-jose.v2/jwt"

	"github.com/dolthub/dolt/go/libraries/utils/jwtauth"
)

const clusterRoleHeader = "x-dolt-cluster-role"
const clusterRoleEpochHeader = "x-dolt-cluster-role-epoch"

var writeEndpoints map[string]bool

func init() {
	writeEndpoints = make(map[string]bool)
	writeEndpoints["/dolt.services.remotesapi.v1alpha1.ChunkStoreService/Commit"] = true
	writeEndpoints["/dolt.services.remotesapi.v1alpha1.ChunkStoreService/AddTableFiles"] = true
	writeEndpoints["/dolt.services.remotesapi.v1alpha1.ChunkStoreService/GetUploadLocations"] = true
}

func isLikelyServerResponse(err error) bool {
	code := status.Code(err)
	switch code {
	case codes.Unavailable:
		fallthrough
	case codes.DeadlineExceeded:
		fallthrough
	case codes.Canceled:
		return false
	default:
		return true
	}
}

// clientinterceptor is installed as a Unary and Stream client interceptor on
// the client conns that are used to communicate with standby remotes. The
// cluster.Controller sets this server's current Role and role epoch on the
// interceptor anytime it changes. In turn, this interceptor:
// * adds the server's current role and epoch to the request headers for every
// outbound request.
// * fails all outgoing requests immediately with codes.FailedPrecondition if
// the role == RoleStandby, since this server should not be replicating when it
// believes it is a standby.
// * watches returned response headers for a situation which causes this server
// to force downgrade from primary to standby. In particular, when a returned
// response header asserts that the standby replica is a primary at a higher
// epoch than this server, this incterceptor coordinates with the Controller to
// immediately transition to standby and to stop replicating to the standby.
type clientinterceptor struct {
	lgr        *logrus.Entry
	roleSetter func(role string, epoch int)
	role       Role
	epoch      int
	mu         sync.Mutex
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
		ci.lgr.Tracef("cluster: clientinterceptor: processing request to %s, role %s", method, string(role))
		if role == RoleStandby {
			return nil, status.Error(codes.FailedPrecondition, "cluster: clientinterceptor: this server is a standby and is not currently replicating to its standby")
		}
		if role == RoleDetectedBrokenConfig {
			return nil, status.Error(codes.FailedPrecondition, "cluster: clientinterceptor: this server is in detected_broken_config and is not currently replicating to its standby")
		}
		ctx = metadata.AppendToOutgoingContext(ctx, clusterRoleHeader, string(role), clusterRoleEpochHeader, strconv.Itoa(epoch))
		var header metadata.MD
		stream, err := streamer(ctx, desc, cc, method, append(opts, grpc.Header(&header))...)
		ci.handleResponseHeaders(header, err)
		return stream, err
	}
}

func (ci *clientinterceptor) Unary() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		role, epoch := ci.getRole()
		ci.lgr.Tracef("cluster: clientinterceptor: processing request to %s, role %s", method, string(role))
		if role == RoleStandby {
			return status.Error(codes.FailedPrecondition, "cluster: clientinterceptor: this server is a standby and is not currently replicating to its standby")
		}
		if role == RoleDetectedBrokenConfig {
			return status.Error(codes.FailedPrecondition, "cluster: clientinterceptor: this server is in detected_broken_config and is not currently replicating to its standby")
		}
		ctx = metadata.AppendToOutgoingContext(ctx, clusterRoleHeader, string(role), clusterRoleEpochHeader, strconv.Itoa(epoch))
		var header metadata.MD
		err := invoker(ctx, method, req, reply, cc, append(opts, grpc.Header(&header))...)
		ci.handleResponseHeaders(header, err)
		return err
	}
}

func (ci *clientinterceptor) handleResponseHeaders(header metadata.MD, err error) {
	role, epoch := ci.getRole()
	if role != RolePrimary {
		// By the time we process this response, we were no longer a primary.
		return
	}
	respEpochs := header.Get(clusterRoleEpochHeader)
	respRoles := header.Get(clusterRoleHeader)
	if len(respEpochs) > 0 && len(respRoles) > 0 {
		respRole := respRoles[0]
		respEpoch, err := strconv.Atoi(respEpochs[0])
		if err == nil {
			if respRole == string(RolePrimary) {
				if respEpoch == epoch {
					ci.lgr.Errorf("cluster: clientinterceptor: this server and the server replicating to it are both primary at the same epoch. force transitioning to detected_broken_config.")
					ci.roleSetter(string(RoleDetectedBrokenConfig), respEpoch)
				} else if respEpoch > epoch {
					// The server we replicate to thinks it is the primary at a higher epoch than us...
					ci.lgr.Warnf("cluster: clientinterceptor: this server is primary at epoch %d. a server it attempted to replicate to is primary at epoch %d. force transitioning to standby.", epoch, respEpoch)
					ci.roleSetter(string(RoleStandby), respEpoch)
				}
			} else if respRole == string(RoleDetectedBrokenConfig) && respEpoch >= epoch {
				ci.lgr.Errorf("cluster: clientinterceptor: this server learned from its standby that the standby is in detected_broken_config at the same or higher epoch. force transitioning to detected_broken_config.")
				ci.roleSetter(string(RoleDetectedBrokenConfig), respEpoch)
			}
		} else {
			ci.lgr.Errorf("cluster: clientinterceptor: failed to parse epoch in response header; something is wrong: %v", err)
		}
	} else if isLikelyServerResponse(err) {
		ci.lgr.Warnf("cluster: clientinterceptor: response was missing role and epoch metadata")
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
// interceptor anytime it changes. In turn, this interceptor has the following
// behavior:
// * for any incoming standby traffic, it will add the server's current role
// and epoch to the response headers for every request.
// * for any incoming standby traffic, it will fail incoming requests
// immediately with codes.FailedPrecondition if the current role !=
// RoleStandby, since nothing should be replicating to us in that state.
// * watches incoming request headers for a situation which causes this server
// to force downgrade from primary to standby. In particular, when an incoming
// request asserts that the client is the current primary at an epoch higher
// than our current epoch, this interceptor coordinates with the Controller to
// immediately transition to standby and allow replication requests through.
// * for incoming requests which are not standby, it will currently fail the
// requests with codes.Unauthenticated. Eventually, it will allow read-only
// traffic through which is authenticated and authorized.
//
// The serverinterceptor is responsible for authenticating incoming requests
// from standby replicas. It is instantiated with a jwtauth.KeyProvider and
// some jwt.Expected. Incoming requests must have a valid, unexpired, signed
// JWT, signed by a key accessible in the KeyProvider.
type serverinterceptor struct {
	keyProvider jwtauth.KeyProvider
	jwtExpected jwt.Expected
	
	lgr        *logrus.Entry
	roleSetter func(role string, epoch int)
	role       Role
	epoch      int
	mu         sync.Mutex
}

func (si *serverinterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		fromClusterMember := false
		if md, ok := metadata.FromIncomingContext(ss.Context()); ok {
			fromClusterMember = si.handleRequestHeaders(md)
		}
		if fromClusterMember {
			if err := si.authenticate(ss.Context()); err != nil {
				return err
			}
			// After handleRequestHeaders, our role may have changed, so we fetch it again here.
			role, epoch := si.getRole()
			if err := grpc.SetHeader(ss.Context(), metadata.Pairs(clusterRoleHeader, string(role), clusterRoleEpochHeader, strconv.Itoa(epoch))); err != nil {
				return err
			}
			if role == RolePrimary {
				// As a primary, we do not accept replication requests.
				return status.Error(codes.FailedPrecondition, "this server is a primary and is not currently accepting replication")
			}
			if role == RoleDetectedBrokenConfig {
				// In detected_brokne_config we do not accept replication requests.
				return status.Error(codes.FailedPrecondition, "this server is currently in detected_broken_config and is not currently accepting replication")
			}
			return handler(srv, ss)
		} else if isWrite := writeEndpoints[info.FullMethod]; isWrite {
			return status.Error(codes.Unimplemented, "unimplemented")
		} else {
			return status.Error(codes.Unauthenticated, "unauthenticated")
		}
	}
}

func (si *serverinterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		fromClusterMember := false
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			fromClusterMember = si.handleRequestHeaders(md)
		}
		if fromClusterMember {
			if err := si.authenticate(ctx); err != nil {
				return nil, err
			}
			// After handleRequestHeaders, our role may have changed, so we fetch it again here.
			role, epoch := si.getRole()
			if err := grpc.SetHeader(ctx, metadata.Pairs(clusterRoleHeader, string(role), clusterRoleEpochHeader, strconv.Itoa(epoch))); err != nil {
				return nil, err
			}
			if role == RolePrimary {
				// As a primary, we do not accept replication requests.
				return nil, status.Error(codes.FailedPrecondition, "this server is a primary and is not currently accepting replication")
			}
			if role == RoleDetectedBrokenConfig {
				// In detected_broken_config we do not accept replication requests.
				return nil, status.Error(codes.FailedPrecondition, "this server is currently in detected_broken_config and is not currently accepting replication")
			}
			return handler(ctx, req)
		} else if isWrite := writeEndpoints[info.FullMethod]; isWrite {
			return nil, status.Error(codes.Unimplemented, "unimplemented")
		} else {
			return nil, status.Error(codes.Unauthenticated, "unauthenticated")
		}
	}
}

func (si *serverinterceptor) handleRequestHeaders(header metadata.MD) bool {
	role, epoch := si.getRole()
	epochs := header.Get(clusterRoleEpochHeader)
	roles := header.Get(clusterRoleHeader)
	if len(epochs) > 0 && len(roles) > 0 {
		if roles[0] == string(RolePrimary) {
			if reqepoch, err := strconv.Atoi(epochs[0]); err == nil {
				if reqepoch == epoch && role == RolePrimary {
					// Misconfiguration in the cluster means this
					// server and its standby are marked as Primary
					// at the same epoch. We will become standby
					// and our peer will become standby. An
					// operator will need to get involved.
					si.lgr.Errorf("cluster: serverinterceptor: this server and its standby replica are both primary at the same epoch. force transitioning to detected_broken_config.")
					si.roleSetter(string(RoleDetectedBrokenConfig), reqepoch)
				} else if reqepoch > epoch {
					if role == RolePrimary {
						// The client replicating to us thinks it is the primary at a higher epoch than us.
						si.lgr.Warnf("cluster: serverinterceptor: this server is primary at epoch %d. the server replicating to it is primary at epoch %d. force transitioning to standby.", epoch, reqepoch)
					} else if role == RoleDetectedBrokenConfig {
						si.lgr.Warnf("cluster: serverinterceptor: this server is detected_broken_config at epoch %d. the server replicating to it is primary at epoch %d. transitioning to standby.", epoch, reqepoch)
					}
					si.roleSetter(string(RoleStandby), reqepoch)
				}
			}
		}
		// returns true if the request was from a cluster replica, false otherwise
		return true
	}
	return false
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

func (si *serverinterceptor) authenticate(ctx context.Context) error {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		auths := md.Get("authorization")
		if len(auths) != 1 {
			si.lgr.Info("incoming standby request had no authorization")
			return status.Error(codes.Unauthenticated, "unauthenticated")
		}
		auth := auths[0]
		if !strings.HasPrefix(auth, "Bearer ") {
			si.lgr.Info("incoming standby request had malformed authentication header")
			return status.Error(codes.Unauthenticated, "unauthenticated")
		}
		auth = strings.TrimPrefix(auth, "Bearer ")
		_, err := jwtauth.ValidateJWT(auth, time.Now(), si.keyProvider, si.jwtExpected)
		if err != nil {
			si.lgr.Infof("incoming standby request authorization header failed to verify: %v", err)
			return status.Error(codes.Unauthenticated, "unauthenticated")
		}
		return nil
	}
	return status.Error(codes.Unauthenticated, "unauthenticated")
}
