// Copyright 2023 Dolthub, Inc.
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

package remotesrv

import (
	"context"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RequestCredentials struct {
	Username string
	Password string
	Address  string
}

type ServerInterceptor struct {
	Lgr              *logrus.Entry
	AccessController AccessControl
}

// AccessControl is an interface that provides authentication and authorization for the gRPC server.
type AccessControl interface {
	// ApiAuthenticate checks the incoming request for authentication credentials and validates them. If the user's
	// identity checks out, the returned context will have the sqlContext within it, which contains the user's ID.
	// If the user is not legitimate, an error is returned.
	ApiAuthenticate(ctx context.Context) (context.Context, error)
	// ApiAuthorize checks that the authenticated user has sufficient privileges to perform the requested action.
	// Currently, CLONE_ADMIN is required. True and a nil error returned if the user is authorized, otherwise false
	// with an error.
	ApiAuthorize(ctx context.Context) (bool, error)
}

func (si *ServerInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := si.authenticate(ss.Context()); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

func (si *ServerInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := si.authenticate(ctx); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func (si *ServerInterceptor) Options() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(si.Unary()),
		grpc.ChainStreamInterceptor(si.Stream()),
	}
}

// authenticate checks the incoming request for authentication credentials and validates them.  If the user is
// legitimate, an authorization check is performed. If no error is returned, the user should be allowed to proceed.
func (si *ServerInterceptor) authenticate(ctx context.Context) error {
	ctx, err := si.AccessController.ApiAuthenticate(ctx)
	if err != nil {
		si.Lgr.Warnf("authentication failed: %s", err.Error())
		status.Error(codes.Unauthenticated, "unauthenticated")
		return err
	}

	// Have a valid user in the context.  Check authorization.
	if authorized, err := si.AccessController.ApiAuthorize(ctx); !authorized {
		si.Lgr.Warnf("authorization failed: %s", err.Error())
		status.Error(codes.PermissionDenied, "unauthorized")
		return err
	}

	// Access Granted.
	return nil
}
