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
	"encoding/base64"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
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

type AccessControl interface {
	ApiAuthenticate(creds *RequestCredentials, lgr *logrus.Entry) *sql.Context
	ApiAuthorize(ctx *sql.Context, lgr *logrus.Entry) bool
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
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		var username string
		var password string

		auths := md.Get("authorization")
		if len(auths) != 1 {
			username = "root"
		} else {
			auth := auths[0]
			if !strings.HasPrefix(auth, "Basic ") {
				si.Lgr.Info("incoming request had malformed authentication header")
				return status.Error(codes.Unauthenticated, "unauthenticated")
			}
			authTrim := strings.TrimPrefix(auth, "Basic ")
			uDec, err := base64.URLEncoding.DecodeString(authTrim)
			if err != nil {
				si.Lgr.Infof("incoming request authorization header failed to decode: %v", err)
				return status.Error(codes.Unauthenticated, "unauthenticated")
			}
			userPass := strings.Split(string(uDec), ":")
			username = userPass[0]
			password = userPass[1]
		}
		addr, ok := peer.FromContext(ctx)
		if !ok {
			si.Lgr.Info("incoming request had no peer")
			return status.Error(codes.Unauthenticated, "unauthenticated")
		}

		creds := &RequestCredentials{Username: username, Password: password, Address: addr.Addr.String()}
		sqlCtx := si.AccessController.ApiAuthenticate(creds, si.Lgr)
		if sqlCtx == nil {
			return status.Error(codes.Unauthenticated, "unauthenticated")
		}

		if authorized := si.AccessController.ApiAuthorize(sqlCtx, si.Lgr); !authorized {
			return status.Error(codes.PermissionDenied, "unauthorized")
		}

		// Access Granted.
		return nil
	}

	return status.Error(codes.Unauthenticated, "unauthenticated 1")
}
