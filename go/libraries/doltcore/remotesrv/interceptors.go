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
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type UserAuth struct {
	User     string
	Password string
}

type serverinterceptor struct {
	Lgr              *logrus.Entry
	ExpectedUserAuth UserAuth
}

func (si *serverinterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := si.authenticate(ss.Context()); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

func (si *serverinterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := si.authenticate(ctx); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func (si *serverinterceptor) Options() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(si.Unary()),
		grpc.ChainStreamInterceptor(si.Stream()),
	}
}

func (si *serverinterceptor) authenticate(ctx context.Context) error {
	if len(si.ExpectedUserAuth.User) == 0 && len(si.ExpectedUserAuth.Password) == 0 {
		return nil
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		auths := md.Get("authorization")
		if len(auths) != 1 {
			si.Lgr.Info("incoming request had no authorization")
			return status.Error(codes.Unauthenticated, "unauthenticated")
		}
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
		compare := subtle.ConstantTimeCompare(uDec, []byte(fmt.Sprintf("%s:%s", si.ExpectedUserAuth.User, si.ExpectedUserAuth.Password)))
		if compare == 0 {

			si.Lgr.Infof("incoming request authorization header failed to match")
			return status.Error(codes.Unauthenticated, "unauthenticated")
		}
		return nil
	}
	return status.Error(codes.Unauthenticated, "unauthenticated")
}
