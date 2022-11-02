// Copyright 2020 Dolthub, Inc.
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

package remotestorage

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc"
)

const (
	grpcRetries = 5
)

func grpcBackOff(ctx context.Context) backoff.BackOff {
	ret := backoff.NewExponentialBackOff()
	return backoff.WithContext(backoff.WithMaxRetries(ret, grpcRetries), ctx)
}

func RetryingUnaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	doit := func() error {
		return processGrpcErr(invoker(ctx, method, req, reply, cc, opts...))
	}
	return backoff.Retry(doit, grpcBackOff(ctx))
}
