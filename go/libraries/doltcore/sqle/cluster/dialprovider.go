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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
)

// We wrap the default environment dial provider. In the standby replication
// case, we want the following differences:
//
// - client interceptors for transmitting our replication role.
// - do not use environment credentials. (for now).
type grpcDialProvider struct {
	orig dbfactory.GRPCDialProvider
	ci   *clientinterceptor
}

func (p grpcDialProvider) GetGRPCDialParams(config grpcendpoint.Config) (string, []grpc.DialOption, error) {
	config.WithEnvCreds = false
	endpoint, opts, err := p.orig.GetGRPCDialParams(config)
	if err != nil {
		return "", nil, err
	}
	opts = append(opts, p.ci.Options()...)
	opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  250 * time.Millisecond,
			Multiplier: 1.6,
			Jitter:     0.6,
			MaxDelay:   10 * time.Second,
		},
		MinConnectTimeout: 250 * time.Millisecond,
	}))
	return endpoint, opts, nil
}
