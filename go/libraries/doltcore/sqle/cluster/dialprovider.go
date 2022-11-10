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
	"crypto/tls"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"

	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
)

// We wrap the default environment dial provider. In the standby replication
// case, we want the following differences:
//
// - client interceptors for transmitting our replication role.
// - do not use environment credentials. (for now).
type grpcDialProvider struct {
	orig   dbfactory.GRPCDialProvider
	ci     *clientinterceptor
	tlsCfg *tls.Config
	creds  credentials.PerRPCCredentials
}

func (p grpcDialProvider) GetGRPCDialParams(config grpcendpoint.Config) (dbfactory.GRPCRemoteConfig, error) {
	config.TLSConfig = p.tlsCfg
	config.Creds = p.creds
	if config.Creds != nil && config.TLSConfig != nil {
		if c, ok := config.Creds.(*creds.RPCCreds); ok {
			c.RequireTLS = true
		}
	}
	config.WithEnvCreds = false
	cfg, err := p.orig.GetGRPCDialParams(config)
	if err != nil {
		return dbfactory.GRPCRemoteConfig{}, err
	}

	cfg.DialOptions = append(cfg.DialOptions, p.ci.Options()...)
	cfg.DialOptions = append(cfg.DialOptions, grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  250 * time.Millisecond,
			Multiplier: 1.6,
			Jitter:     0.6,
			MaxDelay:   10 * time.Second,
		},
		MinConnectTimeout: 250 * time.Millisecond,
	}))

	return cfg, nil
}
