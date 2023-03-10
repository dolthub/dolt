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

package env

import (
	"crypto/tls"
	"net"
	"net/http"
	"runtime"
	"strings"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
)

// GRPCDialProvider implements dbfactory.GRPCDialProvider. By default, it is not able to use custom user credentials, but
// if it is initialized with a DoltEnv, it will load custom user credentials from it.
type GRPCDialProvider struct {
	dEnv *DoltEnv
}

var _ dbfactory.GRPCDialProvider = GRPCDialProvider{}

// NewGRPCDialProvider returns a new GRPCDialProvider, with no DoltEnv configured and without supporting
// custom user credentials.
func NewGRPCDialProvider() *GRPCDialProvider {
	return &GRPCDialProvider{}
}

// NewGRPCDialProviderFromDoltEnv returns a new GRPCDialProvider, configured with the specified DoltEnv
// and uses that DoltEnv to load custom user credentials.
func NewGRPCDialProviderFromDoltEnv(dEnv *DoltEnv) *GRPCDialProvider {
	return &GRPCDialProvider{
		dEnv: dEnv,
	}
}

// GetGRPCDialParms implements dbfactory.GRPCDialProvider
func (p GRPCDialProvider) GetGRPCDialParams(config grpcendpoint.Config) (dbfactory.GRPCRemoteConfig, error) {
	endpoint := config.Endpoint
	if strings.IndexRune(endpoint, ':') == -1 {
		if config.Insecure {
			endpoint += ":80"
		} else {
			endpoint += ":443"
		}
	}

	var httpfetcher grpcendpoint.HTTPFetcher = http.DefaultClient

	var opts []grpc.DialOption
	if config.TLSConfig != nil {
		tc := credentials.NewTLS(config.TLSConfig)
		opts = append(opts, grpc.WithTransportCredentials(tc))

		httpfetcher = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:   config.TLSConfig,
				ForceAttemptHTTP2: true,
			},
		}
	} else if config.Insecure {
		opts = append(opts, grpc.WithInsecure())
	} else {
		tc := credentials.NewTLS(&tls.Config{})
		opts = append(opts, grpc.WithTransportCredentials(tc))
	}

	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024)))
	opts = append(opts, grpc.WithUserAgent(p.getUserAgentString()))

	if config.Creds != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(config.Creds))
	} else if config.WithEnvCreds {
		rpcCreds, err := p.getRPCCreds(endpoint)
		if err != nil {
			return dbfactory.GRPCRemoteConfig{}, err
		}
		if rpcCreds != nil {
			opts = append(opts, grpc.WithPerRPCCredentials(rpcCreds))
		}
	}
	return dbfactory.GRPCRemoteConfig{
		Endpoint:    endpoint,
		DialOptions: opts,
		HTTPFetcher: httpfetcher,
	}, nil
}

// getRPCCreds returns any RPC credentials available to this dial provider. If a DoltEnv has been configured
// in this dial provider, it will be used to load custom user credentials, otherwise nil will be returned.
func (p GRPCDialProvider) getRPCCreds(endpoint string) (credentials.PerRPCCredentials, error) {
	if p.dEnv == nil {
		return nil, nil
	}

	dCreds, valid, err := p.dEnv.UserDoltCreds()
	if err != nil {
		return nil, ErrInvalidCredsFile
	}
	if !valid {
		return nil, nil
	}

	return dCreds.RPCCreds(getHostFromEndpoint(endpoint)), nil
}

func getHostFromEndpoint(endpoint string) string {
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return DefaultRemotesApiHost
	}
	return host
}

// getUserAgentString returns a user agent string to use in GRPC requests.
func (p GRPCDialProvider) getUserAgentString() string {
	version := ""
	if p.dEnv != nil {
		version = p.dEnv.Version
	}

	tokens := []string{
		"dolt_cli",
		version,
		runtime.GOOS,
		runtime.GOARCH,
	}

	for i, t := range tokens {
		tokens[i] = strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) {
				return '_'
			}

			return r
		}, strings.TrimSpace(t))
	}

	return strings.Join(tokens, " ")
}
