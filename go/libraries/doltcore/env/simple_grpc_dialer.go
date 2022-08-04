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
	"runtime"
	"strings"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
)

// SimpleGRPCDialProvider implements GRPCDialProvider. By default, it is not able to use custom user credentials, but
// if a DoltEnv is configured, it will load custom user credentials from it.
type SimpleGRPCDialProvider struct {
	dEnv *DoltEnv
}

var _ dbfactory.GRPCDialProvider = SimpleGRPCDialProvider{}

// NewSimpleGRCDialProvider returns a new SimpleGRPCDialProvider, with no DoltEnv configured and without supporting
// custom user credentials.
func NewSimpleGRPCDialProvider() *SimpleGRPCDialProvider {
	return &SimpleGRPCDialProvider{}
}

// NewSimpleGRPCDialProviderWithDoltEnvreturns a new SimpleGRPCDialProvider, configured with the specified DoltEnv
// and uses that DoltEnv to load custom user credentials.
func NewSimpleGRPCDialProviderWithDoltEnv(dEnv *DoltEnv) *SimpleGRPCDialProvider {
	return &SimpleGRPCDialProvider{
		dEnv: dEnv,
	}
}

// GetGRPCDialParms implements dbfactory.GRPCDialProvider
func (p SimpleGRPCDialProvider) GetGRPCDialParams(config grpcendpoint.Config) (string, []grpc.DialOption, error) {
	endpoint := config.Endpoint
	if strings.IndexRune(endpoint, ':') == -1 {
		if config.Insecure {
			endpoint += ":80"
		} else {
			endpoint += ":443"
		}
	}

	var opts []grpc.DialOption
	if config.Insecure {
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
		rpcCreds, err := p.getRPCCreds()
		if err != nil {
			return "", nil, err
		}
		if rpcCreds != nil {
			opts = append(opts, grpc.WithPerRPCCredentials(rpcCreds))
		}
	}

	return endpoint, opts, nil
}

// getRPCCreds returns any RPC credentials available to this dial provider. If a DoltEnv has been configured
// in this dial provider, it will be used to load custom user credentials, otherwise nil will be returned.
func (p SimpleGRPCDialProvider) getRPCCreds() (credentials.PerRPCCredentials, error) {
	if p.dEnv == nil {
		return nil, nil
	}

	dCreds, valid, err := p.dEnv.UserRPCCreds()
	if err != nil {
		return nil, ErrInvalidCredsFile
	}
	if !valid {
		return nil, nil
	}
	return dCreds, nil
}

// getUserAgentString returns a user agent string to use in GRPC requests.
func (p SimpleGRPCDialProvider) getUserAgentString() string {
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
