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
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	expcreds "google.golang.org/grpc/experimental/credentials"

	"github.com/dolthub/dolt/go/libraries/doltcore/credentialhelper"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory/grpcreresolve"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

var defaultDialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}

var defaultTransport = &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	DialContext:           defaultDialer.DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          1024,
	MaxIdleConnsPerHost:   256,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

var defaultHttpFetcher grpcendpoint.HTTPFetcher = &http.Client{
	Transport: defaultTransport,
}

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

// GetGRPCDialParams implements dbfactory.GRPCDialProvider
func (p GRPCDialProvider) GetGRPCDialParams(grpcConfig grpcendpoint.Config) (dbfactory.GRPCRemoteConfig, error) {
	endpoint := grpcConfig.Endpoint
	if strings.IndexRune(endpoint, ':') == -1 {
		if grpcConfig.Insecure {
			endpoint += ":80"
		} else {
			endpoint += ":443"
		}
	}

	var httpfetcher grpcendpoint.HTTPFetcher = defaultHttpFetcher

	var opts []grpc.DialOption
	if grpcConfig.TLSConfig != nil {
		tc := expcreds.NewTLSWithALPNDisabled(grpcConfig.TLSConfig)
		opts = append(opts, grpc.WithTransportCredentials(tc))

		transport := &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           defaultDialer.DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          1024,
			MaxIdleConnsPerHost:   256,
			IdleConnTimeout:       90 * time.Second,
			TLSClientConfig:       grpcConfig.TLSConfig,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
		httpfetcher = &http.Client{
			Transport: transport,
		}
	} else if grpcConfig.Insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tc := expcreds.NewTLSWithALPNDisabled(&tls.Config{})
		opts = append(opts, grpc.WithTransportCredentials(tc))
	}

	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(remotesrv.MaxGRPCMessageSize)))
	opts = append(opts, grpc.WithUserAgent(p.getUserAgentString()))

	// Use the failure-driven re-resolving load-balancing policy.
	opts = append(opts, grpc.WithDefaultServiceConfig(grpcreresolve.ServiceConfigJSON))

	if grpcConfig.Creds != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(grpcConfig.Creds))
	} else if grpcConfig.WithEnvCreds {
		var rpcCreds credentials.PerRPCCredentials
		var err error
		if grpcConfig.UserIdForOsEnvAuth != "" {
			rpcCreds, err = p.getRPCCredsFromOSEnv(grpcConfig.UserIdForOsEnvAuth)
			if err != nil {
				return dbfactory.GRPCRemoteConfig{}, err
			}
		} else {
			rpcCreds, err = p.getRPCCreds(endpoint)
			if err != nil {
				return dbfactory.GRPCRemoteConfig{}, err
			}
		}
		if rpcCreds != nil {
			opts = append(opts, grpc.WithPerRPCCredentials(rpcCreds))
		}
	}

	helper, err := p.getCredentialHelper(grpcConfig, endpoint)
	if err != nil {
		return dbfactory.GRPCRemoteConfig{}, err
	}
	if helper != nil {
		opts = append(opts, helper.DialOptions()...)
		httpfetcher = helper.WrapHTTPFetcher(httpfetcher)
	}

	return dbfactory.GRPCRemoteConfig{
		Endpoint:    endpoint,
		DialOptions: opts,
		HTTPFetcher: httpfetcher,
	}, nil
}

func (p GRPCDialProvider) getCredentialHelper(grpcConfig grpcendpoint.Config, endpoint string) (*credentialhelper.Helper, error) {
	executable, ok, err := p.globalCredentialHelper()
	if err != nil || !ok {
		return nil, err
	}

	scheme := "https"
	if grpcConfig.TLSConfig == nil && grpcConfig.Insecure {
		scheme = "http"
	}
	return credentialhelper.New(executable, fmt.Sprintf("%s://%s", scheme, endpoint))
}

func (p GRPCDialProvider) globalCredentialHelper() (string, bool, error) {
	if p.dEnv == nil || p.dEnv.Config == nil {
		return "", false, nil
	}

	global, ok := p.dEnv.Config.GetConfig(GlobalConfig)
	if !ok {
		return "", false, nil
	}

	executable, err := global.GetString(config.RemotesApiCredentialHelper)
	if errors.Is(err, config.ErrConfigParamNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return executable, true, nil
}

// getRPCCredsFromOSEnv returns RPC Credentials for the specified username, using the DOLT_REMOTE_PASSWORD
func (p GRPCDialProvider) getRPCCredsFromOSEnv(username string) (credentials.PerRPCCredentials, error) {
	if username == "" {
		return nil, errors.New("Runtime error: username must be provided to getRPCCredsFromOSEnv")
	}

	pass, found := os.LookupEnv(dconfig.EnvDoltRemotePassword)
	if !found {
		return nil, errors.New("error: must set DOLT_REMOTE_PASSWORD environment variable to use --user param")
	}
	c := creds.DoltCredsForPass{
		Username: username,
		Password: pass,
	}

	return c.RPCCreds(), nil
}

// Used for local development and testing, setting this makes Dolt
// sign outgoing authentication JWTs to remotesapi with its value,
// instead of a value derived from the hostname in the authority:
// which the call is going to.
//
// Note that this is a process-wide override, and applies to all
// remotesapi remotes accessed as backups or remotes. It does not
// apply to remotesapi endpoints accessed for cluster
// replication. This feature is undocumented, unsupported, and should
// only used for development and testing.
var OverrideGRPCJWTAudience = os.Getenv("DOLT_OVERRIDE_GRPC_JWT_AUDIENCE")

// getRPCCreds returns any RPC credentials available to this dial provider. If a DoltEnv has been configured
// in this dial provider, it will be used to load custom user credentials, otherwise nil will be returned.
func (p GRPCDialProvider) getRPCCreds(endpoint string) (credentials.PerRPCCredentials, error) {
	if p.dEnv == nil {
		return nil, nil
	}

	if p.dEnv.UserPassConfig != nil {
		return p.dEnv.UserPassConfig.RPCCreds(), nil
	}

	dCreds, valid, err := p.dEnv.UserDoltCreds()
	if err != nil {
		return nil, ErrInvalidCredsFile
	}
	if !valid {
		return nil, nil
	}

	if OverrideGRPCJWTAudience != "" {
		return dCreds.RPCCreds(OverrideGRPCJWTAudience), nil
	} else {
		return dCreds.RPCCreds(getHostFromEndpoint(endpoint)), nil
	}
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
