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

package grpcendpoint

import (
	"crypto/tls"
	"net/http"

	"google.golang.org/grpc/credentials"
)

type Config struct {
	Endpoint     string
	Insecure     bool
	Creds        credentials.PerRPCCredentials
	WithEnvCreds bool

	// If this is non-empty, and WithEnvCreds is true, then the caller is
	// requesting to use username/password authentication instead of JWT
	// authentication against the gRPC endpoint. Currently, the password
	// comes from the OS environment variable DOLT_REMOTE_PASSWORD.
	UserIdForOsEnvAuth string

	// If non-nil, this is used for transport level security in the dial
	// options, instead of a default option based on `Insecure`.
	TLSConfig *tls.Config
}

type HTTPFetcher interface {
	Do(req *http.Request) (*http.Response, error)
}
