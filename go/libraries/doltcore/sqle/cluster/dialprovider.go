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
	"crypto/x509"
	"errors"
	"io/ioutil"
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
	orig   dbfactory.GRPCDialProvider
	ci     *clientinterceptor
	caPath string
}

func (p grpcDialProvider) GetGRPCDialParams(config grpcendpoint.Config) (dbfactory.GRPCRemoteConfig, error) {
	tlsConfig, err := p.tlsConfig()
	if err != nil {
		return dbfactory.GRPCRemoteConfig{}, err
	}
	config.TLSConfig = tlsConfig
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

// Within a cluster, if remotesapi is configured with a tls_ca, we take the
// following semantics:
// * The configured tls_ca file holds a set of PEM encoded x509 certificates,
// all of which are trusted roots for the outbound connections the
// remotestorage client establishes.
// * The certificate chain presented by the server must validate to a root
// which was present in tls_ca. In particular, every certificate in the chain
// must be within its validity window, the signatures must be valid, key usage
// and isCa must be correctly set for the roots and the intermediates, and the
// leaf must have extended key usage server auth.
// * On the other hand, no verification is done against the SAN or the Subject
// of the certificate.
//
// We use these TLS semantics for both connections to the gRPC endpoint which
// is the actual remotesapi, and for connections to any HTTPS endpoints to
// which the gRPC service returns URLs. For now, this works perfectly for our
// use case, but it's tightly coupled to `cluster:` deployment topologies and
// the likes.
//
// If tls_ca is not set then default TLS handling is performed. In particular,
// if the remotesapi endpoints is HTTPS, then the system roots are used and
// ServerName is verified against the presented URL SANs of the certificates.
func (p grpcDialProvider) tlsConfig() (*tls.Config, error) {
	if p.caPath == "" {
		return nil, nil
	}
	pem, err := ioutil.ReadFile(p.caPath)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(pem); !ok {
		return nil, errors.New("error loading ca roots from " + p.caPath)
	}
	verifyFunc := func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		certs := make([]*x509.Certificate, len(rawCerts))
		var err error
		for i, asn1Data := range rawCerts {
			certs[i], err = x509.ParseCertificate(asn1Data)
			if err != nil {
				return err
			}
		}
		keyUsages := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		opts := x509.VerifyOptions{
			Roots:         roots,
			CurrentTime:   time.Now(),
			Intermediates: x509.NewCertPool(),
			KeyUsages:     keyUsages,
		}
		for _, cert := range certs[1:] {
			opts.Intermediates.AddCert(cert)
		}
		_, err = certs[0].Verify(opts)
		if err != nil {
			return err
		}
		return nil
	}
	return &tls.Config{
		// We have to InsecureSkipVerify because ServerName is always
		// set by the grpc dial provider and golang tls.Config does not
		// have good support for performing certificate validation
		// without server name validation.
		InsecureSkipVerify: true,

		VerifyPeerCertificate: verifyFunc,

		NextProtos: []string{"h2"},
	}, nil
}
