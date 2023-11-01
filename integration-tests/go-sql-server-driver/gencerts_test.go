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

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"path/filepath"
	"math/big"
	"net/url"
	"os"
	"time"
)

// Generates a 4096-bit RSA chain and an ed25519 chain.
// Each chain includes a root, an intermediate, a leaf with DNS and URI SANs.
// Root and intermediate have isCA=true and key usage CertSign.
// Leaf has isCA=false and key usage digitalSignature and extKeyUsage ServerAuth.
//
// Generates separate expired leafs for each key type.
//
// Emits private keys of the leafs. RSA keys are emitted PEM encoded PKCS1.
// ed25519 keys are emitted PEM encoded PKCS8.
//
// These certificates and private keys are used by
// tests/sql-server-cluster-tls.yaml and tests/sql-server-tls.yaml, for
// example.
//
// TODO: Further tests which should not verify? (SHA-1 signatures, expired
// roots or intermediates, wrong isCA, wrong key usage, etc.)

func GenerateX509Certs(dir string) error {
	rsacerts, err := MakeRSACerts()
	if err != nil {
		return fmt.Errorf("could not make rsa certs: %w", err)
	}

	err = WriteRSACerts(dir, rsacerts)
	if err != nil {
		return fmt.Errorf("could not write rsa certs: %w", err)
	}

	edcerts, err := MakeEd25519Certs()
	if err != nil {
		return fmt.Errorf("could not make ed25519 certs: %w", err)
	}

	err = WriteEd25519Certs(dir, edcerts)
	if err != nil {
		return fmt.Errorf("could not write ed25519 certs: %w", err)
	}
	return nil
}

func WriteRSACerts(dir string, rsacerts TestCerts) error {
	err := os.WriteFile(filepath.Join(dir, "rsa_root.pem"), pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rsacerts.Root.Raw,
	}), 0664)
	if err != nil {
		return err
	}
	err = os.WriteFile(filepath.Join(dir, "rsa_chain.pem"), append(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rsacerts.Leaf.Raw,
	}), pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rsacerts.Intermediate.Raw,
	})...), 0664)
	if err != nil {
		return err
	}
	err = os.WriteFile(filepath.Join(dir, "rsa_key.pem"), pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsacerts.LeafKey.(*rsa.PrivateKey)),
	}), 0664)
	if err != nil {
		return err
	}

	err = os.WriteFile(filepath.Join(dir, "rsa_exp_chain.pem"), append(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rsacerts.ExpiredLeaf.Raw,
	}), pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rsacerts.Intermediate.Raw,
	})...), 0664)
	if err != nil {
		return err
	}
	err = os.WriteFile(filepath.Join(dir, "rsa_exp_key.pem"), pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsacerts.ExpiredLeafKey.(*rsa.PrivateKey)),
	}), 0664)
	if err != nil {
		return err
	}

	return nil
}

func WriteEd25519Certs(dir string, edcerts TestCerts) error {
	err := os.WriteFile(filepath.Join(dir, "ed25519_root.pem"), pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: edcerts.Root.Raw,
	}), 0664)
	if err != nil {
		return err
	}
	err = os.WriteFile(filepath.Join(dir, "ed25519_chain.pem"), append(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: edcerts.Leaf.Raw,
	}), pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: edcerts.Intermediate.Raw,
	})...), 0664)
	if err != nil {
		return err
	}
	keybytes, err := x509.MarshalPKCS8PrivateKey(edcerts.LeafKey)
	if err != nil {
		return err
	}
	err = os.WriteFile(filepath.Join(dir, "ed25519_key.pem"), pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keybytes,
	}), 0664)
	if err != nil {
		return err
	}

	err = os.WriteFile(filepath.Join(dir, "ed25519_exp_chain.pem"), append(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: edcerts.ExpiredLeaf.Raw,
	}), pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: edcerts.Intermediate.Raw,
	})...), 0664)
	if err != nil {
		return err
	}
	keybytes, err = x509.MarshalPKCS8PrivateKey(edcerts.ExpiredLeafKey)
	if err != nil {
		return err
	}
	err = os.WriteFile(filepath.Join(dir, "edcerts_exp_key.pem"), pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keybytes,
	}), 0664)
	if err != nil {
		return err
	}

	return nil
}

type TestCerts struct {
	Root           *x509.Certificate
	Intermediate   *x509.Certificate
	Leaf           *x509.Certificate
	LeafKey        any
	ExpiredLeaf    *x509.Certificate
	ExpiredLeafKey any
}

func MakeRSACerts() (TestCerts, error) {
	genKey := func() (any, any, error) {
		key, err := rsa.GenerateKey(rand.Reader, 4096)
		if err != nil {
			return nil, nil, err
		}
		return key.Public(), key, nil
	}
	return MakeCerts("RSA 4096-bit", genKey)
}

func MakeEd25519Certs() (TestCerts, error) {
	genKey := func() (any, any, error) {
		return ed25519.GenerateKey(rand.Reader)
	}
	return MakeCerts("ed25519", genKey)
}

func MakeCerts(desc string, genKey func() (any, any, error)) (TestCerts, error) {
	nbf := time.Now().Add(-24 * time.Hour)
	exp := nbf.Add(24 * 365 * 10 * time.Hour)
	badExp := nbf.Add(12 * time.Hour)

	rootpub, rootpriv, err := genKey()
	if err != nil {
		return TestCerts{}, err
	}
	intpub, intpriv, err := genKey()
	if err != nil {
		return TestCerts{}, err
	}
	leafpub, leafpriv, err := genKey()
	if err != nil {
		return TestCerts{}, err
	}
	exppub, exppriv, err := genKey()
	if err != nil {
		return TestCerts{}, err
	}

	signer, err := NewRootSigner(&x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Country:      []string{"US"},
			Organization: []string{"DoltHub, Inc."},
			CommonName:   "dolt integration tests " + desc + " Root",
		},
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		NotBefore:             nbf,
		NotAfter:              exp,
	}, rootpub, rootpriv)
	if err != nil {
		return TestCerts{}, err
	}

	intcert, err := signer.Sign(&x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Country:      []string{"US"},
			Organization: []string{"DoltHub, Inc."},
			CommonName:   "dolt integration tests " + desc + " Intermediate",
		},
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		NotBefore:             nbf,
		NotAfter:              exp,
	}, intpub)
	if err != nil {
		return TestCerts{}, err
	}
	intsigner := Signer{intcert, intpriv}

	leafdns := "dolt-instance.dolt-integration-test.example"
	leafurl, err := url.Parse("spiffe://dolt-integration-tests.dev.trust.dolthub.com.example/dolt-instance")
	if err != nil {
		return TestCerts{}, err
	}

	leafcert, err := intsigner.Sign(&x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			Country:      []string{"US"},
			Organization: []string{"DoltHub, Inc."},
			CommonName:   "dolt integration tests " + desc + " Leaf",
		},
		BasicConstraintsValid: true,
		IsCA:                  false,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		NotBefore:             nbf,
		NotAfter:              exp,
		DNSNames:              []string{leafdns},
		URIs:                  []*url.URL{leafurl},
	}, leafpub)
	if err != nil {
		return TestCerts{}, err
	}

	expcert, err := intsigner.Sign(&x509.Certificate{
		SerialNumber: big.NewInt(4),
		Subject: pkix.Name{
			Country:      []string{"US"},
			Organization: []string{"DoltHub, Inc."},
			CommonName:   "dolt integration tests " + desc + " Expired Leaf",
		},
		BasicConstraintsValid: true,
		IsCA:                  false,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		NotBefore:             nbf,
		NotAfter:              badExp,
		DNSNames:              []string{leafdns},
		URIs:                  []*url.URL{leafurl},
	}, exppub)
	if err != nil {
		return TestCerts{}, err
	}

	return TestCerts{
		Root:           signer.Cert,
		Intermediate:   intsigner.Cert,
		Leaf:           leafcert,
		ExpiredLeaf:    expcert,
		LeafKey:        leafpriv,
		ExpiredLeafKey: exppriv,
	}, nil
}

type Signer struct {
	Cert *x509.Certificate
	Key  interface{}
}

func (s Signer) Sign(cert *x509.Certificate, pub any) (*x509.Certificate, error) {
	der, err := x509.CreateCertificate(rand.Reader, cert, s.Cert, pub, s.Key)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(der)
}

func NewRootSigner(cert *x509.Certificate, pub, priv any) (Signer, error) {
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, priv)
	if err != nil {
		return Signer{}, err
	}
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		return Signer{}, err
	}
	return Signer{cert, priv}, nil
}
