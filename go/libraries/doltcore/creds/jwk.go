// Copyright 2019 Liquidata, Inc.
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

package creds

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"io/ioutil"
	"path/filepath"

	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
)

const (
	JWKFileExtension = ".jwk"
	ed25519Crv       = "Ed25519"
	kty              = "OKP"
)

type jwkData struct {
	D   *string `json:"d"`
	X   *string `json:"x"`
	Kty string  `json:"kty"`
	Crv string  `json:"crv"`
}

func JWKCredSerialize(dc DoltCreds) ([]byte, error) {
	if !dc.IsPubKeyValid() {
		panic("public key missing or invalid.  This is a bug.  Should be validated before calling")
	}

	pubKeyStr := base64.URLEncoding.EncodeToString(dc.PubKey)

	var privKeyStr string
	if dc.HasPrivKey() {
		if !dc.IsPrivKeyValid() {
			panic("Invalid private key. This is a bug. Should be validated before calling")
		}

		privKeyStr = base64.URLEncoding.EncodeToString(dc.PrivKey)
	}

	toSerialize := jwkData{&pubKeyStr, &privKeyStr, kty, ed25519Crv}
	data, err := json.Marshal(toSerialize)

	if err != nil {
		return nil, err
	}

	return data, nil
}

func JWKCredsDeserialize(data []byte) (DoltCreds, error) {
	var jwk jwkData
	err := json.Unmarshal(data, &jwk)

	if err == nil {
		var pub, priv []byte
		pub, err = base64.URLEncoding.DecodeString(*jwk.D)

		if err == nil {
			if jwk.X != nil {
				priv, err = base64.URLEncoding.DecodeString(*jwk.X)
			}

			if err == nil {
				kid := PubKeyToKID(pub)
				return DoltCreds{pub, priv, kid}, nil
			}
		}
	}

	return DoltCreds{}, err
}

func JWKCredsWrite(wr io.Writer, dc DoltCreds) error {
	data, err := JWKCredSerialize(dc)

	if err != nil {
		return err
	}

	return iohelp.WriteAll(wr, data)
}

func JWKCredsRead(rd io.Reader) (DoltCreds, error) {
	data, err := ioutil.ReadAll(rd)

	if err != nil {
		return DoltCreds{}, err
	}

	return JWKCredsDeserialize(data)
}

func JWKCredsWriteToDir(fs filesys.Filesys, dir string, dc DoltCreds) (string, error) {
	outFile := filepath.Join(dir, dc.KeyIDBase32Str()+JWKFileExtension)
	wr, err := fs.OpenForWrite(outFile, 0600)

	if err != nil {
		return "", err
	}

	err = JWKCredsWrite(wr, dc)
	if err == nil {
		err = wr.Close()
	} else {
		wr.Close()
	}

	return outFile, err
}

func JWKCredsReadFromFile(fs filesys.Filesys, path string) (DoltCreds, error) {
	rd, err := fs.OpenForRead(path)

	if err != nil {
		return DoltCreds{}, err
	}

	defer rd.Close()
	return JWKCredsRead(rd)
}
