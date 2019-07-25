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
	"bytes"
	"path/filepath"
	"testing"

	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

func TestSerializeAndDeserialize(t *testing.T) {
	const userDir = "/User/user"
	var credsDir = filepath.Join(userDir, ".dolt/creds")

	fs := filesys.NewInMemFS([]string{credsDir}, nil, userDir)
	creds, err := GenerateCredentials()

	if err != nil {
		t.Fatal("Failed to gen creds", err)
	}

	jwkFile, err := JWKCredsWriteToDir(fs, credsDir, creds)

	if err != nil {
		t.Fatal("Failed to write creds", err)
	}

	deserialized, err := JWKCredsReadFromFile(fs, jwkFile)

	if err != nil {
		t.Fatal("Failed to read creds", err)
	}

	if !bytes.Equal(creds.PubKey, deserialized.PubKey) {
		t.Error(creds.PubKey, "!=", deserialized.PubKey)
	}

	if !bytes.Equal(creds.PrivKey, deserialized.PrivKey) {
		t.Error(creds.PrivKey, "!=", deserialized.PrivKey)
	}

	if !bytes.Equal(creds.KeyID, deserialized.KeyID) {
		t.Error(creds.KeyID, "!=", deserialized.KeyID)
	}
}
