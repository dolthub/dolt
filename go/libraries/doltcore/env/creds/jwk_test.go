package creds

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
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
