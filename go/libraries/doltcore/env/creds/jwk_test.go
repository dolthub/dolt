package creds

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"path/filepath"
	"reflect"
	"testing"
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

	if !reflect.DeepEqual(creds.PubKey, deserialized.PubKey) {
		t.Error(creds.PubKey, "!=", deserialized.PubKey)
	}

	if !reflect.DeepEqual(creds.PrivKey, deserialized.PrivKey) {
		t.Error(creds.PrivKey, "!=", deserialized.PrivKey)
	}

	if creds.KeyID != deserialized.KeyID {
		t.Error(creds.KeyID, "!=", deserialized.KeyID)
	}
}
